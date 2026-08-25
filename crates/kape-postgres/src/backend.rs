use std::{
    hash::Hash,
    marker::PhantomData,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use kape::{CacheBackend, CacheEntry, KapeResult, SetItem, validate_set_items};
use sqlx::{AssertSqlSafe, PgPool, Postgres, Row, Transaction};

use crate::mutation::{MutationPlan, MutationPlanner, PlannedSet, PlannedUpsert};
use crate::{PostgresBackendError, PostgresCodec, PostgresKey, PostgresValue, StringCodec, codec};

/// A PostgreSQL-backed cache adapter.
pub struct PostgresBackend<K = String, V = String, C = StringCodec> {
    pool: PgPool,
    codec: C,
    table: String,
    namespace: String,
    marker: PhantomData<fn(K, V)>,
}

impl<K, V, C> Clone for PostgresBackend<K, V, C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            codec: self.codec.clone(),
            table: self.table.clone(),
            namespace: self.namespace.clone(),
            marker: PhantomData,
        }
    }
}

impl<K, V> PostgresBackend<K, V> {
    /// Creates an adapter using the application-owned `kape_entries` table.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            codec: StringCodec,
            table: "\"kape_entries\"".to_owned(),
            namespace: String::new(),
            marker: PhantomData,
        }
    }
}

impl<K, V, C> PostgresBackend<K, V, C> {
    /// Replaces the key/value codec.
    #[must_use]
    pub fn with_codec<D>(self, codec: D) -> PostgresBackend<K, V, D>
    where
        D: PostgresCodec<K, V>,
    {
        PostgresBackend {
            pool: self.pool,
            codec,
            table: self.table,
            namespace: self.namespace,
            marker: PhantomData,
        }
    }

    /// Selects a validated `table` or `schema.table` identifier.
    ///
    /// # Errors
    ///
    /// Returns an error for unsafe or unsupported identifiers.
    pub fn with_table(mut self, table: &str) -> KapeResult<Self> {
        self.table = validate_table_name(table)
            .ok_or_else(|| PostgresBackendError::InvalidTableName(table.to_owned()))?;
        Ok(self)
    }

    /// Sets the caller-owned namespace.
    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }
}

impl<K, V, C> PostgresBackend<K, V, C>
where
    C: PostgresCodec<K, V>,
{
    fn namespace_prefix(&self) -> C::EncodedKey {
        C::EncodedKey::namespace_prefix(&self.namespace)
    }

    fn encode_key(&self, key: &K) -> Result<C::EncodedKey, PostgresBackendError> {
        codec::encode_namespaced_key(&self.codec, &self.namespace, key)
    }

    /// Deletes expired rows owned by this namespace.
    ///
    /// The application remains responsible for scheduling this operation and
    /// for table-wide maintenance.
    ///
    /// # Errors
    ///
    /// Returns a backend error when `PostgreSQL` rejects the operation.
    pub async fn purge_expired(&self) -> KapeResult<u64> {
        let prefix = self.namespace_prefix();
        let statement = format!(
            "DELETE FROM {} \
             WHERE substring(key from 1 for length($1)) = $1 \
             AND expires_at_ms IS NOT NULL \
             AND expires_at_ms <= (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT",
            self.table
        );
        let result = sqlx::query(AssertSqlSafe(statement))
            .bind(prefix)
            .execute(&self.pool)
            .await
            .map_err(PostgresBackendError::Sqlx)?;
        Ok(result.rows_affected())
    }
}

#[async_trait]
impl<K, V, C> CacheBackend<K, V> for PostgresBackend<K, V, C>
where
    K: Send + Sync,
    V: Send + Sync,
    C: PostgresCodec<K, V>,
{
    async fn get(&self, key: &K) -> KapeResult<Option<CacheEntry<V>>> {
        let key = self.encode_key(key)?;
        let statement = format!(
            "SELECT value, CASE WHEN expires_at_ms IS NULL THEN NULL \
             ELSE expires_at_ms - (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT \
             END AS remaining_ms FROM {} WHERE key = $1",
            self.table
        );
        let Some(row) = sqlx::query(AssertSqlSafe(statement))
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(PostgresBackendError::Sqlx)?
        else {
            return Ok(None);
        };
        let value: C::EncodedValue = row.try_get("value").map_err(PostgresBackendError::Sqlx)?;
        let remaining_ms: Option<i64> = row
            .try_get("remaining_ms")
            .map_err(PostgresBackendError::Sqlx)?;
        crate::lookup::decode_lookup(&self.codec, Some(value), remaining_ms).map_err(Into::into)
    }

    async fn set(&self, key: &K, value: Arc<V>, ttl: i64) -> KapeResult<()> {
        let now_ms = (ttl > 0).then(unix_now_ms).transpose()?;
        let planner = MutationPlanner::new(&self.codec, &self.namespace);
        match planner.plan_set(key, value.as_ref(), ttl, now_ms)? {
            PlannedSet::Upsert(planned) => upsert(&self.pool, &self.table, planned).await,
            PlannedSet::Delete(key) => delete_one(&self.pool, &self.table, key).await,
        }
    }

    async fn remove(&self, key: &K) -> KapeResult<()> {
        let key = self.encode_key(key)?;
        delete_one(&self.pool, &self.table, key).await
    }

    async fn get_many(&self, keys: &[&K]) -> KapeResult<Vec<Option<CacheEntry<V>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let keys = keys
            .iter()
            .map(|key| self.encode_key(key))
            .collect::<Result<Vec<_>, _>>()?;
        let statement = format!(
            "WITH requested(key, ord) AS (\
             SELECT * FROM UNNEST($1) WITH ORDINALITY\
             ) SELECT entries.value, CASE WHEN entries.expires_at_ms IS NULL THEN NULL \
             ELSE entries.expires_at_ms - \
             (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT END AS remaining_ms \
             FROM requested LEFT JOIN {} AS entries ON entries.key = requested.key \
             ORDER BY requested.ord",
            self.table
        );
        let rows = sqlx::query(AssertSqlSafe(statement))
            .bind(keys)
            .fetch_all(&self.pool)
            .await
            .map_err(PostgresBackendError::Sqlx)?;
        rows.into_iter()
            .map(|row| {
                let value: Option<C::EncodedValue> =
                    row.try_get("value").map_err(PostgresBackendError::Sqlx)?;
                let remaining_ms: Option<i64> = row
                    .try_get("remaining_ms")
                    .map_err(PostgresBackendError::Sqlx)?;
                crate::lookup::decode_lookup(&self.codec, value, remaining_ms)
            })
            .collect::<Result<Vec<_>, PostgresBackendError>>()
            .map_err(Into::into)
    }

    async fn set_many(&self, items: &[SetItem<&K, V>]) -> KapeResult<()>
    where
        K: Eq + Hash,
    {
        validate_set_items(items)?;
        if items.is_empty() {
            return Ok(());
        }

        let now_ms = items
            .iter()
            .any(|item| item.ttl > 0)
            .then(unix_now_ms)
            .transpose()?;
        let MutationPlan { upserts, deletes } =
            MutationPlanner::new(&self.codec, &self.namespace).plan_many(items, now_ms)?;

        match (upserts.is_empty(), deletes.is_empty()) {
            (false, true) => upsert_many(&self.pool, &self.table, upserts).await,
            (true, false) => delete_many(&self.pool, &self.table, deletes).await,
            (false, false) => {
                let mut transaction = self
                    .pool
                    .begin()
                    .await
                    .map_err(PostgresBackendError::Sqlx)?;
                upsert_many_in_transaction(&mut transaction, &self.table, upserts).await?;
                delete_many_in_transaction(&mut transaction, &self.table, deletes).await?;
                transaction
                    .commit()
                    .await
                    .map_err(PostgresBackendError::Sqlx)?;
                Ok(())
            }
            (true, true) => Ok(()),
        }
    }

    async fn remove_many(&self, keys: &[&K]) -> KapeResult<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let keys = keys
            .iter()
            .map(|key| self.encode_key(key))
            .collect::<Result<Vec<_>, _>>()?;
        delete_many(&self.pool, &self.table, keys).await
    }

    async fn clear(&self) -> KapeResult<()> {
        let prefix = self.namespace_prefix();
        let statement = format!(
            "DELETE FROM {} WHERE substring(key from 1 for length($1)) = $1",
            self.table
        );
        sqlx::query(AssertSqlSafe(statement))
            .bind(prefix)
            .execute(&self.pool)
            .await
            .map_err(PostgresBackendError::Sqlx)?;
        Ok(())
    }
}

/// Returns the application host's current Unix timestamp in milliseconds.
fn unix_now_ms() -> Result<i64, PostgresBackendError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(PostgresBackendError::SystemClockBeforeUnixEpoch)?;
    i64::try_from(elapsed.as_millis()).map_err(|_| PostgresBackendError::TimestampOverflow)
}

async fn upsert<K, V>(pool: &PgPool, table: &str, upsert: PlannedUpsert<K, V>) -> KapeResult<()>
where
    K: PostgresValue,
    V: PostgresValue,
{
    let PlannedUpsert {
        key,
        value,
        expires_at_ms,
    } = upsert;
    let statement = upsert_statement(table);
    sqlx::query(AssertSqlSafe(statement))
        .bind(key)
        .bind(value)
        .bind(expires_at_ms)
        .execute(pool)
        .await
        .map_err(PostgresBackendError::Sqlx)?;
    Ok(())
}

async fn upsert_many<K, V>(
    pool: &PgPool,
    table: &str,
    upserts: Vec<PlannedUpsert<K, V>>,
) -> KapeResult<()>
where
    K: PostgresValue,
    V: PostgresValue,
{
    let statement = upsert_many_statement::<K, V>(table);
    let (keys, values, expires_at_ms) = sql_upsert_arrays(upserts);
    sqlx::query(AssertSqlSafe(statement))
        .bind(keys)
        .bind(values)
        .bind(expires_at_ms)
        .execute(pool)
        .await
        .map_err(PostgresBackendError::Sqlx)?;
    Ok(())
}

async fn upsert_many_in_transaction<K, V>(
    transaction: &mut Transaction<'_, Postgres>,
    table: &str,
    upserts: Vec<PlannedUpsert<K, V>>,
) -> KapeResult<()>
where
    K: PostgresValue,
    V: PostgresValue,
{
    let statement = upsert_many_statement::<K, V>(table);
    let (keys, values, expires_at_ms) = sql_upsert_arrays(upserts);
    sqlx::query(AssertSqlSafe(statement))
        .bind(keys)
        .bind(values)
        .bind(expires_at_ms)
        .execute(&mut **transaction)
        .await
        .map_err(PostgresBackendError::Sqlx)?;
    Ok(())
}

fn sql_upsert_arrays<K, V>(
    upserts: Vec<PlannedUpsert<K, V>>,
) -> (Vec<K>, Vec<V>, Vec<Option<i64>>) {
    let mut keys = Vec::with_capacity(upserts.len());
    let mut values = Vec::with_capacity(upserts.len());
    let mut expires_at_ms = Vec::with_capacity(upserts.len());
    for upsert in upserts {
        keys.push(upsert.key);
        values.push(upsert.value);
        expires_at_ms.push(upsert.expires_at_ms);
    }
    (keys, values, expires_at_ms)
}

async fn delete_one<K>(pool: &PgPool, table: &str, key: K) -> KapeResult<()>
where
    K: PostgresValue,
{
    let statement = format!("DELETE FROM {table} WHERE key = $1");
    sqlx::query(AssertSqlSafe(statement))
        .bind(key)
        .execute(pool)
        .await
        .map_err(PostgresBackendError::Sqlx)?;
    Ok(())
}

/// Deletes multiple keys.
async fn delete_many<K>(pool: &PgPool, table: &str, keys: Vec<K>) -> KapeResult<()>
where
    K: PostgresValue,
{
    let statement = format!("DELETE FROM {table} WHERE key = ANY($1)");
    sqlx::query(AssertSqlSafe(statement))
        .bind(keys)
        .execute(pool)
        .await
        .map_err(PostgresBackendError::Sqlx)?;
    Ok(())
}

/// Deletes multiple keys in a transaction.
async fn delete_many_in_transaction<K>(
    transaction: &mut Transaction<'_, Postgres>,
    table: &str,
    keys: Vec<K>,
) -> KapeResult<()>
where
    K: PostgresValue,
{
    let statement = format!("DELETE FROM {table} WHERE key = ANY($1)");
    sqlx::query(AssertSqlSafe(statement))
        .bind(keys)
        .execute(&mut **transaction)
        .await
        .map_err(PostgresBackendError::Sqlx)?;
    Ok(())
}

/// Builds an upsert statement for a single key/value pair.
fn upsert_statement(table: &str) -> String {
    format!(
        "INSERT INTO {table} (key, value, expires_at_ms) VALUES ($1, $2, $3) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, \
         expires_at_ms = EXCLUDED.expires_at_ms"
    )
}

fn upsert_many_statement<K, V>(table: &str) -> String
where
    K: PostgresValue,
    V: PostgresValue,
{
    let key_array = K::array_type_name();
    let value_array = V::array_type_name();
    format!(
        "INSERT INTO {table} (key, value, expires_at_ms) \
         SELECT input.key, input.value, input.expires_at_ms \
         FROM UNNEST($1::{key_array}, $2::{value_array}, $3::BIGINT[]) \
         AS input(key, value, expires_at_ms) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, \
         expires_at_ms = EXCLUDED.expires_at_ms"
    )
}

fn validate_table_name(table: &str) -> Option<String> {
    let parts = table.split('.').collect::<Vec<_>>();
    if parts.is_empty() || parts.len() > 2 || !parts.iter().all(|part| valid_identifier(part)) {
        return None;
    }
    Some(
        parts
            .into_iter()
            .map(|part| format!("\"{part}\""))
            .collect::<Vec<_>>()
            .join("."),
    )
}

fn valid_identifier(identifier: &str) -> bool {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_table_names() {
        assert_eq!(
            validate_table_name("kape_entries").as_deref(),
            Some("\"kape_entries\"")
        );
        assert_eq!(
            validate_table_name("app.kape_entries").as_deref(),
            Some("\"app\".\"kape_entries\"")
        );
        assert_eq!(validate_table_name("bad-name"), None);
        assert_eq!(validate_table_name("public.entries;DROP TABLE users"), None);
        assert_eq!(validate_table_name("a.b.c"), None);
    }
}
