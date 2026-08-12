use std::{marker::PhantomData, sync::Arc, time::Duration};

use crate::{PostgresBackendError, PostgresCodec};
use async_trait::async_trait;
use kape::{BackendSetItem, CacheBackend, IterationPage, KapeError, Lookup, ResolvedTTL};
use sqlx::{AssertSqlSafe, PgPool, Row};

/// A `Kape` backend using `PostgreSQL`.
pub struct PostgresBackend<K, V, C> {
    pool: PgPool,
    codec: C,
    table: String,
    namespace: Vec<u8>,
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

impl<K, V, C> PostgresBackend<K, V, C>
where
    C: PostgresCodec<K, V>,
{
    /// Creates an adapter using the `kape_entries` table.
    #[must_use]
    pub fn new(pool: PgPool, codec: C) -> Self {
        Self {
            pool,
            codec,
            table: "kape_entries".to_owned(),
            namespace: Vec::new(),
            marker: PhantomData,
        }
    }

    /// Selects a validated `table` or `schema.table` identifier.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresBackendError::InvalidTableName`] when the value is not
    /// a safe one- or two-component SQL identifier.
    pub fn with_table(mut self, table: &str) -> Result<Self, KapeError> {
        self.table = validate_table_name(table)
            .ok_or_else(|| PostgresBackendError::<C::Error>::InvalidTableName(table.to_owned()))?;
        Ok(self)
    }

    /// Frames every encoded key with the supplied namespace.
    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<Vec<u8>>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Returns the `PostgreSQL` connection pool.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Checks that the configured cache table exists.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresBackendError::TableNotFound`] when the table does not
    /// exist or is not visible through the current connection's search path,
    /// or a `PostgreSQL` operation error when the check fails.
    pub async fn check_table(&self) -> Result<(), KapeError> {
        let row = sqlx::query("SELECT to_regclass($1) IS NOT NULL AS present")
            .bind(&self.table)
            .fetch_one(&self.pool)
            .await
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
        if row
            .try_get("present")
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?
        {
            Ok(())
        } else {
            Err(PostgresBackendError::<C::Error>::TableNotFound(self.table.clone()).into())
        }
    }

    /// Deletes expired rows and returns the number removed.
    ///
    /// # Errors
    ///
    /// Returns a `PostgreSQL` operation error when cleanup fails.
    pub async fn purge_expired(&self) -> Result<u64, KapeError> {
        let statement = format!(
            "DELETE FROM {} \
             WHERE expires_at_ms IS NOT NULL \
             AND expires_at_ms <= (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT",
            self.table
        );
        let result = sqlx::query(AssertSqlSafe(statement))
            .execute(&self.pool)
            .await
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
        Ok(result.rows_affected())
    }

    fn encode_key(&self, key: &K) -> Result<Vec<u8>, PostgresBackendError<C::Error>> {
        let encoded = self
            .codec
            .encode_key(key)
            .map_err(PostgresBackendError::Codec)?;
        frame_key(&self.namespace, &encoded).ok_or(PostgresBackendError::NamespaceTooLong)
    }
}

#[async_trait]
impl<K, V, C> CacheBackend<K, V> for PostgresBackend<K, V, C>
where
    K: Send + Sync,
    V: Send + Sync,
    C: PostgresCodec<K, V>,
{
    async fn get(&self, key: &K) -> Result<Lookup<V>, KapeError> {
        let key = self.encode_key(key)?;
        let statement = format!(
            "SELECT value, \
             CASE WHEN expires_at_ms IS NULL THEN NULL \
             ELSE expires_at_ms - (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT \
             END AS remaining_ms \
             FROM {} WHERE key = $1",
            self.table
        );
        let Some(row) = sqlx::query(AssertSqlSafe(statement))
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?
        else {
            return Ok(Lookup::Miss);
        };

        let bytes: Vec<u8> = row
            .try_get("value")
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
        let remaining_ms: Option<i64> = row
            .try_get("remaining_ms")
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
        crate::lookup::lookup(&self.codec, Some(&bytes), remaining_ms).map_err(Into::into)
    }

    async fn set(&self, key: &K, value: Arc<V>, ttl: ResolvedTTL) -> Result<(), KapeError> {
        let key = self.encode_key(key)?;
        let bytes = self
            .codec
            .encode_value(value.as_ref())
            .map_err(PostgresBackendError::Codec)?;
        let ttl_ms = match ttl {
            ResolvedTTL::Never => None,
            ResolvedTTL::After(duration) => Some(duration_millis::<C::Error>(duration)?),
        };
        let statement = format!(
            "INSERT INTO {} (key, value, expires_at_ms) \
             VALUES ($1, $2, CASE WHEN $3::BIGINT IS NULL THEN NULL \
             ELSE (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT + $3 END) \
             ON CONFLICT (key) DO UPDATE \
             SET value = EXCLUDED.value, expires_at_ms = EXCLUDED.expires_at_ms",
            self.table
        );
        sqlx::query(AssertSqlSafe(statement))
            .bind(key)
            .bind(bytes)
            .bind(ttl_ms)
            .execute(&self.pool)
            .await
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
        Ok(())
    }

    async fn remove(&self, key: &K) -> Result<(), KapeError> {
        let key = self.encode_key(key)?;
        let statement = format!("DELETE FROM {} WHERE key = $1", self.table);
        sqlx::query(AssertSqlSafe(statement))
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
        Ok(())
    }

    async fn get_many(&self, keys: &[&K]) -> Result<Vec<Lookup<V>>, KapeError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let keys = keys
            .iter()
            .map(|key| self.encode_key(key))
            .collect::<Result<Vec<_>, _>>()?;
        let statement = format!(
            "WITH requested(key, ord) AS (\
             SELECT * FROM UNNEST($1::BYTEA[]) WITH ORDINALITY\
             ) \
             SELECT entries.value, \
             CASE WHEN entries.expires_at_ms IS NULL THEN NULL \
             ELSE entries.expires_at_ms \
                  - (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT \
             END AS remaining_ms \
             FROM requested \
             LEFT JOIN {} AS entries ON entries.key = requested.key \
             ORDER BY requested.ord",
            self.table
        );
        let rows = sqlx::query(AssertSqlSafe(statement))
            .bind(keys)
            .fetch_all(&self.pool)
            .await
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
        rows.into_iter()
            .map(|row| {
                let bytes: Option<Vec<u8>> = row
                    .try_get("value")
                    .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
                let Some(bytes) = bytes else {
                    return Ok(Lookup::Miss);
                };
                let remaining_ms: Option<i64> = row
                    .try_get("remaining_ms")
                    .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
                crate::lookup::lookup(&self.codec, Some(&bytes), remaining_ms)
            })
            .collect::<Result<Vec<_>, PostgresBackendError<C::Error>>>()
            .map_err(Into::into)
    }

    async fn set_many(&self, items: &[BackendSetItem<'_, K, V>]) -> Result<(), KapeError> {
        if items.is_empty() {
            return Ok(());
        }
        let encoded = items
            .iter()
            .map(|item| {
                let ttl_ms = match item.ttl {
                    ResolvedTTL::Never => None,
                    ResolvedTTL::After(duration) => Some(duration_millis(duration)?),
                };
                Ok((
                    self.encode_key(item.key)?,
                    self.codec
                        .encode_value(item.value.as_ref())
                        .map_err(PostgresBackendError::Codec)?,
                    ttl_ms,
                ))
            })
            .collect::<Result<Vec<_>, PostgresBackendError<C::Error>>>()?;
        let statement = format!(
            "INSERT INTO {} (key, value, expires_at_ms) \
             VALUES ($1, $2, CASE WHEN $3::BIGINT IS NULL THEN NULL \
             ELSE (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT + $3 END) \
             ON CONFLICT (key) DO UPDATE \
             SET value = EXCLUDED.value, expires_at_ms = EXCLUDED.expires_at_ms",
            self.table
        );
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
        for (key, value, ttl_ms) in encoded {
            sqlx::query(AssertSqlSafe(statement.as_str()))
                .bind(key)
                .bind(value)
                .bind(ttl_ms)
                .execute(&mut *transaction)
                .await
                .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
        }
        transaction
            .commit()
            .await
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
        Ok(())
    }

    async fn has_many(&self, keys: &[&K]) -> Result<Vec<bool>, KapeError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let keys = keys
            .iter()
            .map(|key| self.encode_key(key))
            .collect::<Result<Vec<_>, _>>()?;
        let statement = format!(
            "WITH requested(key, ord) AS (\
             SELECT * FROM UNNEST($1::BYTEA[]) WITH ORDINALITY\
             ) \
             SELECT entries.key IS NOT NULL \
                    AND (entries.expires_at_ms IS NULL \
                         OR entries.expires_at_ms \
                            > (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT) \
                    AS present \
             FROM requested \
             LEFT JOIN {} AS entries ON entries.key = requested.key \
             ORDER BY requested.ord",
            self.table
        );
        let rows = sqlx::query(AssertSqlSafe(statement))
            .bind(keys)
            .fetch_all(&self.pool)
            .await
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
        rows.into_iter()
            .map(|row| {
                row.try_get("present")
                    .map_err(PostgresBackendError::<C::Error>::Sqlx)
                    .map_err(Into::into)
            })
            .collect()
    }

    async fn remove_many(&self, keys: &[&K]) -> Result<(), KapeError> {
        if keys.is_empty() {
            return Ok(());
        }
        let keys = keys
            .iter()
            .map(|key| self.encode_key(key))
            .collect::<Result<Vec<_>, _>>()?;
        let statement = format!("DELETE FROM {} WHERE key = ANY($1::BYTEA[])", self.table);
        sqlx::query(AssertSqlSafe(statement))
            .bind(keys)
            .execute(&self.pool)
            .await
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
        Ok(())
    }

    async fn clear(&self) -> Result<(), KapeError> {
        let prefix = frame_key(&self.namespace, &[])
            .ok_or(PostgresBackendError::<C::Error>::NamespaceTooLong)?;
        let prefix_len = i32::try_from(prefix.len())
            .map_err(|_| PostgresBackendError::<C::Error>::NamespaceTooLong)?;
        let statement = format!(
            "DELETE FROM {} WHERE substring(key from 1 for $2) = $1",
            self.table
        );
        sqlx::query(AssertSqlSafe(statement))
            .bind(prefix)
            .bind(prefix_len)
            .execute(&self.pool)
            .await
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
        Ok(())
    }

    async fn iterate(
        &self,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<IterationPage<K, V>, KapeError> {
        let prefix = frame_key(&self.namespace, &[])
            .ok_or(PostgresBackendError::<C::Error>::NamespaceTooLong)?;
        if cursor.is_some_and(|cursor| !cursor.starts_with(&prefix)) {
            return Err(PostgresBackendError::<C::Error>::InvalidCursor.into());
        }
        let prefix_len = i32::try_from(prefix.len())
            .map_err(|_| PostgresBackendError::<C::Error>::NamespaceTooLong)?;
        let fetch_limit = limit
            .checked_add(1)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(PostgresBackendError::<C::Error>::IterationLimitOverflow)?;
        let statement = format!(
            "SELECT key, value, \
             CASE WHEN expires_at_ms IS NULL THEN NULL \
             ELSE expires_at_ms \
                  - (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT \
             END AS remaining_ms \
             FROM {} \
             WHERE substring(key from 1 for $2) = $1 \
             AND ($3::BYTEA IS NULL OR key > $3) \
             ORDER BY key LIMIT $4",
            self.table
        );
        let rows = sqlx::query(AssertSqlSafe(statement))
            .bind(&prefix)
            .bind(prefix_len)
            .bind(cursor.map(<[u8]>::to_vec))
            .bind(fetch_limit)
            .fetch_all(&self.pool)
            .await
            .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
        let has_more = rows.len() > limit;
        let rows = rows.into_iter().take(limit);
        let mut entries = Vec::with_capacity(limit);
        let mut last_key = None;
        for row in rows {
            let framed_key: Vec<u8> = row
                .try_get("key")
                .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
            let encoded_key = framed_key
                .strip_prefix(prefix.as_slice())
                .ok_or(PostgresBackendError::<C::Error>::InvalidCursor)?;
            let bytes: Vec<u8> = row
                .try_get("value")
                .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
            let remaining_ms: Option<i64> = row
                .try_get("remaining_ms")
                .map_err(PostgresBackendError::<C::Error>::Sqlx)?;
            entries.push(crate::lookup::iteration_entry(
                &self.codec,
                encoded_key,
                &bytes,
                remaining_ms,
            )?);
            last_key = Some(framed_key);
        }
        Ok(IterationPage {
            entries,
            next_cursor: has_more.then_some(last_key).flatten(),
        })
    }

    async fn disconnect(&self) -> Result<(), KapeError> {
        self.pool.close().await;
        Ok(())
    }
}

fn duration_millis<E>(duration: Duration) -> Result<i64, PostgresBackendError<E>> {
    let millis = duration.as_millis();
    let millis = if millis == 0 { 1 } else { millis };
    i64::try_from(millis).map_err(|_| PostgresBackendError::TTLOverflow)
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

fn frame_key(namespace: &[u8], key: &[u8]) -> Option<Vec<u8>> {
    let namespace_len = u64::try_from(namespace.len()).ok()?;
    let mut framed = Vec::with_capacity(8 + namespace.len() + key.len());
    framed.extend_from_slice(&namespace_len.to_be_bytes());
    framed.extend_from_slice(namespace);
    framed.extend_from_slice(key);
    Some(framed)
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

    #[test]
    fn namespace_frame_has_no_delimiter_collision() {
        assert_ne!(
            frame_key(b"a", b"b:c").unwrap(),
            frame_key(b"a:b", b"c").unwrap()
        );
    }
}
