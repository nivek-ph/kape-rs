# Development and releases

## Local verification

The repository's standard checks are:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
mdbook build
mdbook test
cargo package -p kape -p kape-memory -p kape-redis -p kape-postgres --allow-dirty
```

`cargo package --allow-dirty` is useful before the directory has a clean Git
worktree. CI runs the stricter command without `--allow-dirty`. Examples and
`kape-testkit` set `publish = false` and are intentionally omitted from package
verification.

## GitHub Actions

The repository configuration provides:

- `CI` for formatting, Clippy, tests, isolated Redis/PostgreSQL contracts,
  feature combinations, rustdoc, mdBook, and package verification;
- `Deploy mdBook` for tag-triggered or manual GitHub Pages deployment;
- `Prepare release PR` for a manual release-plz version PR;
- Dependabot updates for Cargo and GitHub Actions dependencies.

The release workflow deliberately does not publish crates. Publishing requires
a real repository, final package metadata, current crates.io name checks, and
configured Trusted Publishing.

## Release checklist

Before the first public release:

1. Initialize the intended Git repository and create its real remote.
2. Add the verified repository URL to workspace package metadata and mdBook.
3. Recheck every crate name on crates.io; a search result is not a reservation.
4. Verify the declared Rust 1.96 MSRV before publishing.
5. Rerun Redis and PostgreSQL live contracts after repository-facing changes.
6. Configure GitHub Pages and crates.io Trusted Publishing.
7. Add a publish job only after the release boundary is explicitly approved.

`kape-testkit` remains `publish = false`.
