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
current crates.io name checks, successful package inspection, and configured
Trusted Publishing.

## Release checklist

Before the first public release:

1. Recheck `kape`, `kape-memory`, `kape-redis`, and `kape-postgres` on crates.io;
   a search result is not a reservation.
2. Inspect each `cargo package --list` result and build each package from its
   generated archive in dependency order.
3. Verify the declared Rust 1.96 MSRV before publishing.
4. Rerun Redis and PostgreSQL live contracts after repository-facing changes.
5. Confirm the public guide URL before replacing the repository homepage.
6. Configure GitHub Pages and crates.io Trusted Publishing.
7. Add a publish job only after the release boundary is explicitly approved.

`kape-testkit` remains `publish = false`.
