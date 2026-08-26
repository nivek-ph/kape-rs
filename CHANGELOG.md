# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-08-26

### Added

- Allow `Cache::set_many` to accept any iterable of items convertible to
  `SetItem`, including `(key, value, ttl)` tuples and borrowed `SetItem` values.

[Unreleased]: https://github.com/nivek-ph/kape-rs/compare/kape-v0.1.1...HEAD
[0.1.1]: https://github.com/nivek-ph/kape-rs/compare/kape-v0.1.0...kape-v0.1.1
