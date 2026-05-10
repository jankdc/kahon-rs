# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.1](https://github.com/jankdc/kahon-rs/compare/v0.6.0...v0.6.1) - 2026-05-10

### Fixed

- emit extension header adjacent to payload type-code byte ([#16](https://github.com/jankdc/kahon-rs/pull/16))

## [0.6.0](https://github.com/jankdc/kahon-rs/compare/v0.5.0...v0.6.0) - 2026-05-08

### Added

- migrate sum tags to extension ([#14](https://github.com/jankdc/kahon-rs/pull/14))

## [0.5.0](https://github.com/jankdc/kahon-rs/compare/v0.4.0...v0.5.0) - 2026-05-08

### Added

- Update writer to match new spec ([#12](https://github.com/jankdc/kahon-rs/pull/12))

## [0.4.0](https://github.com/jankdc/kahon-rs/compare/v0.3.1...v0.4.0) - 2026-05-07

### Added

- add RawWriter, lift Writer to typestate, and split out trailer/frame

## [0.3.1](https://github.com/jankdc/kahon-rs/compare/v0.3.0...v0.3.1) - 2026-05-05

### Added

- pivot to more ergonomic try_write
- reduce clones
- add initial checkpoint feature

### Other

- add ci + apply test and lint
- cleanup

## [0.3.0](https://github.com/jankdc/kahon-rs/compare/v0.2.0...v0.3.0) - 2026-04-29

### Added

- prune unreachable WriteError variants

## [0.2.0](https://github.com/jankdc/kahon-rs/compare/v0.1.1...v0.2.0) - 2026-04-29

### Added

- loosen up duplicate key requirements according to spec + more tests
