# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Changed
- Upgrade build to sbt 2.x (bare settings, sbt 2 compatible plugins)
- Rework script support: parse SQL scripts into structured `Statement` objects with comments and directives, add `@loop` directive for batched `INSERT ... SELECT`, move parsing into `Parser` and execution into `Runner`
- Replace interactive `Sql` shell with one-shot `Main` and add `sql.sh` / `sql.ps1` launchers
- Update `beangle-commons` to 6.2.2 in launcher scripts

## [1.1.10] - 2026-07-24

### Fixed
- Handle empty string in `PostgresCsvReader`

### Changed
- Update to parent 0.15.14

## [1.1.9] - 2026-06-15

### Changed
- Update to `beangle-commons` 6.2.0

## [1.1.8] - 2026-02-09

### Fixed
- Serializer to xml and head

## [1.1.7] - 2026-02-05

### Changed
- Update to beangle xml module

## [1.1.6] - 2026-01-27

### Changed
- Update to `beangle-commons` 5.8.1

## [1.1.5] - 2026-01-13

### Changed
- MySQL boolean mapping from `bit` to `tinyint(1)`

## [1.1.4] - 2025-11-24

### Added
- Support `poolName` and `applicationName` in datasource config
- Add default max and min pool size to datasource config

### Changed
- Default `minimumIdle` from 0 to 1
- Replace `contains` with `containKey`

## [1.1.3] - 2025-10-27

### Changed
- Update to parent 0.15.0

## [1.1.2] - 2025-10-14

### Added
- Support `TableType`

### Changed
- Update to sbt 1.11.7

## [1.1.1] - 2025-10-11

### Added
- `resolveCode` with scale parameter

### Fixed
- PostgreSQL test

## [1.1.0] - 2025-09-04

### Added
- `setParam` in `JdbcExecutor`

## [1.0.13] - 2025-08-23

### Added
- JSON type support in dialect
- Extract and bind JSON

## [1.0.12] - 2025-07-27

### Changed
- Update to parent 0.14.1

## [1.0.11] - 2025-05-29

### Added
- Add url to datasource props

### Changed
- Update to Scala 3.3.6

## [1.0.10] - 2025-02-19

### Improved
- Better `setNull` support

## [1.0.9] - 2025-02-11

### Changed
- Update to `beangle-commons` 5.6.26

## [1.0.8] - 2025-01-15

### Changed
- Update to `beangle-commons` 5.6.25 and parent 0.13.5

## [1.0.7] - 2025-01-02

### Added
- COPY support
- no-text-type support

## [1.0.6] - 2024-11-17

### Changed
- Update to `beangle-commons` 5.6.22

## [1.0.5] - 2024-11-13

### Changed
- Update to `beangle-commons` 5.6.21

## [1.0.4] - 2024-10-01

### Changed
- Update to `beangle-commons` 5.6.19

## [1.0.3] - 2024-07-23

### Changed
- Update default value

## [1.0.2] - 2024-07-17

### Changed
- Version bump

## [1.0.1] - 2024-04-20

### Changed
- Update to parent 0.12.1

## [1.0.0] - 2024-04-07

### Added
- Initial release
