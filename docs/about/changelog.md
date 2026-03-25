# Changelog

All notable changes to DecentDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- SQL window enhancements: `NTILE`, `PERCENT_RANK`, `CUME_DIST`, aggregate window functions, and frame-aware execution for `ROWS` and supported `RANGE` bounds.
- Statistical and ordered-set aggregates: `ARRAY_AGG`, `MEDIAN`, `PERCENTILE_CONT ... WITHIN GROUP`, and `PERCENTILE_DISC ... WITHIN GROUP`.
- Trigonometric math functions: `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `PI`, `DEGREES`, `RADIANS`, and `COT`.
- Conditional scalar functions: `GREATEST`, `LEAST`, and `IIF`.
- DML enhancements: `UPDATE ... RETURNING` and `DELETE ... RETURNING`.
- Subquery comparison operators: `expr op ANY/SOME (subquery)` and `expr op ALL (subquery)`.
- Regex comparison operators: `~`, `~*`, `!~`, `!~*`.

### Changed
- Refreshed repository documentation to present DecentDB as the current Rust engine and binding ecosystem.
- Clarified release/versioning docs around the current public `v2.x` release line.
