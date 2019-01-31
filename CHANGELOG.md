# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## 0.0.11 - 2019-01-31

## 0.0.10 - 2019-01-25

## 0.0.9 - 2019-01-25

## 0.0.8 - 2019-01-25

## 0.0.8 - 2019-01-25

## 0.0.8 - 2019-01-25

## 0.0.6 - 2019-01-20
### Added
- Add `alter topic` command for increasing partition count and editing topic configs

### Changed
- Sort result of `kafkactl get topics`
- `consume` now uses a simpler consumer without consumerGroup.
- Changed name of global flags to avoid collisions with local flags

### Fixed
- Do not auto-create topics when using `describe`, `consume` or `produce`

## 0.0.1 - 2018-12-12
### Added
- Initial version

