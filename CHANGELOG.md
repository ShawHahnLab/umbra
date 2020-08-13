# Changelog

## dev

### Changed

 * Suppress duplicate log messages for skipped runs ([#85])

### Fixed

 * Specify minimum versions for dependencies during install ([#84])

[#85]: https://github.com/ShawHahnLab/umbra/pull/85
[#84]: https://github.com/ShawHahnLab/umbra/pull/84

## 0.0.2 - 2020-08-04

### Added

 * Support for custom task classes ([#56])
 * Timeout feature for manual and geneious tasks ([#66])
 * Flow cell ID property for Illumina run objects ([#75])

### Changed

 * Append flow cell ID from each run to project workdir names ([#76])
 * Ignore empty rows and columns when parsing metadata.csv files ([#73])
 * Use chunked Box.com uploader for more reliable uploads of large files ([#69])
 * Handle missing Experiment Name in sample sheets ([#59])

### Fixed

 * Catch and log errors parsing metadata.csv files during project setup ([#74])
 * Handle missing FASTQ file case during project setup ([#63])

[#76]: https://github.com/ShawHahnLab/umbra/pull/76
[#75]: https://github.com/ShawHahnLab/umbra/pull/75
[#74]: https://github.com/ShawHahnLab/umbra/pull/74
[#73]: https://github.com/ShawHahnLab/umbra/pull/73
[#69]: https://github.com/ShawHahnLab/umbra/pull/69
[#66]: https://github.com/ShawHahnLab/umbra/pull/66
[#63]: https://github.com/ShawHahnLab/umbra/pull/63
[#59]: https://github.com/ShawHahnLab/umbra/pull/59
[#56]: https://github.com/ShawHahnLab/umbra/pull/56

## 0.0.1 - 2019-06-03

First named release
