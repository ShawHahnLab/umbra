# Changelog

## dev

### Added

 * `illumina.util.load_rta_complete` now handles the syntax used by NextSeq and
   MiSeq i100 Plus software for RTAComplete.txt ([#132])

[#132]: https://github.com/ShawHahnLab/umbra/pull/132

## 0.0.6 - 2025-07-10

### Added

 * "mergereads" task for merging forward and reverse reads using
   [PEAR](https://cme.h-its.org/exelixis/web/software/pear/) ([#121])

### Changed

 * Configured Alignment setup to skip and retry later if a permission error
   occurs, and Alignment refresh to wait up to 10 minutes for .fastq.gz files
   to appear
 * Reorganized Box client and updated auth URL to search for in server logs
   when authenticating with OAuth ([#120])

### Fixed

 * No longer crash when encountering runs missing the `Sample_Name` column in
   their sample sheets ([#129])
 * Ignore leading and/or trailing whitespace in the Contact column when loading
   an experiment metadata CSV, so all-whitespace entries are now treated as
   missing contact info rather than causing a crash during parsing ([#128])

[#129]: https://github.com/ShawHahnLab/umbra/pull/129
[#128]: https://github.com/ShawHahnLab/umbra/pull/128
[#127]: https://github.com/ShawHahnLab/umbra/pull/127
[#121]: https://github.com/ShawHahnLab/umbra/pull/121
[#120]: https://github.com/ShawHahnLab/umbra/pull/120

## 0.0.5 - 2021-01-19

### Added

 * `error` property for `illumina.alignment.Alignment` class ([#104])
 * BCL stats file parsing functions for applicable Illumina runs ([#106])

### Changed

 * Use faster re-tries for small file uploads to Box ([#111])
 * Use fully-resolved paths for alignment directory tracking in Run.refresh
   ([#111])
 * Reorganize unit tests for speed and simplicity ([#111])

### Fixed

 * Only process an alignment when its containing run is marked complete
   ([#108])
 * Handle non-unicode characters in CSV files ([#105], [#115])

[#115]: https://github.com/ShawHahnLab/umbra/pull/115
[#111]: https://github.com/ShawHahnLab/umbra/pull/111
[#108]: https://github.com/ShawHahnLab/umbra/pull/108
[#106]: https://github.com/ShawHahnLab/umbra/pull/106
[#105]: https://github.com/ShawHahnLab/umbra/pull/105
[#104]: https://github.com/ShawHahnLab/umbra/pull/104

## 0.0.4 - 2020-09-08

### Added

 * Unit tests for each individual task class ([#99])

### Changed

 * Always read task names as lowercase when parsing metadata.csv files ([#95])
 * Parse fastq.gz files per sample from disk rather than predicting from sample
   name alone ([#94])

### Fixed

 * Correct parsing of contig numbers in task_assemble to include all digits
   ([#100])
 * Correct parsing for Illumina's Checkpoint.txt files to work with
   incomplete alignment outputs ([#97])

[#100]: https://github.com/ShawHahnLab/umbra/pull/100
[#99]: https://github.com/ShawHahnLab/umbra/pull/99
[#97]: https://github.com/ShawHahnLab/umbra/pull/97
[#95]: https://github.com/ShawHahnLab/umbra/pull/95
[#94]: https://github.com/ShawHahnLab/umbra/pull/94

## 0.0.3 - 2020-08-19

### Added

 * `--version` argument to command-line interface ([#87])

### Changed

 * Use Unix-style line endings in report CSV ([#86])
 * Suppress duplicate log messages for skipped runs ([#85])

### Fixed

 * Suppress excessive logging for Box file uploads ([#89])
 * Specify minimum versions for dependencies during install ([#84])

[#89]: https://github.com/ShawHahnLab/umbra/pull/89
[#87]: https://github.com/ShawHahnLab/umbra/pull/87
[#86]: https://github.com/ShawHahnLab/umbra/pull/86
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
