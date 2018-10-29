# Umbra

A Python package and executable for Linux to help manage Illumina sequencing
runs.  *The below and included Python docstrings are a start, but this draft
version requires much more documentation and testing.*

Umbra will watch a directory for incoming sequencing runs and dispatch a number
of parallel processors to handle new run data.  Automated processing tasks
include adapter trimming, read interleaving, basic contig assembly, uploading
finished datasets to [Box], and alerting end users via email.  A CSV report is
refreshed on disk that summarizes processing status.  A readonly mode allows
for watching/reporting without processing.

An `illumina` sub-package provides some basic parsers for various Illumina file
and directory fomats that can be used independently of the automated processing
functionality.

Requirements:

 * Python libraries: Biopython, BoxSDK, PyYAML
 * Box API credentials for automated uploads (optional)
 * Access to an SMTP mail server for sending mail, for example, a local postfix
   installation (optional)

Limitations/assumptions:

 * Tested with MiniSeq and MiSeq output
 * Assumes the GenerateFASTQ workflow is enabled on the sequencer

[Box]: https://www.box.com/
