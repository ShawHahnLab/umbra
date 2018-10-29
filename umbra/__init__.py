"""
Package to help manage Illumina sequencing runs.

Brief package structure overview:

processor.IlluminaProcessor can load Illumina run data from disk, dispatch
handlers in parallel for new finished runs, and report processing status.  An
instance of this object does the public-facing work when the package is called
as a script.  project.ProjectData handles processing for a subset of a single
run applicable to a single project (identified with a simple string).
mailer.Mailer provides simple email-sending support.  box_uploader.BoxUploader
provides support for uploading individual files to a folder via the Box API.
The illumina sub-package contains classes representing some Illumina data
structures on disk.  The experiment module contains helper functions for
matching the Experiment field from a sample sheet with a matching set of
metadata on disk.
"""
