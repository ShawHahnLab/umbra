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

from . import config
CONFIG = config.layer_configs([config.path_for_config()])

def __deduce_version():
    """Return version string for this package, if installed.

    This infers the version originally defined in setup.py, but only if it can
    find an installed package and the filesystem path for the loaded package
    agrees with it.
    """
    from importlib.metadata import version, files, PackageNotFoundError
    from pathlib import Path
    try:
        # Is there an installed package matching this package name, *and* does
        # that package refer to this very file we're currently in?  If so,
        # return that version string, but in any other case, return an empty
        # string.
        ver = version(__package__)
        this = [p for p in files(__package__) if Path(__file__).samefile(p.locate())]
        if this:
            return ver
    except PackageNotFoundError:
        pass
    return ""

__version__ = __deduce_version()
