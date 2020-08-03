"""
A read-only interface to an Illumina run directory.

See the Run class for usage.
"""

import warnings
import logging
import time
from pathlib import Path
from .util import load_xml, load_rta_complete
from .alignment import Alignment

LOGGER = logging.getLogger(__name__)

class Run:
    """A single Illumina sequencing run, based on a directory tree."""

    def __init__(
            self,
            path,
            strict=None,
            alignment_callback=None,
            min_alignment_dir_age=None):
        # Setup run path
        path = Path(path).resolve()
        self.path = path
        self.alignment_callback = alignment_callback
        self.min_alignment_dir_age = min_alignment_dir_age

        self.invalid = False
        self.rundata = {}
        # RunInfo.xml is one of the first files to show up in a run directory,
        # so we'll use that to define a Run (finished or not).
        try:
            self.rundata["run_info"] = load_xml(path/"RunInfo.xml")
        except FileNotFoundError:
            msg = 'Not a recognized Illumina run: "%s"' % path
            # strict behavior: raise an error
            if strict or strict is None:
                raise ValueError(msg)
            else:
                warnings.warn(msg)
                self.invalid = True
                return
        info_run_id = self.run_info.find('Run').attrib["Id"]
        if info_run_id != path.name:
            if strict:
                args = (path.name, info_run_id)
                msg = 'Run directory does not match Run ID: %s / %s' % args
                warnings.warn(msg)

        # Load in RTA completion status and available alignment directories.
        self.rundata["rta_complete"] = None
        self.alignments = []
        self.refresh()
        # CompletedJobInfo.xml should be there if a workflow (job) completed,
        # like GenerateFASTQ.  It looks like this file is just copied over at
        # the end of the most recent job from the Alignment sub-folder (or
        # written there directly, for newer MiSeqs).
        try:
            self.rundata["completed_job_info"] = load_xml(path/"CompletedJobInfo.xml")
        except FileNotFoundError:
            self.rundata["completed_job_info"] = None

    def refresh(self):
        """Check for run completion and any new or completed alignments.

        Aside from RTAComplete.txt and the Alignment directories, nothing else
        is checked.  If other files may have changed, instatiate a new Run
        object."""
        if not self.rta_complete:
            fpath = self.path/"RTAComplete.txt"
            self.rundata["rta_complete"] = load_rta_complete(fpath)
        self._refresh_alignments()

    def _refresh_alignments(self):
        # First refresh any existing alignments
        for aln in self.alignments:
            aln.refresh()
        # Load from expected paths, using patterns for MiSeq and MiniSeq
        al_loc1 = self.path.glob("Data/Intensities/BaseCalls/Alignment*")
        al_loc2 = self.path.glob("Alignment*")
        al_loc = list(al_loc1) + list(al_loc2)
        # Filter out those already loaded and process new ones
        al_loc_known = [aln.path for aln in self.alignments]
        is_new = lambda d: not d in al_loc_known
        al_loc = [d for d in al_loc if is_new(d)]
        aln = [self._alignment_setup(d) for d in al_loc]
        # Filter out any blanks.  These were either not recognized as
        # alignments or skipped as too new and potentially unfinished (and
        # logged appropriately) below.
        aln = [a for a in aln if a]
        # Merge new ones into existing list
        self.alignments += aln

    def _alignment_setup(self, path):
        # Try loading an alignment directory, but skip if the alignment
        # directory looks too new on disk (according to min_alignment_dir_age)
        # or just throw a warning and return None if it doesn't look like an
        # Alignment.  This should handle not-yet-complete Alignment directories
        # on disk while avoiding spurious warnings.
        min_age = self.min_alignment_dir_age
        time_change = path.stat().st_ctime
        time_now = time.time()
        if min_age is not None and (time_now - time_change < min_age):
            msg = "skipping alignment; timestamp too new:.../%s/.../%s" % (
                self.path.name, path.name)
            LOGGER.debug(msg)
            return None
        try:
            aln = Alignment(path, self, self.alignment_callback)
        except ValueError:
            warnings.warn("Alignment not recognized: %s" % path)
            return None
        else:
            return aln

    @property
    def run_id(self):
        """The run identifier as defined in the RunInfo XML."""
        return self.run_info.find('Run').attrib["Id"]

    @property
    def complete(self):
        """Is the run complete?"""
        return self.rta_complete is not None

    @property
    def run_info(self):
        """RunInfo.xml data."""
        return self.rundata["run_info"]

    @property
    def rta_complete(self):
        """RTAComplete.txt data."""
        return self.rundata["rta_complete"]

    @property
    def completed_job_info(self):
        """CompletedJobInfo.xml data."""
        return self.rundata["completed_job_info"]

    @property
    def flowcell(self):
        """Flow cell ID."""
        return self.run_info.find("./Run/Flowcell").text
