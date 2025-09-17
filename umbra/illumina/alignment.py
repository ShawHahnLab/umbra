"""
A read-only interface to an Illumina "Alignment" directory within a run.

DEPRECATED.  Use the equivalent functionality in the analysis module instead.
"""

import gzip
import re
from pathlib import Path
from datetime import datetime
from .util import load_sample_sheet, load_xml, load_checkpoint, load_sample_filenames

class Alignment:
    """An "Alignment" (FASTQ generation) within a run.

    This is the output of the FASTQGeneration workflow that runs on the
    sequencer, consisting of a specific sample sheet and set of demultiplexed
    fastq.gz files. This can happen multiple times per run, even though the
    .bcl files and other output from Real Time Analysis are the same."""

    def __init__(self, path, run=None, completion_callback=None):
        self.run = run
        # Absolute path to the Alignment directory itself
        path = Path(path).resolve()
        self.path = path
        self.completion_callback = completion_callback
        # Absolute path to various files within the Alignment directory
        self.paths = {}
        try:
            try:
                # MiSeq, directly in Alignment folder
                self.paths["sample_sheet"] = (path/"SampleSheetUsed.csv").resolve(strict=True)
                self.paths["fastq"] = (path / "..").resolve()
                self.paths["checkpoint"] = path/"Checkpoint.txt"
                self.paths["job_info"] = path/"CompletedJobInfo.xml"
            except FileNotFoundError:
                # MiniSeq, within timstamped subfolder
                filt = lambda p: re.match("[0-9]{8}_[0-9]{6}", p.name)
                dirs = [d for d in path.glob("*") if d.is_dir() and filt(d)]
                # If there are no subdirectories this doesn't look like a MiniSeq alignment
                if not dirs:
                    raise ValueError('Not a recognized Illumina alignment: "%s"' % path)
                try:
                    self.paths["sample_sheet"] = (dirs[0]/"SampleSheetUsed.csv").resolve(strict=True)
                # If both possible sample sheet paths threw FileNotFound, we won't
                # consider this input path to be an alignment directory.
                except FileNotFoundError:
                    raise ValueError('Not a recognized Illumina alignment: "%s"' % path)
                self.paths["fastq"] = dirs[0] / "Fastq"
                self.paths["checkpoint"] = dirs[0]/"Checkpoint.txt"
                self.paths["job_info"] = dirs[0]/"CompletedJobInfo.xml"
        except PermissionError as err:
            # I'm seeing this happen sporadically with the updated MiSeq
            # software.  Looks like the permissions are restricted temporarily,
            # I'm thinking until the alignment dir is ready?
            raise ValueError("Permission error accessing %s" % path) from err
        self.sample_sheet = load_sample_sheet(self.paths["sample_sheet"])
        # This doesn't always exist.  On our MiniSeq and one of two MiSeqs it's
        # always written, but on a newer MiSeq we only have the copy saved to
        # the root of the run directory for the most recent alignment.
        # (The Run class also has this idiom to load one if it can be found.)
        try:
            self.completed_job_info = load_xml(self.paths["job_info"])
        except FileNotFoundError:
            self.completed_job_info = None
        self.__fastq_first_checked = None
        self.refresh()

    def refresh(self):
        """Reload alignment status from disk.

        If the alignment has just completed, and a callback function was
        provided during instantiation, call it."""
        self.__path_attrs = load_sample_filenames(self.paths["fastq"])
        if (self.run is None or self.run.complete) and not self.complete:
            self.checkpoint = load_checkpoint(self.paths["checkpoint"])
            if self.complete:
                try:
                    if not self.__fastq_first_checked:
                        self.__fastq_first_checked = datetime.now()
                    self.sample_paths()
                except FileNotFoundError:
                    # If FASTQ files are missing, delay declaring the Alignment
                    # complete for a while (up to 30 minutes) in case it's a file
                    # transfer issue.
                    # This is a kludgy attempted fix to buy us time while I
                    # figure out the root cause of these temporarily "missing"
                    # fastq files.
                    delta = self.__fastq_first_checked - datetime.now()
                    if not self.__fastq_first_checked or delta.seconds < 1800:
                        self.checkpoint = None
                        return
                if self.completion_callback:
                    self.completion_callback(self)

    @property
    def index(self):
        """Zero-indexed position of this alignment in the Run's list"""
        # Of course, if we use this during instantiation of this Alignment,
        # this won't be in the list yet!  Assuming that each alignment is
        # appended as soon as it's created, we should be able safely assume
        # that this will be the next one in the list.
        if self.run:
            try:
                idx = self.run.alignments.index(self)
            except ValueError:
                idx = len(self.run.alignments)
            return idx
        return None

    @property
    def error(self):
        """Text of the Error entry in CompletedJobInfo.xml

        None if there is no XML file or Error entry.
        """
        if self.completed_job_info:
            elem = self.completed_job_info.find("Error")
            if elem is not None:
                return elem.text
        return None

    @property
    def complete(self):
        """Is the alignment complete?"""
        # This is true when the checkpoint file reaches stage 3 or whatever
        # Illumina probably calls it
        checkpoint = getattr(self, "checkpoint", None)
        if checkpoint:
            return checkpoint[0] == 3
        return False

    @property
    def experiment(self):
        """Experiment name given in sample sheet."""
        hdr = self.sample_sheet["Header"]
        # I've seen both versions across multiple MiSeqs, but just the space
        # one for MiniSeq, where the machine creates its own sample sheet from
        # a separate input spreadsheet.  Maybe Illumina's parsing is flexible
        # but officially it prefers the space?
        exp = hdr.get("Experiment_Name") or hdr.get("Experiment Name")
        return exp

    @property
    def sample_numbers(self):
        """Ordered list of all sample numbers (indexed from one).

        Note that the sample number (the integer after the "S" in filenames) is
        not the same thing as the Sample_ID values given in a sample sheet,
        though they may happen to have the same values in a run."""
        num_range = range(len(self.sample_sheet["Data"]))
        nums = [i+1 for i in num_range]
        return nums

    @property
    def sample_names(self):
        """Ordered list of all sample names."""
        names = [row.get("Sample_Name") for row in self.sample_sheet["Data"]]
        return names

    @property
    def samples(self):
        """A copy of sample data from the sample sheet.

        This is a list of dictionaries using Sample_ID as the keys.  See
        illumina.util.load_sample_sheet for more info."""
        data = self.sample_sheet["Data"]
        newdata = [row.copy() for row in data]
        return newdata

    def sample_paths_for_num(self, sample_num, strict=True):
        """Locate files (absolute Paths) for the given sample number on disk."""
        fps = []
        if len(self.sample_sheet["Reads"]) > 1:
            reads = ["R1", "R2"]
        else:
            reads = ["R1"]
        for attrs in self.__path_attrs:
            if attrs["sample_num"] == sample_num and attrs["read"] in reads:
                # This replicates the existing behavior where we only deliver
                # R1 and then also R2 if expected, and never I1 or I2.  This
                # should be changed at some point to account for I1 and I2
                # though.
                fpath = Path(attrs["path"]).resolve(strict=strict)
                fps.append(fpath)
        if strict and len(fps) < len(reads):
            # Is this legit?  The previous approach constructed filenames and
            # then tried to access them, so the error came from the OS.  Can I
            # instead raise it myself when I don't actually have a specific
            # path I'm complaining about?  Seems to work.
            raise FileNotFoundError
        return fps

    def sample_paths(self, strict=True):
        """Create dictionary mapping each sample name to list of file paths.

        NOTE: Sample names are not guaranteed to be unique in a given
        alignment, so we should not rely on this to behave as expected.  This
        function will be removed in a later release.
        """
        sample_paths = {}
        for s_num, s_name in zip(self.sample_numbers, self.sample_names):
            sps = self.sample_paths_for_num(s_num, strict)
            sample_paths[s_name] = sps
        return sample_paths

    def _make_dummy_files(self):
        """Create blank fastq.gz files in place of any missing ones."""
        # This is used in building test directories.
        s_paths = self.sample_paths(strict=False)
        for paths in s_paths.values():
            for path in paths:
                if not path.exists():
                    with gzip.open(path, "wb"):
                        pass
