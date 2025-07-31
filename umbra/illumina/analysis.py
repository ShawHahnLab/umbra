"""
A read-only interface to an Illumina Analysis (formerly "Alignment") directory
within a run.
"""

import re
from pathlib import Path
from datetime import datetime
from abc import ABC, abstractmethod
from collections import defaultdict
from .util import load_sample_sheet, load_checkpoint, load_sample_filenames

def init_analysis_obj(path, run, completion_callback=None):
    """Initialize a new Analysis object, inferring class from the given run

    The Analysis directories for a run (Alignment, for old MIseq and MIniSeq)
    are structured differently depending on the instrument type.  This helper
    will instantiate the Analysis class corresponding to the instrument type of
    the run object provided.
    """
    analysis_by_instr = {
    #"MiSeq": AnalysisMiSeq,
    #"MiniSeq": AnalysisMiniSeq,
    "MiSeq": AnalysisClassic,
    "MiniSeq": AnalysisClassic,
    "MiSeqi100Plus": AnalysisMiSeqi100Plus,
    "NextSeq 2000": AnalysisNextSeq2000,
    }
    try:
        cls = analysis_by_instr[run.instrument_type]
    except KeyError as err:
        raise ValueError(
            f"Instrument type \"{run.instrument_type}\" not "
            "recognized for Analysis setup") from err
    analysis = cls(path, run, completion_callback)
    return analysis


class Analysis(ABC):
    """An Analysis output (generally FASTQ generation) within a run.

    This is the output of the FASTQGeneration workflow that runs on the
    sequencer, consisting of a specific sample sheet and set of demultiplexed
    fastq.gz files. This can happen multiple times per run, even though the
    .bcl files and other output from Real Time Analysis are the same.

    This class only exists so it can be sub-classed by the applicable
    instrument-specific classes below.
    """

    @abstractmethod
    def refresh(self):
        """Refresh the analysis data from disk

        If the alignment has just completed, and a callback function was
        provided during instantiation, call it."""

    @property
    def index(self):
        """Zero-indexed position of this alignment in the Run's list"""
        # Of course, if we use this during instantiation of this Analysis,
        # this won't be in the list yet!  Assuming that each alignment is
        # appended as soon as it's created, we should be able safely assume
        # that this will be the next one in the list.
        if self.run:
            try:
                idx = self.run.analyses.index(self)
            except ValueError:
                idx = len(self.run.analyses)
            return idx
        return None

    @property
    @abstractmethod
    def complete(self):
        """Is the analysis complete?"""

    @property
    @abstractmethod
    def run(self):
        """Associated run object"""

    @property
    @abstractmethod
    def path(self):
        """Associated absolute path on disk"""

    @property
    @abstractmethod
    def sample_sheet_path(self):
        """Path to sample sheet"""

    @property
    @abstractmethod
    def sample_sheet(self):
        """Loaded sample sheet data"""

    @property
    def run_name(self):
        """RunName from the SampleSheet (or Experiment Name, for the v1 case)"""
        hdr = self.sample_sheet["Header"]
        # RunName: v2 sample sheet as in MiSeq i100 Plus and NextSeq 2000
        # Experiment Name: MiniSeq, one MiSeq
        # Experiment_Name: another MiSeq
        for field in ("RunName", "Experiment_Name", "Experiment Name"):
            if (val := hdr.get(field)):
                return val
        return None

    @property
    def experiment(self):
        """Alias for run_name"""
        return self.run_name

    @abstractmethod
    def sample_paths(self):
        """Create dictionary mapping each sample name to list of file paths.

        If Sample_Name is not present in the sample sheet, Sample_ID is used
        instead.

        NOTE: Sample names are not guaranteed to be unique in a given
        alignment, so we should not rely on this to behave as expected.  This
        function will be removed in a later release.
        """


class AnalysisClassic(Analysis):
    """The Analysis logic for MiSeq and MiniSeq from the former Alignment class."""

    def __init__(self, path, run=None, completion_callback=None):
        self._run = run
        # Absolute path to the Alignment directory itself
        path = Path(path).resolve()
        self._path = path
        self._completion_callback = completion_callback
        # Absolute path to various files within the Alignment directory
        self._paths = {}
        try:
            try:
                # MiSeq, directly in Alignment folder
                self._paths["sample_sheet"] = (path/"SampleSheetUsed.csv").resolve(strict=True)
                self._paths["fastq"] = (path / "..").resolve()
                self._paths["checkpoint"] = path/"Checkpoint.txt"
            except FileNotFoundError as err:
                # MiniSeq, within timstamped subfolder
                filt = lambda p: re.match("[0-9]{8}_[0-9]{6}", p.name)
                dirs = [d for d in path.glob("*") if d.is_dir() and filt(d)]
                # If there are no subdirectories this doesn't look like a MiniSeq alignment
                if not dirs:
                    raise ValueError(f'Not a recognized Illumina alignment: "{path}"') from err
                try:
                    self._paths["sample_sheet"] = (
                        dirs[0]/"SampleSheetUsed.csv").resolve(strict=True)
                # If both possible sample sheet paths threw FileNotFound, we won't
                # consider this input path to be an alignment directory.
                except FileNotFoundError as err2:
                    raise ValueError(f'Not a recognized Illumina alignment: "{path}"') from err2
                self._paths["fastq"] = dirs[0] / "Fastq"
                self._paths["checkpoint"] = dirs[0]/"Checkpoint.txt"
        except PermissionError as err:
            # I'm seeing this happen sporadically with the updated MiSeq
            # software.  Looks like the permissions are restricted temporarily,
            # I'm thinking until the alignment dir is ready?
            raise ValueError(f"Permission error accessing {path}") from err
        self._sample_sheet = load_sample_sheet(self._paths["sample_sheet"])
        self.__fastq_first_checked = None
        self.refresh()

    path = property(lambda self: self._path)
    sample_sheet = property(lambda self: self._sample_sheet)
    sample_sheet_path = property(lambda self: self._paths["sample_sheet"])
    run = property(lambda self: self._run)

    def refresh(self):
        """Reload alignment status from disk.

        If the alignment has just completed, and a callback function was
        provided during instantiation, call it."""
        self.__path_attrs = load_sample_filenames(self._paths["fastq"])
        if (self.run is None or self.run.complete) and not self.complete:
            self.checkpoint = load_checkpoint(self._paths["checkpoint"])
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
                if self._completion_callback:
                    self._completion_callback(self)

    @property
    def complete(self):
        # This is true when the checkpoint file reaches stage 3 or whatever
        # Illumina probably calls it
        checkpoint = getattr(self, "checkpoint", None)
        return checkpoint[0] == 3 if checkpoint else False

    def sample_paths(self, strict=True):
        reads = ["R1", "R2"] if len(self._sample_sheet["Reads"]) > 1 else ["R1"]
        fps = defaultdict(list)
        for attrs in self.__path_attrs:
            if attrs["read"] in reads:
                fps[attrs["sample_num"]].append(Path(attrs["path"]).resolve(strict=strict))
        sample_names = [row.get("Sample_Name") for row in self._sample_sheet["Data"]]
        sample_paths = {s_name: fps.get(idx+1, []) for idx, s_name in enumerate(sample_names)}
        if strict and any(len(vals) < len(reads) for vals in sample_paths.values()):
            # we should have either R1 or R1 & R2 for each sample.  If not and
            # if strict=True, raise an exception.
            raise FileNotFoundError
        return sample_paths


class AnalysisMiniSeq(Analysis):
    ...


class AnalysisMiSeq(Analysis):
    ...


class AnalysisMiSeqi100Plus(Analysis):
    ...


class AnalysisNextSeq2000(Analysis):
    ...
