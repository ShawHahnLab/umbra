from .util import *
import gzip
import time

class Alignment:
    """An "Alignment" (FASTQ generation) within a run."""

    def __init__(self, path, run=None, completion_callback=None):
        self.run = run
        path = Path(path).resolve()
        self.path = path
        self.completion_callback = completion_callback
        try:
            # MiSeq, directly in Alignment folder
            self.path_sample_sheet = (path/"SampleSheetUsed.csv").resolve(strict=True)
            self.path_fastq = (path / "..").resolve()
            self.path_checkpoint = path/"Checkpoint.txt"
            self.path_job_info = path/"CompletedJobInfo.xml"
        except FileNotFoundError:
            # MiniSeq, within timstamped subfolder
            filt = lambda p: re.match("[0-9]{8}_[0-9]{6}", p.name)
            dirs = [d for d in path.glob("*") if d.is_dir() and filt(d)]
            # If there are no subdirectories this doesn't look like a MiniSeq alignment
            if not dirs:
                raise(ValueError('Not a recognized Illumina alignment: "%s"' % path))
            try:
                self.path_sample_sheet = (dirs[0]/"SampleSheetUsed.csv").resolve(strict=True)
            # If both possible sample sheet paths threw FileNotFound, we won't
            # consider this input path to be an alignment directory.
            except FileNotFoundError:
                raise(ValueError('Not a recognized Illumina alignment: "%s"' % path))
            self.path_fastq = dirs[0] / "Fastq"
            self.path_checkpoint = dirs[0]/"Checkpoint.txt"
            self.path_job_info = dirs[0]/"CompletedJobInfo.xml"
        self.sample_sheet = load_sample_sheet(self.path_sample_sheet)
        # This doesn't always exist.  On our MiniSeq and one of two MiSeqs it's
        # always written, but on a newer MiSeq we only have the copy saved to
        # the root of the run directory for the most recent alignment.
        # TODO merge this logic with Run's own version
        try:
            self.completed_job_info = load_xml(self.path_job_info)
        except FileNotFoundError:
            self.completed_job_info = None
        self.refresh()

    def refresh(self):
        """Reload alignment status from disk.
        
        If the alignment has just completed, and a callback function was
        provided during instantiation, call it."""
        if not self.complete:
            self.checkpoint = self._load_checkpoint(self.path_checkpoint)
            if self.complete and self.completion_callback:
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
            return(idx)
        return(None)

    @property
    def complete(self):
        """Is the alignment complete?"""
        return(getattr(self, "checkpoint", None) == 3)

    @property
    def experiment(self):
        """Experiment name given in sample sheet."""
        h = self.sample_sheet["Header"]
        # MiSeq vs MiniSeq
        exp = h.get("Experiment_Name") or h.get("Experiment Name")
        return(exp)

    @property
    def sample_numbers(self):
        """Ordered list of all sample numbers (indexed from one)."""
        num_range = range(len(self.sample_sheet["Data"]))
        nums = [i+1 for i in num_range]
        return(nums)

    @property
    def sample_names(self):
        """Ordered list of all sample names."""
        names = [row["Sample_Name"] for row in self.sample_sheet["Data"]]
        return(names)

    @property
    def samples(self):
        data = self.sample_sheet["Data"]
        newdata = [row.copy() for row in data]
        return(newdata)

    def sample_files_for_num(self, sample_num,
            fmt = "{sname}_S{snum}_L{lane:03d}_R{rp}_001.fastq.gz"):
        """Predict filenames (no paths) for the given sample number."""
        samples = self.samples
        try:
            sample = samples[int(sample_num)-1]
        except IndexError:
            raise ValueError("Sample number not found: %s" % sample_id)
        sname = sample["Sample_Name"].strip()
        # If there's a name defined, mask all the special characters we know
        # of and trim them from both ends.
        # SEE:
        # 171031_M05588_0004_000000000-BGFVN /
        # 171204_M00281_0300_000000000-D2W6Y +
        # 180711_M05588_0090_000000000-D4K5J #
        # If there's no name, Illumina just uses the sample ID instead.
        # SEE: 171026_M00281_0285_000000000-BGM65
        if not sname:
            sname = sample["Sample_ID"]
        sname = re.sub("[/+#_ .\-]+", "-", sname)
        sname = re.sub("-+$", "", sname)
        sname = re.sub("^-+", "", sname)
        fields = {"sname": sname, "snum": sample_num, "lane": 1, "rp": 1}
        fps = []
        for r_idx in range(len(self.sample_sheet["Reads"])):
            fields["rp"] = r_idx + 1
            fps.append(fmt.format(**fields))
        return(fps)

    def sample_paths_for_num(self, sample_num, strict = True):
        """Locate files (absolute Paths) for the given sample number on disk."""
        filenames = self.sample_files_for_num(sample_num)
        fps = []
        for filename in filenames:
            fp = (self.path_fastq / filename).resolve(strict = strict)
            fps.append(fp)
        return(fps)

    def sample_paths(self, strict = True):
        """Create dictionary mapping each sample name to list of file paths."""
        sample_paths = {}
        for s_num, s_name in zip(self.sample_numbers, self.sample_names):
            sps = self.sample_paths_for_num(s_num, strict)
            sample_paths[s_name] = sps
        return(sample_paths)

    def _make_dummy_files(self):
        """Create blank fastq.gz files in place of any missing ones."""
        # This is used in building test directories.
        s_paths = self.sample_paths(strict = False)
        for paths in s_paths.values():
            for path in paths:
                if not path.exists():
                    with gzip.open(path, "wb") as f:
                        pass

    def _load_checkpoint(self, path):
        """Load the number from a Checkpoint.txt file, or None if not found."""
        try:
            with open(path) as f:
                data = f.read().strip()
        except FileNotFoundError:
            data = None
        else:
            data = int(data)
        return(data)
