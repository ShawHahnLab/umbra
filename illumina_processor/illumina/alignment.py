from .util import *

class Alignment:
    """An "Alignment" (FASTQ generation) within a run."""

    def __init__(self, path, run=None):
        self.run = run
        path = Path(path).resolve()
        self.path = path
        try:
            # MiSeq, directly in Alignment folder
            self.path_sample_sheet = (path/"SampleSheetUsed.csv").resolve(strict=True)
            self.path_fastq = (path / "..").resolve()
            self.path_checkpoint = path/"Checkpoint.txt"
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
        self.sample_sheet = load_sample_sheet(self.path_sample_sheet)
        self.checkpoint = self.load_checkpoint(self.path_checkpoint)

    def refresh(self):
        if not self.complete:
            self.checkpoint = self.load_checkpoint(self.path_checkpoint)

    @property
    def complete(self):
        return(self.checkpoint == 3)

    @property
    def experiment(self):
        h = self.sample_sheet["Header"]
        # MiSeq vs MiniSeq
        exp = h.get("Experiment_Name") or h.get("Experiment Name")
        return(exp)

    def sample_paths_by_name(self):
        """Create dictionary mapping each sample name to list of file paths."""
        sample_paths = {}
        idxs = range(len(self.sample_sheet["Data"]))
        for idx, row in zip(idxs, self.sample_sheet["Data"]):
            sample_name = row["Sample_Name"]
            sps = self.sample_paths(idx+1)
            sample_paths[sample_name] = sps
        return(sample_paths)

    def sample_paths(self, sample_num):
        """Locate files (absolute Paths) for the given sample number on disk."""
        filenames = self.sample_files(sample_num)
        fps = []
        for filename in filenames:
            fp = (self.path_fastq / filename).resolve(strict = True)
            fps.append(fp)
        return(fps)

    def sample_files(self, sample_num,
            fmt = "{sname}_S{snum}_L{lane:03d}_R{rp}_001.fastq.gz"):
        """Predict filenames (no paths) for the given sample number."""
        samples = self.sample_sheet["Data"]
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

    def load_checkpoint(self, path):
        """Load the number from a Checkpoint.txt file, or None if not found."""
        try:
            with open(path) as f:
                data = f.read().strip()
        except FileNotFoundError:
            data = None
        else:
            data = int(data)
        return(data)
