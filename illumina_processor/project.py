from .util import *
from . import experiment
import yaml
import traceback
from zipfile import ZipFile
import subprocess
from Bio import SeqIO
import warnings
from tempfile import TemporaryDirectory
# requires on PATH:
#  * cutadapt
#  * spades.py

class ProjectError(Exception):
    """Any sort of project-related exception."""
    pass

class ProjectData:
    """The data for a Run and Alignment specific to one project.
    
    This references the data files within a specific run relevant to a single
    project, tracks the associated additional metadata provided via the
    "experiment" identified in the sample sheet, and handles post-processing.

    The same project may span many separate runs, but a ProjectData object
    refers only to a specific portion of a single Run.
    """
    # TODO pass in experiment metadata for relevant samples
    # track run/alignment_num/project_name at this point.


    # ProjectData processing status enumerated type.
    NONE          = "none"
    PROCESSING    = "processing"
    PACKAGE_READY = "package-ready"
    COMPLETE      = "complete"
    FAILED        = "failed"
    STATUS = [NONE, PROCESSING, PACKAGE_READY, COMPLETE, FAILED]

    # Recognized tasks:
    TASK_NOOP     = "noop"     # do nothing
    TASK_COPY     = "copy"     # copy raw files
    TASK_TRIM     = "trim"     # trip adapters
    TASK_MERGE    = "merge"    # interleave trimmed R1/R2 reads
    TASK_ASSEMBLE = "assemble" # contig assembly
    TASK_PACKAGE  = "package"  # package in zip file
    TASK_UPLOAD   = "upload"   # upload to Box
    TASK_EMAIL    = "email"    # email contacts

    # Adding an explicit order and dependencies.
    TASKS = [
            TASK_NOOP,
            TASK_COPY,
            TASK_TRIM,
            TASK_MERGE,
            TASK_ASSEMBLE,
            TASK_PACKAGE,
            TASK_UPLOAD,
            TASK_EMAIL
            ]

    TASK_DEPS = {
            TASK_EMAIL: [TASK_UPLOAD],
            TASK_UPLOAD: [TASK_PACKAGE],
            TASK_MERGE: [TASK_TRIM],
            TASK_ASSEMBLE: [TASK_MERGE],
            }

    # We'll always include these tasks:
    TASK_DEFAULTS = [
            TASK_EMAIL
            ]

    # If nothing else is given, this will be added:
    TASK_NULL = [
            TASK_COPY
            ]

    def from_alignment(alignment, path_exp, dp_align, dp_proc, dp_pack):
        """Make dict of ProjectData objects from alignment/experiment table."""
        # Row by row, build up a dict for each unique project.  Even though
        # we're reading it in as a spreadsheet we'll treat most of this as
        # an unordered sets for each project.

        exp_path = path_exp / alignment.experiment / "metadata.csv"
        projects = set()
        try:
            # Load the spreadsheet of per-sample project information
            experiment_info = experiment.load_metadata(exp_path)
        except FileNotFoundError:
            pass
        else:
            sample_paths = None
            try:
                sample_paths = alignment.sample_paths()
            except FileNotFoundError as e:
                msg = "\nFASTQ file not found:\n"
                msg += "Run:       %s\n" % alignment.run.path
                msg += "Alignment: %s\n" % alignment.path
                msg += "File:      %s\n" % e.filename
                warnings.warn(msg)
            # Set of unique names in the experiment spreadsheet
            names = {row["Project"] for row in experiment_info}
            run_id = alignment.run.run_id
            al_idx = str(alignment.index)
            for name in names:
                proj_file = slugify(name) + ".yml"
                fp = Path(dp_align) / run_id / al_idx / proj_file
                proj = ProjectData(name, fp,
                        dp_proc,
                        dp_pack,
                        alignment,
                        experiment_info,
                        exp_path)
                proj.sample_paths = sample_paths
                projects.add(proj)
        return(projects)

    def __init__(self, name, path, dp_proc, dp_pack, alignment, exp_info_full,
            exp_path=None,
            threads=1):

        self.name = name
        self.alignment = alignment
        self.path = path # YAML metadata path
        self.threads = threads # max threads to give tasks
        # TODO phred score should really be a property of the Illumina
        # alignment data, since it depends on the software generating the
        # fastqs.
        self.phred_offset = 33 # FASTQ quality score encoding offset

        self.metadata = {"status": ProjectData.NONE}
        self.readonly = self.path.exists()
        self.load_metadata()
        # TODO tidy up these keys and protect behind getters/setters with
        # automatic file updating.
        self.metadata["alignment_info"] = {}
        self.metadata["experiment_info"] = self._setup_exp_info(exp_info_full)
        self.metadata["experiment_info"]["path"] = str(exp_path or "")
        self.metadata["run_info"] = {}
        self.metadata["sample_paths"] = {}
        self.metadata["task_status"] = self._setup_task_status()
        if self.alignment:
            self.metadata["alignment_info"]["path"] = str(self.alignment.path)
            self.metadata["experiment_info"]["name"] = self.alignment.experiment
        if self.alignment.run:
            self.metadata["run_info"]["path"] = str(self.alignment.run.path)

        self.path_proc = Path(dp_proc) / self.work_dir
        self.path_pack = Path(dp_pack) / (self.work_dir + ".zip")
        if not self.readonly:
            self.save_metadata()

    @property
    def status(self):
        return(self.metadata["status"])

    @status.setter
    def status(self, value):
        if not value in ProjectData.STATUS:
            raise ValueError
        self.metadata["status"] = value
        if not self.readonly:
            self.save_metadata()

    @property
    def experiment_info(self):
        return(self.metadata["experiment_info"])

    @property
    def work_dir(self):
        """Short name for working directory, without path"""
        txt_date = datestamp(self.alignment.completion_time)
        txt_proj = slugify(self.name)
        # first names of contacts given
        who = self.experiment_info["contacts"]
        who = [txt.split(" ")[0] for txt in who.keys()]
        who = "-".join(who)
        txt_name = slugify(who)
        dirname = "%s-%s-%s" % (txt_date, txt_proj, txt_name)
        return(dirname)

    @property
    def sample_paths(self):
        paths = self.metadata["sample_paths"]
        if not paths: 
            return({})
        paths2 = {}
        for k in paths:
            paths2[k] = [Path(p) for p in paths[k]]
        return(paths2)

    @sample_paths.setter
    def sample_paths(self, sample_paths):
        if sample_paths:
            self.metadata["sample_paths"] = {}
            for sample_name in self.metadata["experiment_info"]["sample_names"]:
                paths = [str(p) for p in sample_paths[sample_name]]
                self.metadata["sample_paths"][sample_name] = paths
        else:
            self.metadata["sample_paths"] = None

    @property
    def tasks_pending(self):
        return(self.metadata["task_status"]["pending"])

    @property
    def tasks_completed(self):
        return(self.metadata["task_status"]["completed"])

    @property
    def task_current(self):
        return(self.metadata["task_status"]["current"])

    def deps_completed(self, task):
        """ Are all dependencies of a given task already completed?"""
        deps = ProjectData.TASK_DEPS.get(task, [])
        remaining = [d for d in deps if d not in self.tasks_completed]
        return(not remaining)

    def process(self):
        """Run all tasks.
        
        This function will block until processing is complete.  Calling process
        if readyonly=True raises ProjectError."""
        if self.readonly:
            raise ProjectError("ProjectData is read-only")
        self.status = ProjectData.PROCESSING
        ts = self.metadata["task_status"]
        while self.tasks_pending: 
            if self.task_current:
                raise ProjectError("a task is already running")
            ts["current"] = ts["pending"].pop(0)
            if not self.deps_completed(ts["current"]):
                raise ProjectError("not all dependencies for task completed")
            self.save_metadata()
            try:
                self._run_task(ts["current"])
            except Exception as e:
                self.metadata["failure_exception"] = traceback.format_exc()
                self.status = ProjectData.FAILED
                raise(e)
            ts["completed"].append(ts["current"])
            ts["current"] = ""
            self.save_metadata()
        self.status = ProjectData.COMPLETE

    def load_metadata(self):
        try:
            with open(self.path) as f:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore",category=DeprecationWarning)
                    data = yaml.safe_load(f)
        except FileNotFoundError:
            data = None
        else:
            self.metadata.update(data)
        return(data)

    def save_metadata(self):
        """Update project metadata on disk."""
        if self.readonly:
            raise ProjectError("ProjectData is read-only")
        # We need to be careful because other threads may be working in the
        # exact same directory right now.  pathlib didn't handle this correctly
        # until recently:
        # https://bugs.python.org/issue29694
        # I'll stick with the os module for now, since it seems to handle it
        # just fine.
        dp = Path(self.path.parent)
        os.makedirs(dp, exist_ok=True)
        with open(self.path, "w") as f:
            f.write(yaml.dump(self.metadata))

    # Task-related methods

    def copy_run(self):
        """Copy the run directory into the processing directory."""
        src = str(self.alignment.run.path)
        dest = str(self.path_proc / self.alignment.run.run_id)
        copy_tree(src, dest)

    def _trimmed_path(self, path_fastq):
        name = re.sub("\\.fastq\\.gz$", "", path_fastq.name)
        fastq_out = self.path_proc / "trimmed" / (name + ".trimmed.fastq")
        mkparent(fastq_out)
        return(fastq_out)

    def _log_path(self, task):
        path = self.path_proc / "logs" / ("log_" + str(task) + ".txt")
        mkparent(path)
        return(path)

    def trim(self):
        # For each sample, separately process the one or more associated files.
        # TODO open trim log
        with open(self._log_path("trim"), "w") as f:
            for samp in self.sample_paths.keys():
                paths = self.sample_paths[samp]
                if len(paths) > 2:
                    raise ProjectError("trimming can't handle >2 files per sample")
                for i in range(len(paths)):
                    adapter = illumina.adapters["Nextera"][i]
                    fastq_in = str(paths[i])
                    fastq_out = self._trimmed_path(paths[i])
                    args = ["cutadapt", "-a", adapter, "-o", fastq_out, fastq_in]
                    # Call cutadapt with each file.  If the exit status is
                    # nonzero or if the expected output file is missing, raise
                    # an exception.
                    subprocess.run(args, stdout=f, stderr=f, check=True)
                    if not Path(fastq_out).exists():
                        msg = "missing output file %s" % fastq_out
                        raise ProjectError(msg)

    def _merged_path(self, path_fastq):
        # Generalize R1/R2 to just "R" and replace extension with merged
        # version.
        pat = "(.*_L[0-9]+_)R[12](_001)\\.fastq\\.gz"
        name = re.sub("\\.fastq\\.gz$", "", path_fastq.name)
        name = re.sub(pat, "\\1R\\2.merged.fastq", path_fastq.name)
        fastq_out = self.path_proc / "PairedReads" / name
        mkparent(fastq_out)
        return(fastq_out)

    def _merge_pair(self, fq_out, fqs_in):
        with open(fq_out, "w") as f_out, \
                open(fqs_in[0], "r") as f_r1, \
                open(fqs_in[1], "r") as f_r2:
            r1 = SeqIO.parse(f_r1, "fastq")
            r2 = SeqIO.parse(f_r2, "fastq")
            for rec1, rec2 in zip(r1, r2):
                SeqIO.write(rec1, f_out, "fastq")
                SeqIO.write(rec2, f_out, "fastq")

    def merge(self):
        with open(self._log_path("merge"), "w") as f:
            try:
                for samp in self.sample_paths.keys():
                    paths = self.sample_paths[samp]
                    if len(paths) != 2:
                        raise ProjectError("merging needs 2 files per sample")
                    fqs_in = [self._trimmed_path(p) for p in paths]
                    fq_out = self._merged_path(paths[0])
                    self._merge_pair(fq_out, fqs_in)
                    # Merge each file pair. If the expected output file is missing,
                    # raise an exception.
                    if not Path(fq_out).exists():
                        msg = "missing output file %s" % fq_out
                        raise ProjectError(msg)
            except Exception as e:
                msg = traceback.format_exc()
                f.write(msg + "\n")
                raise e

    def _assemble_reads(self, fqs_in, dir_out, f_log):
        """Assemble a pair of read files with SPAdes.
        
        This runs spades.py on a single sample, saving the output to a given
        directory.  The contigs, if built, will be in contigs.fasta.  Spades
        seems to crash a lot so if anything goes wrong we just create an empty
        contigs.fasta in the directory and log the error."""
        fp_out = dir_out / "contigs.fasta"
        # Spades always fails for empty input, so we'll explicitly skip that
        # case.  It might crash anyway, so we handle that below too.
        if Path(fqs_in[0]).stat().st_size == 0:
            s = tuple([str(s) for s in fqs_in])
            f_log.write("Skipping assembly for empty files: %s, %s\n" % s)
            f_log.write("creating placeholder contig file.\n")
            touch(fp_out)
            return
        args = ["spades.py", "-1", fqs_in[0], "-2", fqs_in[1],
                "-o", dir_out,
                "-t", self.threads,
                "--phred-offset", self.phred_offset]
        args = [str(x) for x in args]
        # spades tends to throw nonzero exit codes with short files, empty
        # files, etc.   If something goes wrong during assembly we'll just make
        # a stub file and move on.
        try:
            subprocess.run(args, stdout=f_log, stderr=f_log, check=True)
        except subprocess.CalledProcessError:
            f_log.write("spades exited with errors.\n")
            f_log.write("creating placeholder contig file.\n")
            touch(fp_out)

    def _assembled_dir_path(self, path_fastq):
        pat = "(.*_L[0-9]+_)R[12]?(_001)\\.fastq\\.gz"
        name = re.sub("\\.fastq\\.gz$", "", path_fastq.name)
        name = re.sub(pat, "\\1R\\2", path_fastq.name)
        dir_out = self.path_proc / "assembled" / name
        mkparent(dir_out)
        return(dir_out)

    def assemble(self):
        """Assemble contigs from all samples.
        
        This handles de-novo assembly with Spades and some of our own
        post-processing."""
        with open(self._log_path("assemble"), "w") as f:
            try:
                for samp in self.sample_paths.keys():
                    paths = self.sample_paths[samp]
                    fqs_in = [self._trimmed_path(p) for p in paths]
                    dir_out = self._assembled_dir_path(paths[0])
                    self._assemble_reads(fqs_in, dir_out, f)
                    # TODO the remaining steps from the original script:
                    # filter contigs > 255 bases long
                    # save as fastq (?)
                    # what else?
            except Exception as e:
                msg = traceback.format_exc()
                f.write(msg + "\n")
                raise e

    def zip(self):
        """Create zipfile of processing directory and metadata."""
        with ZipFile(self.path_pack, "x") as z:
            # Archive everything in the processing directory
            for root, dirs, files in os.walk(self.path_proc):
                for fn in files:
                    # Archive the file but trim the name so it's relative to
                    # the processing directory.
                    filename = os.path.join(root, fn)
                    arcname = Path(filename).relative_to(self.path_proc.parent)
                    z.write(filename, arcname)
            # Also add in a copy of the metadata YAML file as it currently
            # stands.
            filename = self.path
            arcname = Path(self.path_proc.name) / ("." + str(filename.name))
            z.write(filename, arcname)

    # Implementation Details

    def _setup_exp_info(self, exp_info_full):
        exp_info = {
                "sample_names": [],
                "tasks": [],
                "contacts": dict()
                }
        for row in exp_info_full:
            if row["Project"] == self.name:
                sample_name = row["Sample_Name"].strip()
                if not sample_name in exp_info["sample_names"]:
                    exp_info["sample_names"].append(sample_name)
                exp_info["contacts"].update(row["Contacts"])
                for task in row["Tasks"]:
                    if not task in exp_info["tasks"]:
                        exp_info["tasks"].append(task)
        return(exp_info)

    def _setup_task_status(self):
        tasks = self.experiment_info["tasks"][:]
        tasks = self._normalize_tasks(tasks)
        task_status = {}
        task_status["pending"] = tasks
        task_status["completed"] = []
        task_status["current"] = ""
        return(task_status)

    def _normalize_tasks(self, tasks):
        """Add default and dependency tasks and order as needed."""
        # Add in any defaults.  If nothing is given, always include the
        # default.
        if not tasks:
            tasks = ProjectData.TASK_NULL[:]
        tasks += ProjectData.TASK_DEFAULTS
        # Add in dependencies for any that exist.
        deps = set()
        for task in tasks:
            deps.update(self._deps_for(task))
        tasks += deps
        # Keep unique only.
        tasks = list(set(tasks))
        # order and verify.  We'll get a ValueError from index() if any of
        # these are not found.
        indexes = [ProjectData.TASKS.index(t) for t in tasks]
        tasks = [t for _,t in sorted(zip(indexes, tasks))]
        return(tasks)

    def _deps_for(self, task, total=None):
        """Get all dependent tasks (including indirect) for one task."""
        # Set of all dependencies seen so far.
        if not total:
            total = set()
        deps = ProjectData.TASK_DEPS.get(task, set())
        for dep in deps:
            if dep not in total:
                total.update(self._deps_for(dep, total))
            total.add(dep)
        return(total)

    def _run_task(self, task):
        """Process the next pending task."""
        
        # No-op: do nothing!
        if task == ProjectData.TASK_NOOP:
            pass

        # Copy run directory to within processing directory
        elif task == ProjectData.TASK_COPY:
            self.copy_run()

        # Run cutadapt to trim the Illumina adapters
        elif task == ProjectData.TASK_TRIM:
            self.trim()

        # Interleave the read pairs
        elif task == ProjectData.TASK_MERGE:
            self.merge()

        # Run SPAdes to assemble contigs from the interleaved files
        elif task == ProjectData.TASK_ASSEMBLE:
            self.assemble()

        # Zip up all files in the processing directory
        elif task == ProjectData.TASK_PACKAGE:
            self.zip()

        # Upload the zip archive to Box
        elif task == ProjectData.TASK_UPLOAD:
            pass
            #raise NotImplementedError(task)

        # Email contacts with link to Box download
        elif task == ProjectData.TASK_EMAIL:
            pass
            #raise NotImplementedError(task)

        # This should never happen (so it probably will).
        else:
            raise ProjectError("task \"%s\" not recognized" % task)
