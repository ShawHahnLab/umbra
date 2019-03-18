"""
Handle processing of select samples from a single run.

This is the glue that connects raw sequence data and additional metadata
supplied separately, and executes any processing tasks defined.  Objects of
class ProjectData are created primarily via ProjectData.from_alignment in
IlluminaProcessor, creating an arbitrary number of ProjectData instances for a
given run as defined by the supplied metadata.csv.

A single custom exception, ProjectError, is used for any "expected" problems
that may arise (e.g., attempting to process a readonly project, existing files
in the processing output directory, unexpected input data formats, etc).
"""

import shutil
import traceback
import zipfile
import subprocess
import csv
import logging
import re
import time
import sys
import os
import warnings
from pathlib import Path
from distutils.dir_util import copy_tree
import yaml
from Bio import SeqIO
from . import experiment, illumina
from .util import touch, mkparent, slugify, datestamp
# requires on PATH:
#  * cutadapt
#  * spades.py

class ProjectError(Exception):
    """Any sort of project-related exception."""

class ProjectData:
    """The data for a Run and Alignment specific to one project.

    This references the data files within a specific run relevant to a single
    project, tracks the associated additional metadata provided via the
    "experiment" identified in the sample sheet, and handles post-processing.

    The same project may span many separate runs, but a ProjectData object
    refers only to a specific portion of a single Run.
    """

    # ProjectData processing status enumerated type.
    NONE          = "none"
    PROCESSING    = "processing"
    PACKAGE_READY = "package-ready"
    COMPLETE      = "complete"
    FAILED        = "failed"
    STATUS = [NONE, PROCESSING, PACKAGE_READY, COMPLETE, FAILED]

    # Recognized tasks:
    TASK_NOOP     = "noop"     # do nothing
    TASK_FAIL     = "fail"     # trigger a failure
    TASK_COPY     = "copy"     # copy raw files
    TASK_TRIM     = "trim"     # trim adapters
    TASK_MERGE    = "merge"    # interleave trimmed R1/R2 reads
    TASK_ASSEMBLE = "assemble" # contig assembly
    TASK_MANUAL   = "manual"   # manual intervention step
    TASK_METADATA = "metadata" # copy metadata to working dir
    TASK_PACKAGE  = "package"  # package in zip file
    TASK_UPLOAD   = "upload"   # upload to Box
    TASK_EMAIL    = "email"    # email contacts

    # Adding an explicit order and dependencies.
    TASKS = [
        TASK_NOOP,
        TASK_FAIL,
        TASK_COPY,
        TASK_TRIM,
        TASK_MERGE,
        TASK_ASSEMBLE,
        TASK_MANUAL,
        TASK_METADATA,
        TASK_PACKAGE,
        TASK_UPLOAD,
        TASK_EMAIL
        ]

    TASK_DEPS = {
        TASK_EMAIL: [TASK_UPLOAD],
        TASK_UPLOAD: [TASK_PACKAGE],
        TASK_MERGE: [TASK_TRIM],
        TASK_ASSEMBLE: [TASK_MERGE]
        }

    # We'll always include these tasks:
    TASK_DEFAULTS = [
        TASK_METADATA,
        TASK_EMAIL
        ]

    # If nothing else is given, this will be added:
    TASK_NULL = [
        TASK_COPY
        ]

    @staticmethod
    def from_alignment(alignment, path_exp, dp_align, dp_proc, dp_pack,
                       uploader, mailer, nthreads=1, readonly=False,
                       config=None):
        """Make set of ProjectData objects from alignment/experiment table.

        This is called from IlluminaProcessor and so is protected from a set of
        "expected" errors and just logs them appropriately.  Exceptions outside
        of the expected type will still propogate so the processor will halt
        and catch fire, though."""

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
            except FileNotFoundError as exception:
                # In this case there's a mismatch in the data files expected
                # from the Alignment directory's metadata and the data files
                # actually on disk.
                msg = "\nFASTQ file not found:\n"
                msg += "Run:       %s\n" % alignment.run.path
                msg += "Alignment: %s\n" % alignment.path
                msg += "File:      %s\n" % exception.filename
                warnings.warn(msg)
            # Set of unique names in the experiment spreadsheet
            names = {row["Project"] for row in experiment_info}
            run_id = alignment.run.run_id
            al_idx = str(alignment.index)
            for name in names:
                proj_file = slugify(name) + ".yml"
                fpath = Path(dp_align) / run_id / al_idx / proj_file
                proj = ProjectData(
                    name=name,
                    path=fpath,
                    dp_proc=dp_proc,
                    dp_pack=dp_pack,
                    alignment=alignment,
                    exp_info_full=experiment_info,
                    uploader=uploader,
                    mailer=mailer,
                    exp_path=exp_path,
                    nthreads=nthreads,
                    readonly=readonly,
                    config=config)
                try:
                    proj.sample_paths = sample_paths
                except ProjectError:
                    # If something went wrong at this point, complain, but
                    # still add the (failed) project to the set, since it did
                    # initialize already.
                    proj.fail()
                    msg = "ProjectData setup failed for %s (%s)"
                    msg = msg % (proj.work_dir, alignment.run.run_id)
                    logging.getLogger(__name__).error(msg)
                projects.add(proj)
        return projects

    def __init__(
            self, name, path, dp_proc, dp_pack, alignment, exp_info_full,
            uploader, mailer, exp_path=None, nthreads=1, readonly=False,
            config=None):

        self.logger = logging.getLogger(__name__)
        self.name = name
        self.alignment = alignment
        self.exp_path = exp_path # Orig experiment spreadsheet (maybe >1 proj)
        self.path = path # YAML metadata path
        self.nthreads = nthreads # max threads to give tasks
        self.uploader = uploader # callback to upload zip file
        self.mailer = mailer # callback to send email
        # general configuration including per-task options
        self.config = config
        if self.config is None:
            self.config = {}
        # TODO phred score should really be a property of the Illumina
        # alignment data, since it depends on the software generating the
        # fastqs.
        self.phred_offset = 33 # FASTQ quality score encoding offset
        self.contig_length_min = 255

        self._metadata = {"status": ProjectData.NONE}
        self.readonly = self.path.exists() or readonly
        self.load_metadata()
        self._metadata["alignment_info"] = {}
        self._metadata["experiment_info"] = self._setup_exp_info(exp_info_full)
        self._metadata["experiment_info"]["path"] = str(exp_path or "")
        self._metadata["run_info"] = {}
        self._metadata["sample_paths"] = {}
        self._metadata["task_status"] = self._setup_task_status()
        self._metadata["task_output"] = {}
        if self.alignment:
            self._metadata["alignment_info"]["path"] = str(self.alignment.path)
            self._metadata["experiment_info"]["name"] = self.alignment.experiment
        if self.alignment.run:
            self._metadata["run_info"]["path"] = str(self.alignment.run.path)
        self._metadata["work_dir"] = self._init_work_dir_name()

        self.path_proc = Path(dp_proc) / self.work_dir
        self.path_pack = Path(dp_pack) / (self.work_dir + ".zip")
        if not self.readonly:
            if self.path_proc.exists() and self.path_proc.glob("*"):
                self.logger.warning(
                    "Processing directory exists and is not empty: %s",
                    str(self.path_proc))
                self.logger.warning(
                    "Marking project readonly: %s", self.work_dir)
                self.readonly = True
            else:
                self.save_metadata()
        self.logger.info("ProjectData initialized: %s", self.work_dir)

    def _init_work_dir_name(self):
        txt_date = datestamp(self.alignment.run.rta_complete["Date"])
        txt_proj = slugify(self.name)
        # first names of contacts given
        who = self.experiment_info["contacts"]
        who = [txt.split(" ")[0] for txt in who.keys()]
        who = "-".join(who)
        txt_name = slugify(who)
        fields = [txt_date, txt_proj, txt_name]
        fields = [f for f in fields if f]
        dirname = "-".join(fields)
        # TODO better exception here
        if not dirname:
            raise Exception("empty work_dir")
        return dirname

    @property
    def status(self):
        """Get processing status with shorthand method."""
        return self._metadata["status"]

    @status.setter
    def status(self, value):
        """Set status to an allowed value and update metadata on disk."""
        if not value in ProjectData.STATUS:
            raise ValueError
        self._metadata["status"] = value
        if not self.readonly:
            self.save_metadata()

    @property
    def experiment_info(self):
        """Dict of experiment metdata specific to this project."""
        return self._metadata["experiment_info"]

    @property
    def work_dir(self):
        """Short name for working directory, without path."""
        return self._metadata["work_dir"]

    @property
    def sample_paths(self):
        """Dict mapping sample names to filesystem paths."""
        paths = self._metadata["sample_paths"]
        if not paths:
            return {}
        paths2 = {}
        for k in paths:
            paths2[k] = [Path(p) for p in paths[k]]
        return paths2

    @sample_paths.setter
    def sample_paths(self, sample_paths):
        # Here we expect that each sample name listed in the metadata
        # spreadsheet will also be found in the given sample_paths from
        # the run data.  But what if not?
        # If just some, report a warning.  Maybe a typo or maybe one
        # spreadsheet is being used across multiple runs.  If all are
        # missing, throw exception.
        # Keep paths for the sample names present in both the experiment
        # metadata spreadsheet and the given sample paths by sample name.
        # Exclude sample names we didn't find in the given sample paths.
        # (Also implicitly excludes unrelated sample paths that may be for
        # other projects.)
        from_exp = set(self.experiment_info["sample_names"])
        from_given = set(sample_paths.keys())
        keepers = from_exp & from_given
        excluded = from_exp - keepers
        msg = "Samples from experiment/run/both: %d/%d/%d"
        msg = msg % (len(from_exp), len(from_given), len(keepers))
        try:
            if excluded and keepers:
                # some excluded, some kept
                msg += " (some not matched in run)"
                lvl = logging.WARNING
            elif excluded:
                # all excluded, none kept
                msg += " (none matched in run)"
                lvl = logging.ERROR
                err = "No matching samples between experiment and run"
                raise ProjectError(err)
            elif keepers:
                # OK! all kept.
                lvl = logging.DEBUG
            else:
                # huh, nothing given here?  warning?
                msg += " (none given in run)"
                lvl = logging.WARNING
        finally:
            # In any case, log the sample numbers at the appropriate level.  An
            # exception will continue from here, if present.
            self.logger.log(lvl, msg)
        self._metadata["sample_paths"] = {}
        for sample_name in keepers:
            paths = [str(p) for p in sample_paths[sample_name]]
            self._metadata["sample_paths"][sample_name] = paths

    @property
    def tasks_pending(self):
        """List of task names not yet completed."""
        return self._metadata["task_status"]["pending"]

    @property
    def tasks_completed(self):
        """List of task names completed."""
        return self._metadata["task_status"]["completed"]

    @property
    def task_current(self):
        """Name of task currently running."""
        return self._metadata["task_status"]["current"]

    def deps_completed(self, task):
        """ Are all dependencies of a given task already completed?"""
        deps = ProjectData.TASK_DEPS.get(task, [])
        remaining = [d for d in deps if d not in self.tasks_completed]
        return not remaining

    def process(self):
        """Run all tasks.

        This function will block until processing is complete.  Calling process
        if readyonly=True or status != NONE raises ProjectError."""
        self.logger.info("ProjectData processing: %s", self.work_dir)
        if self.readonly:
            raise ProjectError("ProjectData is read-only")
        elif self.status != ProjectData.NONE:
            msg = "ProjectData status already defined as \"%s\"" % self.status
            raise ProjectError(msg)
        self.status = ProjectData.PROCESSING
        try:
            self.path_proc.mkdir(parents=True, exist_ok=True)
            tstat = self._metadata["task_status"]
            while self.tasks_pending:
                if self.task_current:
                    raise ProjectError("a task is already running")
                tstat["current"] = tstat["pending"].pop(0)
                if not self.deps_completed(tstat["current"]):
                    raise ProjectError("not all dependencies for task completed")
                self.save_metadata()
                self._run_task(tstat["current"])
                tstat["completed"].append(tstat["current"])
                tstat["current"] = ""
                self.save_metadata()
        except Exception as exception:
            self.fail()
            raise exception
        self.status = ProjectData.COMPLETE

    def fail(self):
        """Mark processing status as failed, and note exception, if any."""
        msg = ""
        if any(sys.exc_info()):
            msg = traceback.format_exc()
        self._metadata["failure_exception"] = msg
        self.status = ProjectData.FAILED

    def load_metadata(self):
        """Load metadata YAML file from disk."""
        try:
            # TODO wait, why not using load_yaml here?
            with open(self.path) as fin:
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        category=DeprecationWarning)
                    data = yaml.safe_load(fin)
        except FileNotFoundError:
            data = None
        else:
            self._metadata.update(data)
        return data

    def save_metadata(self):
        """Update project metadata on disk."""
        if self.readonly:
            raise ProjectError("ProjectData is read-only")
        mkparent(self.path)
        with open(self.path, "w") as fout:
            fout.write(yaml.dump(self._metadata))

    ###### Task-related methods

    def copy_run(self):
        """Copy the run directory into the processing directory."""
        src = str(self.alignment.run.path)
        dest = str(self._task_dir_parent(self.TASK_COPY) /
                   self.alignment.run.run_id)
        copy_tree(src, dest)

    def _task_dir_parent(self, taskname):
        """Give processing parent path for a given task.

        This will take into account configuration settings for the project and
        the specific task, if present.  If all defaults are set this will just
        be the processing directory.
        """
        path_implicit = "."
        if taskname not in self.experiment_info["tasks"]:
            path_implicit = self.config.get("implicit_tasks_path", ".")
        path = (self.path_proc / path_implicit).resolve()
        return path

    def task_path(self, readfile, taskname, subdir, suffix="", r1only=True):
        """Give readfile-related path, following the originals' name."""
        pat = "(.*_L[0-9]+_)R([12])(_001)\\.fastq\\.gz"
        if r1only:
            name = re.sub(pat, "\\1R\\3" + suffix, readfile.name)
        else:
            name = re.sub(pat, "\\1R\\2\\3" + suffix, readfile.name)
        fastq_out = self._task_dir_parent(taskname) / subdir / name
        mkparent(fastq_out)
        return fastq_out

    def _log_path(self, task):
        # TODO set up sane handling of defaults, using the package's config
        # file.
        path = (self.path_proc /
                self.config.get("log_path", "logs") /
                ("log_" + str(task) + ".txt")).resolve()
        mkparent(path)
        return path

    def trim(self):
        """Trim adapters from raw fastq.gz files."""
        # For each sample, separately process the one or more associated files.
        with open(self._log_path("trim"), "w") as fout:
            for samp in self.sample_paths.keys():
                paths = self.sample_paths[samp]
                if len(paths) > 2:
                    raise ProjectError("trimming can't handle >2 files per sample")
                for i, path in enumerate(paths):
                    adapter = illumina.util.ADAPTERS["Nextera"][i]
                    fastq_in = str(path)
                    fastq_out = self.task_path(
                        readfile=path,
                        taskname=self.TASK_TRIM,
                        subdir="trimmed",
                        suffix=".trimmed.fastq",
                        r1only=False)
                    args = ["cutadapt", "-a", adapter, "-o", fastq_out, fastq_in]
                    # Call cutadapt with each file.  If the exit status is
                    # nonzero or if the expected output file is missing, raise
                    # an exception.
                    subprocess.run(args, stdout=fout, stderr=fout, check=True)
                    if not Path(fastq_out).exists():
                        msg = "missing output file %s" % fastq_out
                        raise ProjectError(msg)

    ### Merge

    def _merge_pair(self, fq_out, fqs_in):
        with open(fq_out, "w") as f_out, \
                open(fqs_in[0], "r") as f_r1, \
                open(fqs_in[1], "r") as f_r2:
            riter1 = SeqIO.parse(f_r1, "fastq")
            riter2 = SeqIO.parse(f_r2, "fastq")
            for rec1, rec2 in zip(riter1, riter2):
                SeqIO.write(rec1, f_out, "fastq")
                SeqIO.write(rec2, f_out, "fastq")

    def merge(self):
        """Interleave forward and reverse reads for each sample."""
        with open(self._log_path("merge"), "w") as fout:
            try:
                for samp in self.sample_paths.keys():
                    paths = self.sample_paths[samp]
                    if len(paths) != 2:
                        raise ProjectError("merging needs 2 files per sample")
                    get_tp = lambda p: self.task_path(p, "trim", "trimmed", ".trimmed.fastq", r1only=False)
                    fqs_in = [get_tp(p) for p in paths]
                    fq_out = self.task_path(paths[0],
                                            "merge",
                                            "PairedReads",
                                            ".merged.fastq")
                    self._merge_pair(fq_out, fqs_in)
                    # Merge each file pair. If the expected output file is missing,
                    # raise an exception.
                    if not Path(fq_out).exists():
                        msg = "missing output file %s" % fq_out
                        raise ProjectError(msg)
            except Exception as exception:
                msg = traceback.format_exc()
                fout.write(msg + "\n")
                raise exception

    ### Assemble

    def _assemble_reads(self, fq_in, dir_out, f_log):
        """Assemble a pair of read files with SPAdes.

        This runs spades.py on a single sample, saving the output to a given
        directory.  The contigs, if built, will be in contigs.fasta.  Spades
        seems to crash a lot so if anything goes wrong we just create an empty
        contigs.fasta in the directory and log the error."""
        fp_out = dir_out / "contigs.fasta"
        # Spades always fails for empty input, so we'll explicitly skip that
        # case.  It might crash anyway, so we handle that below too.
        if Path(fq_in).stat().st_size == 0:
            f_log.write("Skipping assembly for empty file: %s\n" % str(fq_in))
            f_log.write("creating placeholder contig file.\n")
            touch(fp_out)
            return fp_out
        args = ["spades.py", "--12", fq_in,
                "-o", dir_out,
                "-t", self.nthreads,
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
        return fp_out

    def _prep_contigs_for_geneious(self, fa_in, fq_out):
        """Filter and format contigs for use in Geneious.

        Keep contigs above a length threshold, and fake quality scores so we
        can get a FASTQ file to combine with the reads in the next step.
        Modify the sequence ID line to be: <sample>-contig_<contig_number>
        """
        match = re.match("(.*)\\.contigs\\.fastq$", fq_out.name)
        sample_prefix = match.group(1)
        with open(fq_out, "w") as f_out, open(fa_in, "r") as f_in:
            for rec in SeqIO.parse(f_in, "fasta"):
                if len(rec.seq) > self.contig_length_min:
                    rec.letter_annotations["phred_quality"] = [40]*len(rec.seq)
                    match = re.match("^NODE_([0-9])+_.*", rec.id)
                    contig_num = match.group(1)
                    rec.id = "%s-contig_%s" % (sample_prefix, contig_num)
                    rec.description = ""
                    SeqIO.write(rec, f_out, "fastq")

    def _combine_contigs_for_geneious(self, fq_contigs, fq_reads, fq_out):
        """Concatenate formatted contigs and merged reads for Geneious."""
        with open(fq_contigs) as f_contigs, open(fq_reads) as f_reads, open(fq_out, "w") as f_out:
            for line in f_contigs:
                f_out.write(line)
            for line in f_reads:
                f_out.write(line)

    def assemble(self):
        """Assemble contigs from all samples.

        This handles de-novo assembly with Spades and some of our own
        post-processing."""
        with open(self._log_path("assemble"), "w") as fout:
            try:
                for samp in self.sample_paths.keys():
                    # Set up paths to use
                    paths = self.sample_paths[samp]
                    fq_merged = self.task_path(paths[0],
                                               self.TASK_MERGE,
                                               "PairedReads",
                                               ".merged.fastq")
                    fq_contigs = self.task_path(paths[0],
                                                self.TASK_ASSEMBLE,
                                                "ContigsGeneious",
                                                ".contigs.fastq")
                    fq_combo = self.task_path(paths[0],
                                              self.TASK_ASSEMBLE,
                                              "CombinedGeneious",
                                              ".contigs_reads.fastq")
                    spades_dir = self.task_path(paths[0], self.TASK_ASSEMBLE, "assembled")
                    # Assemble and post-process: create FASTQ version for all
                    # contigs above a given length, using altered sequence
                    # descriptions, and then combine with the original reads.
                    fa_contigs = self._assemble_reads(fq_merged, spades_dir, fout)
                    self._prep_contigs_for_geneious(fa_contigs, fq_contigs)
                    self._combine_contigs_for_geneious(fq_contigs, fq_merged, fq_combo)
            except Exception as exception:
                msg = traceback.format_exc()
                fout.write(msg + "\n")
                raise exception

    ### Zip

    def zip(self):
        """Create zipfile of processing directory and metadata."""
        mkparent(self.path_pack)
        # By default ZipFile will not actually compress!  We need to specify a
        # compression method explicitly for that.
        with zipfile.ZipFile(self.path_pack, "x", zipfile.ZIP_DEFLATED) as zipper:
            # Archive everything in the processing directory
            for root, dummy, files in os.walk(self.path_proc):
                for fname in files:
                    # Archive the file but trim the name so it's relative to
                    # the processing directory.
                    filename = os.path.join(root, fname)
                    arcname = Path(filename).relative_to(self.path_proc.parent)
                    zipper.write(filename, arcname)

    ### Metadata

    def copy_metadata(self):
        """Copy metadata spreadsheets and YAML into working directory."""
        dest = self._task_dir_parent(self.TASK_METADATA) / "Metadata"
        dest.mkdir(exist_ok=True)
        paths = []
        paths.append(self.alignment.path_sample_sheet) # Sample Sheet
        paths.append(self.path) # Metadata YAML file (as it currently stands)
        for path in paths:
            shutil.copy(path, dest)
        # metadata is special: need to filter out other projects
        path_md_out = dest / self.exp_path.name
        with open(self.exp_path) as f_in, open(path_md_out, "w") as f_out:
            # Read in all the rows in the original metadata spreadsheet
            reader = csv.DictReader(f_in)
            data = list(reader)
            # Here, write out the experiment spreadsheet but only include our
            # rows
            writer = csv.DictWriter(f_out, fieldnames=data[0].keys())
            writer.writeheader()
            for row in data:
                if row["Project"] == self.name:
                    writer.writerow(row)

    ### Mail

    def send_email(self):
        """Send notfication email for a finished ProjectData."""
        # Gather fields to fill in for the message
        # (The name prefix is considered OK by RFC822, so we should be able to
        # leave that intact for both the sending part and the "To:" field.)
        contacts = self._metadata["experiment_info"]["contacts"]
        contacts = ["%s <%s>" % (k, contacts[k]) for k in contacts]
        url = self._metadata["task_output"].get("upload", {}).get("url", "")
        subject = "Illumina Run Processing Complete for %s" % self.work_dir
        # Build message text and html
        body = "Hello,\n\n"
        body += "Illumina run processing is complete for %s\n" % self.work_dir
        body += "and a zip file with results can be downloaded from this url:\n"
        body += "\n%s\n" % url
        html = "Hello,\n"
        html += "<br><br>\n\n"
        html += "Illumina run processing is complete for %s\n" % self.work_dir
        html += "and a zip file with results can be downloaded from this url:\n"
        html += "<br><br>\n"
        html += "\n<a href='%s'>%s</a>\n" % (url, url)
        # Send
        kwargs = {
            "to_addrs": contacts,
            "subject": subject,
            "msg_body": body,
            "msg_html": html
            }
        self.mailer(**kwargs)

    ###### Implementation Details

    def _setup_exp_info(self, exp_info_full):
        # Row by row, build up a dict for this project.  Even though we're
        # reading the experiment info as a spreadsheet we'll treat most of this
        # as though it's unordered sets for each project.  (Not actually using
        # the set object as that gave me trouble with the YAML, but plain lists
        # do fine.)
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
        return exp_info

    def _setup_task_status(self):
        tasks = self.experiment_info["tasks"][:]
        tasks = self._normalize_tasks(tasks)
        task_status = {}
        task_status["pending"] = tasks
        task_status["completed"] = []
        task_status["current"] = ""
        return task_status

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
        tasks = [t for _, t in sorted(zip(indexes, tasks))]
        return tasks

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
        return total

    def _run_task(self, task):
        """Process the next pending task."""

        msg = "ProjectData processing: %s, task: %s" % (self.work_dir, task)
        self.logger.debug(msg)

        self._metadata["task_output"][task] = {}

        # No-op: do nothing!
        if task == ProjectData.TASK_NOOP:
            pass

        elif task == ProjectData.TASK_FAIL:
            raise ProjectError("Failing ProjectData as requested")

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

        # Wait for a "Manual" subdirectory to appear in the processing
        # directory.
        elif task == ProjectData.TASK_MANUAL:
            while not (self.path_proc / "Manual").exists():
                time.sleep(1)

        # Copy over metadata to working directory
        elif task == ProjectData.TASK_METADATA:
            self.copy_metadata()

        # Zip up all files in the processing directory
        elif task == ProjectData.TASK_PACKAGE:
            self.zip()

        # Upload the zip archive to Box
        elif task == ProjectData.TASK_UPLOAD:
            url = self.uploader(path=self.path_pack)
            self._metadata["task_output"][task] = {"url": url}

        # Email contacts with link to Box download
        elif task == ProjectData.TASK_EMAIL:
            self.send_email()

        # This should never happen (so it probably will).
        else:
            raise ProjectError("task \"%s\" not recognized" % task)
