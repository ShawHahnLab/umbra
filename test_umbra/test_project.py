#!/usr/bin/env python
"""
Tests for ProjectData objects.

These tests confirm that the portions of a run specific to a single project are
processed as expected, including the trimming/merging/assembling tasks and
calling external uploading/emailing functions (but not the outcome of those
calls).  This does not handle anything to do with multithreading between
multiple simultaneous projects, either; see test_umbra.py for
that.
"""

from .test_common import *
import gzip
import zipfile
import warnings
import yaml
import threading
import time

DEFAULT_TASKS = ["metadata", "package", "upload", "email"]

class TestProjectData(TestBase):
    """Main tests for ProjectData.

    This became unwieldy pretty fast.  Merge into TestProjectDataOneTask,
    maybe."""

    def setUp(self):
        self.setUpTmpdir()
        self.maxDiff = None
        # TODO rename this!  TestCase already has a "run" attribute.
        self.run = Run(self.path_runs / "180101_M00000_0000_000000000-XXXXX")
        self.alignment = self.run.alignments[0]
        self.projs = ProjectData.from_alignment(self.alignment,
                self.path_exp,
                self.path_status,
                self.path_proc,
                self.path_pack,
                self.uploader,
                self.mailer)
        # switch to dictionary to make these easier to work with
        self.projs = {p.name: p for p in self.projs}
        self.exp_path = str(self.path_exp / "Partials_1_1_18" / "metadata.csv")
        # Make sure we have what we expect before the real tests
        self.assertEqual(sorted(self.projs.keys()),
                ["STR", "Something Else"])

    def test_attrs(self):
        s = self.path_status / self.run.run_id / "0"
        self.assertEqual(self.projs["STR"].name, "STR")
        self.assertEqual(self.projs["STR"].alignment, self.alignment)
        self.assertEqual(self.projs["STR"].path, s / "STR.yml")
        self.assertEqual(self.projs["Something Else"].name, "Something Else")
        self.assertEqual(self.projs["Something Else"].alignment, self.alignment)
        self.assertEqual(self.projs["Something Else"].path, s / "Something_Else.yml")

    def test_work_dir(self):
        # make sure it gives "date-project-names"
        # test any edge cases that may come up for those components
        works = {
                # The date stamp is by completion date of the alignment, not
                # start date of the run (i.e., the date encoded in the Run ID).
                "STR": "2018-01-01-STR-Jesse",
                # The names are organized in the order presented in the
                # experiment metadata spreadsheet.  Any non-alphanumeric
                # characters are converted to _.
                "Something Else": "2018-01-01-Something_Else-Someone-Jesse"
                }
        for key in works.keys():
            self.assertEqual(works[key], self.projs[key].work_dir)

    def test_metadata(self):
        """Test that the project metadata is set up as expected.

        These are edging toward private attributes but it's easier here to just
        check them all in one go, at least until that's more cleaned up."""

        md = {}
        md["status"] = "none"
        md["run_info"] = {"path": str(self.run.path)}
        md["sample_paths"] = {}
        md["alignment_info"] = {"path": str(self.alignment.path)}
        md["run_info"]["path"] = str(self.alignment.run.path)

        md_str = dict(md)
        md_se = dict(md)

        exp_info_str = {
                "name": "Partials_1_1_18",
                "sample_names": ["1086S1_01", "1086S1_02"],
                "tasks": ['trim'],
                "contacts": {'Jesse': 'ancon@upenn.edu'},
                "path": self.exp_path
                }
        exp_info_se = {
                "name": "Partials_1_1_18",
                "sample_names": ["1086S1_03", "1086S1_04"],
                "tasks": [],
                "contacts": {
                    "Someone": "person@gmail.com",
                    "Jesse Connell": "ancon@upenn.edu"
                    },
                "path": self.exp_path
                }
        ts_str = {
                "pending": ['trim', 'metadata', 'package', 'upload', 'email'],
                "current": "",
                "completed": []
                }
        ts_se = {
                "pending": ['copy', 'metadata', 'package', 'upload', 'email'],
                "current": "",
                "completed": []
                }

        md_str["experiment_info"] = exp_info_str
        md_se["experiment_info"] = exp_info_se
        md_str["task_status"] = ts_str
        md_se["task_status"] = ts_se
        md_str["status"] = "complete"
        md_str["task_output"] = {}
        md_se["task_output"] = {}
        md_se["work_dir"] = "2018-01-01-Something_Else-Someone-Jesse"
        md_str["work_dir"] = "2018-01-01-STR-Jesse"

        fq = self.path_runs/"180101_M00000_0000_000000000-XXXXX/Data/Intensities/BaseCalls"
        fps = {
                "1086S1_01": [str(fq/"1086S1-01_S1_L001_R1_001.fastq.gz"),
                              str(fq/"1086S1-01_S1_L001_R2_001.fastq.gz")],
                "1086S1_02": [str(fq/"1086S1-02_S2_L001_R1_001.fastq.gz"),
                              str(fq/"1086S1-02_S2_L001_R2_001.fastq.gz")]
                }
        md_str["sample_paths"] = fps
        fps = {
                "1086S1_03": [str(fq/"1086S1-03_S3_L001_R1_001.fastq.gz"),
                              str(fq/"1086S1-03_S3_L001_R2_001.fastq.gz")],
                "1086S1_04": [str(fq/"1086S1-04_S4_L001_R1_001.fastq.gz"),
                              str(fq/"1086S1-04_S4_L001_R2_001.fastq.gz")]
                }
        md_se["sample_paths"] = fps

        self.assertEqual(self.projs["STR"]._metadata, md_str)
        self.assertEqual(self.projs["Something Else"]._metadata, md_se)

    def test_readonly(self):
        self.assertTrue(self.projs["STR"].readonly)
        self.assertFalse(self.projs["Something Else"].readonly)

    def test_status(self):
        # Is the status what we expect from the initial metadata on disk?
        self.assertEqual(self.projs["STR"].status, "complete")
        self.assertEqual(self.projs["Something Else"].status, "none")
        # Is the setter protecting against invalid values?
        with self.assertRaises(ValueError):
            self.projs["STR"].status = "invalid status"
        # is the setter magically keeping the data on disk up to date?
        self.projs["Something Else"].status = "processing"
        with open(self.projs["Something Else"].path) as f:
            # https://stackoverflow.com/a/1640777
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore",category=DeprecationWarning)
                data = yaml.safe_load(f)
            self.assertEqual(data["status"], "processing")

    def test_process(self):
        # test processing all tasks
        # make sure exceptions are logged to metadata, too.
        # so far we have STR marked as complete, and Something Else with no
        # config yet.
        # A read-only ProjectData (including previously-complete ones) cannot
        # be re-processed.
        with self.assertRaises(ProjectError):
            self.projs["STR"].process()
        p = self.projs["Something Else"]
        p.process()
        self.assertEqual(p.status, "complete")


# Single-task ProjectData tests.

class TestProjectDataOneTask(TestBase):
    """Base class for one-project-one-task tests.

    This handles the noop case and can be subclassed for other cases.  This
    base class only sets certain attribute values if they haven't already been
    overridden."""

    def setUp(self):
        self.setUpTmpdir()
        self.setUpVars()
        self.setUpRun()
        # modify project spreadsheet, then create ProjectData
        self.write_test_experiment()
        # Readymade hook for any extra stuff for sub-classes
        if "setUpPreProject" in dir(self):
            self.setUpPreProject()
        self.setUpProj()

    def setUpVars(self):
        super().setUpVars()
        if not hasattr(self, "task"):
            self.task = "noop"
        self.maxDiff = None
        # Expected and shared attributes
        self.sample_names = ["1086S1_01", "1086S1_02", "1086S1_03", "1086S1_04"]
        if not hasattr(self, "rundir"):
            self.rundir = "180101_M00000_0000_000000000-XXXXX"
        if not hasattr(self, "exp_name"):
            self.exp_name = "Partials_1_1_18"
        self.exp_path = str(self.path_exp / self.exp_name / "metadata.csv")
        # Note the write_test_experiment below using this name.
        self.project_name = "TestProject"
        if not hasattr(self, "tasks_run"):
            if self.task in DEFAULT_TASKS:
                self.tasks_run = DEFAULT_TASKS[:]
            else:
                self.tasks_run = [self.task] + DEFAULT_TASKS
        if not hasattr(self, "contacts_str"):
            self.contacts_str = "Name Lastname <name@example.com>"
        if not hasattr(self, "contacts"):
            self.contacts = {"Name Lastname": "name@example.com"}
        if not hasattr(self, "work_dir_exp"):
            self.work_dir_exp = "2018-01-01-TestProject-Name"

    def setUpRun(self):
        self.run = Run(self.path_runs / self.rundir)
        self.alignment = self.run.alignments[0]

    def write_test_experiment(self):
        fieldnames = ["Sample_Name","Project","Contacts","Tasks"]
        exp_row = lambda sample_name: {
                "Sample_Name": sample_name,
                "Project": self.project_name,
                "Contacts": self.contacts_str,
                "Tasks": self.task
                }
        with open(self.exp_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames)
            writer.writeheader()
            for sample_name in self.sample_names:
                writer.writerow((exp_row(sample_name)))

    def setUpProj(self):
        # Pull out the project we want (to support possibility of additional
        # projects under this run+alignment)
        projs = ProjectData.from_alignment(self.alignment,
                self.path_exp,
                self.path_status,
                self.path_proc,
                self.path_pack,
                self.uploader,
                self.mailer)
        for proj in projs:
            if proj.name == self.project_name:
                self.proj = proj

    def fake_fastq_entry(self, seq, qual):
        # TODO can I just md5(seq)? check back on this.
        name = hashlib.md5(seq.encode("utf-8")).hexdigest()
        txt = "@%s\n%s\n+\n%s\n" % (name, seq, qual)
        return(txt)

    def fake_fastq(self, seq_pair, readlen=100):
        fastq_paths = self.alignment.sample_paths_for_num(1)
        [p.chmod(0o644) for p in fastq_paths]
        adp = illumina.util.ADAPTERS["Nextera"]
        fills = ("G", "A")
        for path, seq, a, fill in zip(fastq_paths, seq_pair, adp, fills):
            read = (seq + a).ljust(readlen, fill)
            read = read[0:min(readlen, len(read))]
            qual = ("@" * len(seq)) + ("!" * (len(read)-len(seq)))
            with gzip.open(path, "wt") as gz:
                gz.write(self.fake_fastq_entry(read, qual))

    def expected_paths(self, suffix=".trimmed.fastq", r1only=False):
        paths = []
        for sample in self.sample_names:
            i = self.alignment.sample_names.index(sample)
            p = self.alignment.sample_files_for_num(i+1) # (1-indexed)
            if r1only:
                paths.append(re.sub("_R1_", "_R_", p[0]))
            else:
                paths.extend(p)
        paths = [re.sub("\\.fastq\\.gz$", suffix, p) for p in paths]
        paths = sorted(paths)
        return(paths)

    def check_log(self):
        """Does the log file exist?

        It always should if a task was run for a project.
        """
        logpath = self.proj.path_proc / "logs" / ("log_"+self.task+".txt")
        self.assertTrue(logpath.exists())

    def check_zipfile(self, files_exp):
        """Check that filenames are present in the zipfile.

        This will also check for the YAML metadata file in the expected hidden
        location."""
        with zipfile.ZipFile(self.proj.path_pack, "r") as z:
            info = z.infolist()
            # Check compression status.  It should actually be compressed, and
            # with the expected method.
            item = info[0]
            self.assertTrue(item.compress_size < item.file_size)
            self.assertEqual(item.compress_type, zipfile.ZIP_DEFLATED)
            # Check that the expected files are all present in the zipfile.
            files = [i.filename for i in info]
            for f in files_exp:
                p = Path(self.proj.work_dir) / self.run.run_id / f
                with self.subTest(p=p):
                    self.assertIn(str(p), files)

    def test_attrs(self):
        s = self.path_status / self.run.run_id / "0"
        self.assertEqual(self.proj.name, self.project_name)
        self.assertEqual(self.proj.alignment, self.alignment)
        self.assertEqual(self.proj.path, s / (self.project_name + ".yml"))

    def test_work_dir(self):
        self.assertEqual(self.proj.work_dir, self.work_dir_exp)

    def test_readonly(self):
        self.assertFalse(self.proj.readonly)

    def test_status(self):
        # Here, we started from scratch.
        self.assertEqual(self.proj.status, "none")
        # Is the setter protecting against invalid values?
        with self.assertRaises(ValueError):
            self.proj.status = "invalid status"
        # is the setter magically keeping the data on disk up to date?
        self.proj.status = "processing"
        with open(self.proj.path) as f:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore",category=DeprecationWarning)
                data = yaml.safe_load(f)
            self.assertEqual(data["status"], "processing")

    def test_experiment_info(self):
        if hasattr(self, "tasks_listed"):
            tasks = self.tasks_listed
        else:
            tasks = [self.task]
        exp_info = {
                "name": self.exp_name,
                "sample_names": self.sample_names,
                "tasks": tasks,
                "contacts": self.contacts,
                "path": self.exp_path
                }
        self.assertEqual(self.proj.experiment_info, exp_info)

    # Test task properties, at start

    def test_tasks_pending(self):
        self.assertEqual(self.proj.tasks_pending, self.tasks_run)

    def test_tasks_completed(self):
        self.assertEqual(self.proj.tasks_completed, [])

    def test_task_current(self):
        self.assertEqual(self.proj.task_current, "")

    # After processing, we should see the change in each task property.

    def test_process(self):
        """Test that task status reported correctly after process()."""
        # test processing all tasks
        self.proj.process()
        self.assertEqual(self.proj.status, "complete")
        self.assertEqual(self.proj.tasks_pending, [])
        self.assertEqual(self.proj.tasks_completed, self.tasks_run)
        self.assertEqual(self.proj.task_current, "")
        self.check_log()


class TestProjectDataFail(TestProjectDataOneTask):
    """ Test for single-task "fail".

    Here we should see a failure during processing get caught and logged."""

    def setUp(self):
        self.task = "fail"
        super().setUp()

    def test_process(self):
        """Test that failure is caught and reported correctly in process()."""
        with self.assertRaises(ProjectError):
            self.proj.process()
        self.assertEqual(self.proj.status, "failed")
        self.assertEqual(self.proj.tasks_pending, DEFAULT_TASKS)
        self.assertEqual(self.proj.tasks_completed, [])
        self.assertEqual(self.proj.task_current, self.task)


class TestProjectDataCopy(TestProjectDataOneTask):
    """ Test for single-task "copy".

    Here the whole run directory should be copied into the processing directory
    and zipped."""

    def setUp(self):
        self.task = "copy"
        super().setUp()

    def test_process(self):
        # The basic checks
        super().test_process()
        # Here we should have a copy of the raw run data inside of the work
        # directory.
        # The top-level work directory should contain the run directory and the
        # default Metadata directory.
        dirpath = self.proj.path_proc
        dir_exp = sorted(["Metadata", "logs", self.run.run_id])
        dir_obs = sorted([x.name for x in (dirpath.glob("*"))])
        self.assertEqual(dir_obs, dir_exp)
        # The files in the top-level of the run directory should match, too.
        files_in = lambda d, s: [x.name for x in d.glob(s) if x.is_file()]
        files_exp = sorted(files_in(self.run.path, "*"))
        files_obs = sorted(files_in(dirpath / self.run.run_id, "*"))
        self.assertEqual(files_obs, files_exp)
        self.check_zipfile(files_exp)


class TestProjectDataTrim(TestProjectDataOneTask):
    """ Test for single-task "trim".

    Here we should have a set of fastq files in a "trimmed" subdirectory."""

    def setUp(self):
        self.task = "trim"
        super().setUp()

    def test_process(self):
        """Test that the trim task completed as expected."""
        # Let's set up a detailed example in one file pair, to make sure the
        # trimming itself worked.
        seq_pair = ("ACTG" * 10, "CAGT" * 10)
        self.fake_fastq(seq_pair)
        # The basics
        super().test_process()
        # We should have a subdirectory with the trimmed files.
        dirpath = self.proj.path_proc / "trimmed"
        # What trimmed files did we observe?
        fastq_obs = [x.name for x in dirpath.glob("*.trimmed.fastq")]
        fastq_obs = sorted(fastq_obs)
        # What trimmed files do we expect for the sample names we have?
        fastq_exp = self.expected_paths()
        # Now, do they match?
        self.assertEqual(fastq_obs, fastq_exp)
        # Was anything else in there?  Shouldn't be.
        files_all = [x.name for x in dirpath.glob("*")]
        files_all = sorted(files_all)
        self.assertEqual(files_all, fastq_exp)
        # Did the specific read pair we created get trimmed as expected?
        pat = str(dirpath / "1086S1-01_S1_L001_R%d_001.trimmed.fastq")
        fps = [pat % d for d in (1,2)]
        for fp, seq_exp in zip(fps, seq_pair):
            with open(fp, "r") as f:
                seq_obs = f.readlines()[1].strip()
                self.assertEqual(seq_obs, seq_exp)


class TestProjectDataMerge(TestProjectDataOneTask):
    """ Test for single-task "merge".

    Here we should have a set of fastq files in a "PairedReads" subdirectory.
    This will be the interleaved version of the separate trimmed R1/R2 files
    from the trim task."""

    def setUp(self):
        self.task = "merge"
        # trim is a dependency of merge.
        self.tasks_run = ["trim", self.task] + DEFAULT_TASKS
        super().setUp()

    def test_process(self):
        """Test that the merge task completed as expected."""
        # Let's set up a detailed example in one file pair, to make sure the
        # merging itself worked (separately testing trimming above).
        seq_pair = ["ACTG" * 10, "CAGT" * 10]
        self.fake_fastq(seq_pair)
        # The basics
        super().test_process()
        # We should have a subdirectory with the merged files.
        dirpath = self.proj.path_proc / "PairedReads"
        # What merged files did we observe?
        fastq_obs = [x.name for x in dirpath.glob("*.merged.fastq")]
        fastq_obs = sorted(fastq_obs)
        # What merged files do we expect for the sample names we have?
        fastq_exp = self.expected_paths(".merged.fastq", r1only=True)
        fastq_exp = [re.sub("_R1_", "_R_", p) for p in fastq_exp]
        # Now, do they match?
        self.assertEqual(fastq_obs, fastq_exp)
        # Was anything else in there?  Shouldn't be.
        files_all = [x.name for x in dirpath.glob("*")]
        files_all = sorted(files_all)
        self.assertEqual(files_all, fastq_exp)
        # Did the specific read pair we created get merged as expected?
        # (This isn't super thorough since in this case it's just the same as
        # concatenating the two files.  Maybe add more to prove they're
        # interleaved.)
        fp = str(dirpath / "1086S1-01_S1_L001_R_001.merged.fastq")
        with open(fp, "r") as f:
            data = f.readlines()
            seq_obs = [data[i].strip() for i in [1,5]]
            self.assertEqual(seq_obs, seq_pair)


@unittest.skip("not yet implemented")
class TestProjectDataMergeSingleEnded(TestProjectDataMerge):
    """ Test for single-task "merge" for a singled-ended Run.

    What *should* happen here?  (What does the original trim script do?)
    """
    pass


class TestProjectDataSpades(TestProjectDataOneTask):
    """Test for single-task "spades".

    This will automatically run the trim and merge tasks, and then build
    contigs de-novo from the reads with SPAdes.
    """

    def setUp(self):
        self.task = "spades"
        # trim and merge are dependencies of assemble.
        self.tasks_run = ["trim", "merge", self.task] + DEFAULT_TASKS
        super().setUp()

    def test_process(self):
        """Test that the spades task completed as expected."""
        seq_pair = ["ACTG" * 10, "CAGT" * 10]
        self.fake_fastq(seq_pair)
        # The basics
        super().test_process()
        # TODO
        # Next, check that we have the output we expect from spades.  Ideally
        # we should have a true test but right now we get no contigs built.


class TestProjectDataAssemble(TestProjectDataOneTask):
    """ Test for single-task "assemble".

    This will automatically run the trim/merge/spades tasks, and then
    post-process the contigs: The contigs will be filtered to just those
    greater than a minimum length, renamed to match the sample names, and
    converted to FASTQ for easy combining with the reads.  (This is the
    ContigsGeneious subdirectory.)  Those modified contigs will also be
    concatenated with the original merged reads (CombinedGeneious
    subdirectory).
    """

    def setUp(self):
        self.task = "assemble"
        # trim and merge are dependencies of assemble.
        self.tasks_run = ["trim", "merge", "spades", self.task] + DEFAULT_TASKS
        super().setUp()

    def test_process(self):
        """Test that the assemble task completed as expected."""
        seq_pair = ["ACTG" * 10, "CAGT" * 10]
        self.fake_fastq(seq_pair)
        # The basics
        super().test_process()
        # We should have one file each in ContigsGeneious and CombinedGeneious
        # per sample.
        dirpath_contigs = self.proj.path_proc / "ContigsGeneious"
        contigs_obs = [x.name for x in dirpath_contigs.glob("*.contigs.fastq")]
        contigs_obs = sorted(contigs_obs)
        dirpath_combo = self.proj.path_proc / "CombinedGeneious"
        combo_obs = [x.name for x in dirpath_combo.glob("*.contigs_reads.fastq")]
        combo_obs = sorted(combo_obs)
        contigs_exp = self.expected_paths(".contigs.fastq", r1only=True)
        combo_exp = self.expected_paths(".contigs_reads.fastq", r1only=True)
        self.assertEqual(contigs_obs, contigs_exp)
        self.assertEqual(combo_obs, combo_exp)


class TestProjectDataManual(TestProjectDataOneTask):
    """ Test for single-task "manual".

    Test that a ProjectData with a manual task specified will wait until a
    marker appears and then will continue processing.
    """

    def setUp(self):
        self.task = "manual"
        self.tasks_run = ["manual"] + DEFAULT_TASKS
        super().setUp()

    def finish_manual(self):
        (self.proj.path_proc / "Manual").mkdir()

    def test_process(self):
        # It should finish as long as it finds the Manual directory
        t = threading.Timer(1, self.finish_manual)
        t.start()
        super().test_process()


class TestProjectDataGeneious(TestProjectDataOneTask):
    """ Test for single-task "geneious".

    Test that a ProjectData with a geneious task specified will wait until a
    marker appears and then will continue processing.
    """

    def setUpVars(self):
        self.task = "geneious"
        self.tasks_run = ["trim", "merge", "spades", "assemble", "geneious"] + DEFAULT_TASKS
        super().setUpVars()

    def setUpProj(self):
        self.tasks_config  = {
            "implicit_tasks_path": "RunDiagnostics/ImplicitTasks"
            }
        projs = ProjectData.from_alignment(self.alignment,
            self.path_exp,
            self.path_status,
            self.path_proc,
            self.path_pack,
            self.uploader,
            self.mailer,
            conf = self.tasks_config)
        for proj in projs:
            if proj.name == self.project_name:
                self.proj = proj

    def finish_manual(self):
        (self.proj.path_proc / "Geneious").mkdir()

    def test_process(self):
        # It should finish as long as it finds the Geneious directory
        t = threading.Timer(1, self.finish_manual)
        t.start()
        super().test_process()
        # Despite the config, these directories should now be at the top level.
        self.assertTrue((self.proj.path_proc / "PairedReads").exists())
        self.assertTrue((self.proj.path_proc / "ContigsGeneious").exists())
        self.assertTrue((self.proj.path_proc / "CombinedGeneious").exists())


class TestProjectDataMetadata(TestProjectDataOneTask):
    """ Test for single-task "metadata".

    Test that a ProjectData gets the expected metadata files copied over to the
    working directory.
    """

    def setUp(self):
        self.task = "metadata"
        super().setUp()

    def test_process(self):
        # The basic checks
        super().test_process()
        # Also make sure that the expected files are there
        # SampleSheetUsed.csv from Alignment
        # metadata.csv for just this project
        # YAML for this project
        path_root = Path(self.proj.path_proc) / "Metadata"
        paths = {
                "sample_sheet": path_root / "SampleSheetUsed.csv",
                "metadata":     path_root / "metadata.csv",
                "yaml":         path_root / self.proj.path.name
                }
        path_md5s = {
                "sample_sheet": "1fbbfa008ef3038560e9904f3d8579a2",
                "metadata":     "5ef2284fced0e08105c0670518ae2eae"
                }
        # First off, they should all be there.
        for path in paths.values():
            self.assertTrue(path.exists())
        # They should also have the expected contents.  (YAML has paths so it'd
        # be random, though.  We'll skip that one.)
        for key, md5_exp in path_md5s.items():
            with self.subTest(md_file=key):
                path = paths[key]
                with open(path, 'rb') as f:
                    contents = f.read().decode("ASCII")
                md5_obs = md5(contents)
                self.assertEqual(md5_obs, md5_exp)

    def write_test_experiment(self):
        # We need a more nuanced experiment metadata spreadsheet here to test
        # that only this project's rows are copied.
        fieldnames = ["Sample_Name","Project","Contacts","Tasks"]
        exp_row = lambda sample_name, pname: {
                "Sample_Name": sample_name,
                "Project": pname,
                "Contacts": self.contacts_str,
                "Tasks": self.task
                }
        sample_names_extra = ["1086S1_05", "1086S2_01", "1086S2_02", "1086S2_03"]
        with open(self.exp_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames)
            writer.writeheader()
            for sample_name in self.sample_names:
                writer.writerow((exp_row(sample_name, self.project_name)))
            for sample_name in sample_names_extra:
                writer.writerow((exp_row(sample_name, "ZZ_Another")))


class TestProjectDataPackage(TestProjectDataOneTask):
    """ Test for single-task "package".

    This will barely do anything at all since no files get included in the zip
    file. (TestProjectDataCopy actually makes for a more thorough test.)
    """

    def setUp(self):
        self.task = "package"
        super().setUp()

    def test_process(self):
        # The basic checks
        super().test_process()
        self.check_zipfile([])


class TestProjectDataUpload(TestProjectDataOneTask):
    """ Test for single-task "upload".

    The uploader here is a stub that just returns a fake URL, so this doesn't
    test much, just that the URL is recorded as expected.
    """

    def setUp(self):
        self.task = "upload"
        super().setUp()

    def test_process(self):
        # The basic checks
        super().test_process()
        # After processing, there should be a URL recorded for the upload task.
        url_obs = self.proj._metadata["task_output"]["upload"]["url"]
        self.assertEqual(url_obs[0:8], "https://")


class TestProjectDataEmail(TestProjectDataOneTask):
    """ Test for single-task "email".

    The mailer here is a stub that just records the email parameters given to
    it, so this doesn't test much, just that the message was constructed as
    expected.
    """

    def setUp(self):
        self.task = "email"
        if not hasattr(self, "msg_body"):
            self.msg_body = "6a4ac9de2b9a60cf199533bb445698f7"
        if not hasattr(self, "msg_html"):
            self.msg_html = "60418f13707b73b32f0f7be4edd76fb4"
        if not hasattr(self, "to_addrs_exp"):
            self.to_addrs_exp = ["Name Lastname <name@example.com>"]
        super().setUp()

    def test_process(self):
        # The basic checks
        super().test_process()
        # After processing, there should be an email "sent" with the expected
        # attributes.  Using MD5 checksums on message text/html since it's a
        # bit bulky.
        email_obs = self.mails
        self.assertEqual(len(email_obs), 1)
        m = email_obs[0]
        keys_exp = ["msg_body", "msg_html", "subject", "to_addrs"]
        self.assertEqual(sorted(m.keys()), keys_exp)
        subject_exp = "Illumina Run Processing Complete for %s" % \
            self.proj.work_dir
        to_addrs_exp = self.to_addrs_exp
        self.assertEqual(md5(m["msg_body"]), self.msg_body)
        self.assertEqual(md5(m["msg_html"]), self.msg_html)
        self.assertEqual(m["subject"], subject_exp)
        self.assertEqual(m["to_addrs"], to_addrs_exp)


class TestProjectDataEmailOneName(TestProjectDataEmail):
    """What should happen if the email task just has one name?

    No difference.
    """

    def setUp(self):
        self.contacts_str = "Name <name@example.com>"
        self.contacts = {"Name": "name@example.com"}
        self.to_addrs_exp = ["Name <name@example.com>"]
        super().setUp()


class TestProjectDataEmailNoName(TestProjectDataEmail):
    """What should happen if the email task just has a plain email address?

    This should use the first part of the address as the contact dict key and
    in the work_dir text, but nothing much else should change.
    """

    def setUp(self):
        self.contacts_str = "name@example.com"
        self.contacts = {"name": "name@example.com"}
        self.to_addrs_exp = ["name <name@example.com>"]
        # (Very slightly different workdir (lowercase "name") and thus download
        # URL and thus message checksums)
        self.work_dir_exp = "2018-01-01-TestProject-name"
        self.msg_body = "30c16605d9b5f3ddfb14ac50260c5812"
        self.msg_html = "a58d68ea9d8e188b6764df254e680e96"
        super().setUp()


class TestProjectDataEmailNoContacts(TestProjectDataEmail):
    """What should happen if the email task is run with no contact info at all?

    Nothing much different here.  The mailer should still be called as usual
    (it might have recipients it always appends) with the expected arguments.
    (As for other cases the actual Mailer behavior is tested separately for
    that class.) In this ProjectData, the work_dir slug will be shorter and the
    contacts should be an empty dict, modifying the formatted message slightly,
    but that's about it.
    """

    def setUp(self):
        self.contacts_str = ""
        self.contacts = {}
        self.to_addrs_exp = []
        self.work_dir_exp = "2018-01-01-TestProject"
        self.msg_body = "925bfb376b0a2bc2b6797ef992ddbb00"
        self.msg_html = "e2a8c65f1d67ba6abd09caf2dddbc370"
        super().setUp()


# Other ProjectData test cases

class TestProjectDataFailure(TestProjectDataOneTask):
    """What should happen if an exception is raised during processing?

    The exception should reach the caller but the ProjectData object should do
    some cleanup of its own, trying to update its status to "failed" before
    re-raising the exception.
    """

    def setUp(self):
        super().setUp()
        # Processing won't like that there's already something using the path
        # it wants to use.  Let's make sure it fails, but in the way we expect.
        util.mkparent(self.proj.path_proc)
        self.proj.path_proc.touch(mode=0o000)

    def test_process(self):
        """Test that process() fails in the expected way.

        It should raise an exception, but still set its status attribute in the
        process and also record the exception details."""
        with self.assertRaises(FileExistsError):
            self.proj.process()
        self.assertEqual(self.proj.status, "failed")
        self.assertEqual(self.proj.tasks_pending, self.tasks_run)
        self.assertEqual(self.proj.tasks_completed, [])
        self.assertEqual(self.proj.task_current, "")
        self.assertTrue("failure_exception" in self.proj._metadata.keys())


class TestProjectDataFilesExist(TestProjectDataOneTask):
    """What should happen when there are already files in the processing dir?

    We should log a warning about it and mark the ProjectData as readonly.
    """

    def setUp(self):
        # Here we'll get a warning from within __init__ about the unexpected
        # subdirectory (see setUpPreProject below).  We'll check other
        # attributes in the tests.
        with self.assertLogs(level = logging.WARNING):
            super().setUp()

    def setUpPreProject(self):
        # Put something inside the processing directory that will trip up
        # ProjectData initialization.
        (self.path_proc / self.work_dir_exp / "subdir").mkdir(parents=True)

    def test_readonly(self):
        """Test that readonly=True."""
        self.assertTrue(self.proj.readonly)

    def test_status(self):
        """Test that status attribute works but file is not written."""
        self.assertEqual(self.proj.status, "none")
        # We won't touch anything on disk in this case due to the readyonly
        # flag.
        self.assertFalse(self.proj.path.exists())

    def test_process(self):
        """Test that processing throws exception."""
        with self.assertRaises(ProjectError):
            self.proj.process()


class TestProjectDataMissingSamples(TestProjectDataOneTask):
    """What if samples listed in metadata.csv are not in the sample sheet?

    Aside from typos, this could come up if one experiment name and metadata
    spreadsheet matches multiple runs.  This is a bit weird and warrants a
    warning but is allowed.
    """

    def setUpVars(self):
        super().setUpVars()
        # Include an extra sample in the experiment metadata spreadsheet that
        # won't be in the run data.
        self.sample_names = ["1086S1_01", "1086S1_02", "1086S1_03", "1086S1_04",
                "somethingelse"]

    def setUpProj(self):
        # The project should be initialized fine, but with a warning logged
        # about the sample name mismatch.
        with self.assertLogs(level = logging.WARNING):
            projs = ProjectData.from_alignment(self.alignment,
                    self.path_exp,
                    self.path_status,
                    self.path_proc,
                    self.path_pack,
                    self.uploader,
                    self.mailer)
        for proj in projs:
            if proj.name == self.project_name:
                self.proj = proj


class TestProjectDataNoSamples(TestProjectDataOneTask):
    """What if no samples listed in metadata.csv are in the sample sheet?

    If no samples at all are found it should be an error.  For simplicity in
    setting up multiple projects, we'll log an error and mark the project
    failed, but won't raise an exception.
    """

    def setUpVars(self):
        super().setUpVars()
        # Include an extra sample in the experiment metadata spreadsheet that
        # won't be in the run data.
        self.sample_names = ["somethingelse1", "somethingelse2"]

    def setUpProj(self):
        with self.assertLogs(level = logging.ERROR):
            projs = ProjectData.from_alignment(self.alignment,
                    self.path_exp,
                    self.path_status,
                    self.path_proc,
                    self.path_pack,
                    self.uploader,
                    self.mailer)
        for proj in projs:
            if proj.name == self.project_name:
                self.proj = proj

    def test_status(self):
        self.assertEqual(self.proj.status, "failed")

    def test_process(self):
        """Test that process()ing a failed project is not allowed."""
        with self.assertRaises(ProjectError):
            self.proj.process()
        self.assertEqual(self.proj.status, "failed")
        self.assertEqual(self.proj.tasks_pending, self.tasks_run)
        self.assertEqual(self.proj.tasks_completed, [])
        self.assertEqual(self.proj.task_current, "")


class TestProjectDataPathConfig(TestProjectDataOneTask):
    """Test custom output paths for logs and implicitly-dependent tasks.

    This should enable us to have just task directories we explicitly requested
    at the top level of the processed directory, stashing any implicit ones
    (dependencies or defaults) in a single subdirectory, and logs in a specific
    subdirectory.
    """

    def setUpVars(self):
        # merge needs trim.  This way we'll check both implicit-via-defaults
        # and implicit-via-dependencies.
        self.task = "merge"
        self.tasks_run = ["trim", self.task] + DEFAULT_TASKS
        super().setUpVars()

    def setUpProj(self):
        self.tasks_config  = {
            "log_path": "RunDiagnostics/logs",
            "implicit_tasks_path": "RunDiagnostics/ImplicitTasks"
            }
        projs = ProjectData.from_alignment(self.alignment,
            self.path_exp,
            self.path_status,
            self.path_proc,
            self.path_pack,
            self.uploader,
            self.mailer,
            conf = self.tasks_config)
        for proj in projs:
            if proj.name == self.project_name:
                self.proj = proj

    def test_process(self):
        super().test_process()
        metadata_path = self.proj.path_proc / self.tasks_config["implicit_tasks_path"] / "Metadata"
        self.assertTrue(metadata_path.exists())
        trim_path_default = self.proj.path_proc / "trimmed"
        trim_path = (self.proj.path_proc /
            self.tasks_config["implicit_tasks_path"] /
            "trimmed")
        merge_path = self.proj.path_proc / "PairedReads"
        # Trim path should have changed.  Merge path should have been left the
        # same.
        self.assertFalse(trim_path_default.exists())
        self.assertTrue(trim_path.exists())
        self.assertTrue(merge_path.exists())

    def check_log(self):
        """Override default log path when checking to match config."""
        logpath = self.proj.path_proc / self.tasks_config["log_path"] / ("log_"+self.task+".txt")
        self.assertTrue(logpath.exists())


class TestProjectDataImplicitTasks(TestProjectDataOneTask):
    """Test custom output paths for other implicit (non-dependency) tasks.

    In live usage processing failed because a late-running task (metadata)
    assumed the parent directories were already created.
    """

    def setUpVars(self):
        self.task = "trim"
        super().setUpVars()

    def setUpProj(self):
        self.tasks_config  = {
            "implicit_tasks_path": "RunDiagnostics/ImplicitTasks"
            }
        projs = ProjectData.from_alignment(self.alignment,
            self.path_exp,
            self.path_status,
            self.path_proc,
            self.path_pack,
            self.uploader,
            self.mailer,
            conf = self.tasks_config)
        for proj in projs:
            if proj.name == self.project_name:
                self.proj = proj

    def test_process(self):
        super().test_process()
        self.check_log()
        metadata_path = self.proj.path_proc / self.tasks_config["implicit_tasks_path"] / "Metadata"
        self.assertTrue(metadata_path.exists())
        # Trim path should not have changed.
        trim_path = self.proj.path_proc / "trimmed"
        self.assertTrue(trim_path.exists())


class TestProjectDataBlank(TestProjectDataOneTask):
    # TODO test with no tasks at all
    # this just needs to confim that the TASK_NULL code correctly inserts
    # "copy" when nothing else is specified.  put it in self.tasks_run.
    pass


class TestProjectDataAlreadyProcessing(TestBase):

    # TODO test the case of having a project whose existing metadata points to
    # an already-running process.  We should abort in that case.
    pass


if __name__ == '__main__':
    unittest.main()
