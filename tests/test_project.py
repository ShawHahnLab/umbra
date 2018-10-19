#!/usr/bin/env python
"""
Tests for ProjectData objects.

These tests confirm that the portions of a run specific to a single project are
processed as expected, including the trimming/merging/assembling tasks and
calling external uploading/emailing functions (but not the outcome of those
calls).  This does not handle anything to do with multithreading between
multiple simultaneous projects, either; see test_illumina_processor.py for
that.
"""

from .test_common import *
import gzip
import warnings
import yaml
import threading
import time

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
        """Test that the project metadata is set up as expected."""

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
                "contacts": dict(),
                "contacts": {
                    "Someone": "person@gmail.com",
                    "Jesse Connell": "ancon@upenn.edu"
                    },
                "path": self.exp_path
                }
        ts_str = {
                "pending": ['trim', 'package', 'upload', 'email'],
                "current": "",
                "completed": []
                }
        ts_se = {
                "pending": ['copy', 'package', 'upload', 'email'],
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

        self.assertEqual(self.projs["STR"].metadata, md_str)
        self.assertEqual(self.projs["Something Else"].metadata, md_se)

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
    
    This handles the noop case and can be subclassed for other cases."""

    def setUp(self):
        if not hasattr(self, "task"):
            self.task = "noop"
        self.setUpTmpdir()
        self.maxDiff = None
        # Expected and shared attributes
        self.sample_names = ["1086S1_01", "1086S1_02", "1086S1_03", "1086S1_04"]
        if not hasattr(self, "rundir"):
            self.rundir = "180101_M00000_0000_000000000-XXXXX"
        self.run = Run(self.path_runs / self.rundir)
        self.alignment = self.run.alignments[0]
        self.exp_path = str(self.path_exp / "Partials_1_1_18" / "metadata.csv")
        self.project_name = "TestProject"
        if not hasattr(self, "tasks_run"):
            self.tasks_run = [self.task, "package", "upload", "email"]
        # modify project spreadsheet, then create ProjectData
        self.write_test_experiment()
        self.proj = ProjectData.from_alignment(self.alignment,
                self.path_exp,
                self.path_status,
                self.path_proc,
                self.path_pack,
                self.uploader,
                self.mailer).pop()

    def write_test_experiment(self):
        fieldnames = ["Sample_Name","Project","Contacts","Tasks"]
        exp_row = lambda sample_name: {
                "Sample_Name": sample_name,
                "Project": self.project_name,
                "Contacts": "Name Lastname <name@example.com>",
                "Tasks": self.task
                }
        with open(self.exp_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames)
            writer.writeheader()
            for sample_name in self.sample_names:
                writer.writerow((exp_row(sample_name)))

    def fake_fastq_entry(self, seq, qual):
        name = hashlib.md5(seq.encode("utf-8")).hexdigest()
        txt = "@%s\n%s\n+\n%s\n" % (name, seq, qual)
        return(txt)

    def fake_fastq(self, seq_pair, readlen=100):
        fastq_paths = self.alignment.sample_paths_for_num(1)
        [p.chmod(0o644) for p in fastq_paths]
        adp = illumina.adapters["Nextera"]
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
        logpath = self.proj.path_proc / "logs" / ("log_"+self.task+".txt")
        self.assertTrue(logpath.exists())

    def check_zipfile(self, files_exp):
        """Check that filenames are present in the zipfile.
        
        This will also check for the YAML metadata file in the expected hidden
        location."""
        # In the zipfile we should also find those same files.
        with ZipFile(self.proj.path_pack, "r") as z:
            info = z.infolist()
            files = [i.filename for i in info]
            for f in files_exp:
                p = Path(self.proj.work_dir) / self.run.run_id / f
                with self.subTest(p=p):
                    self.assertIn(str(p), files)
            # While we're at it let's check that a copy of the YAML metadata
            # was put in the zip, too.  This is really more about task=package
            # though.
            p = Path(self.proj.work_dir) / ("." + self.proj.path.name)
            self.assertIn(str(p), files)

    def test_attrs(self):
        s = self.path_status / self.run.run_id / "0"
        self.assertEqual(self.proj.name, self.project_name)
        self.assertEqual(self.proj.alignment, self.alignment)
        self.assertEqual(self.proj.path, s / (self.project_name + ".yml"))

    def test_work_dir(self):
        self.assertEqual(self.proj.work_dir, "2018-01-01-TestProject-Name")

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
        # The top-level work directory should contain the run directory.
        dirpath = self.proj.path_proc
        dir_exp = [self.run.run_id]
        dir_obs = [x.name for x in (dirpath.glob("*"))]
        self.assertEqual(dir_obs, dir_exp)
        # The files in the top-level of the run directory should match, too.
        files_in = lambda d, s: [x.name for x in d.glob(s) if x.is_file()]
        files_exp = sorted(files_in(self.run.path, "*"))
        files_obs = sorted(files_in(dirpath, "*/*"))
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
        self.check_log()


class TestProjectDataMerge(TestProjectDataOneTask):
    """ Test for single-task "merge".

    Here we should have a set of fastq files in a "PairedReads" subdirectory.
    This will be the interleaved version of the separate trimmed R1/R2 files
    from the trim task."""

    def setUp(self):
        self.task = "merge"
        # trim is a dependency of merge.
        self.tasks_run = ["trim", self.task, "package", "upload", "email"]
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
        self.check_log()


class TestProjectDataMergeSingleEnded(TestProjectDataMerge):
    """ Test for single-task "merge" for a singled-ended Run.

    What *should* happen here?  (What does the original trim script do?)
    """
    pass


class TestProjectDataAssemble(TestProjectDataOneTask):
    """ Test for single-task "assemble".
    
    This will automatically run the trim and merge tasks, and then build
    contigs de-novo from the reads with SPAdes.  The contigs will be filtered
    to just those greater than a minimum length, renamed to match the sample
    names, and converted to FASTQ for easy combining with the reads.  (This is
    the ContigsGeneious subdirectory.)  Those modified contigs will also be
    concatenated with the original merged reads (CombinedGeneious
    subdirectory)."""

    def setUp(self):
        self.task = "assemble"
        # trim and merge are dependencies of assemble.
        self.tasks_run = ["trim", "merge", self.task,
                "package", "upload", "email"]
        # TODO remove this, it's a real run for a first attempt at a real-life
        # test.
        #self.rundir = "180919_M05588_0119_000000000-D4VL7"
        super().setUp()

    def test_process(self):
        """Test that the merge task completed as expected."""
        # Let's set up a detailed example in one file pair, to make sure the
        # merging itself worked (separately testing trimming above).
        seq_pair = ["ACTG" * 10, "CAGT" * 10]
        self.fake_fastq(seq_pair)
        # The basics
        super().test_process()
        # TODO
        # Next, check that we have the output we expect from spades.  Ideally
        # we should have a true test but right now we get no contigs built.
        # Using a real run dir (see setUp above) to check it for now.

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
        self.check_log()


class TestProjectDataManual(TestProjectDataOneTask):
    """ Test for single-task "manual".

    Test that a ProjectData with a manual task specified will wait until a
    marker appears and then will continue processing.
    """

    def setUp(self):
        self.task = "manual"
        self.tasks_run = ["manual", "package", "upload", "email"]
        super().setUp()

    def finish_manual(self):
        (self.proj.path_proc / "Manual").mkdir()

    def test_process(self):
        # It should finish as long as it finds the Manual directory
        t = threading.Timer(1, self.finish_manual)
        t.start()
        super().test_process()


class TestProjectDataPackage(TestProjectDataOneTask):
    """ Test for single-task "package".

    This will barely do anything at all since only the project metadata file
    will be included in the zip file. (TestProjectDataCopy actually makes for a
    more thorough test.)
    """

    def setUp(self):
        self.task = "package"
        self.tasks_run = ["package", "upload", "email"]
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
        self.tasks_run = ["package", "upload", "email"]
        super().setUp()

    def test_process(self):
        # The basic checks
        super().test_process()
        # After processing, there should be a URL recorded for the upload task.
        url_obs = self.proj.metadata["task_output"]["upload"]["url"]
        self.assertEqual(url_obs[0:8], "https://")


class TestProjectDataEmail(TestProjectDataOneTask):
    """ Test for single-task "upload".

    The mailer here is a stub that just records the email parameters given to
    it, so this doesn't test much, just that the message was constructed as
    expected.
    """

    def setUp(self):
        self.task = "email"
        self.tasks_run = ["package", "upload", "email"]
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
        to_addrs_exp = ["<Name Lastname> name@example.com"]
        self.assertEqual(md5(m["msg_body"]), "6a4ac9de2b9a60cf199533bb445698f7")
        self.assertEqual(md5(m["msg_html"]), "60418f13707b73b32f0f7be4edd76fb4")
        self.assertEqual(m["subject"], subject_exp)
        self.assertEqual(m["to_addrs"], to_addrs_exp)


class TestProjectDataEmailNocontacts(TestProjectDataOneTask):
    # TODO test with task = email but empty contacts field
    # work_dir should be correct
    # contcts shoudl be correct (empty dict)
    pass


class TestProjectDataBlank(TestProjectDataOneTask):
    # TODO test with no tasks at all
    # this just needs to confim that the TASK_NULL code correctly inserts
    # "copy" when nothing else is specified.  put it in self.tasks_run.
    pass
        

# Other ProjectData test cases

class TestProjectDataFailure(TestBase):
    # TODO test the case of a failure during processing.  An Exception should
    # be raised but also logged to the ProjectData object and updated on disk.
    pass


class TestProjectDataBlank(TestBase):

    # TODO test the case of having a blank in the project column.  The run ID
    # should be used instead.
    pass


class TestProjectDataAlreadyProcessing(TestBase):

    # TODO test the case of having a project whose existing metadata points to
    # an already-running process.  We should abort in that case.
    pass


if __name__ == '__main__':
    unittest.main()
