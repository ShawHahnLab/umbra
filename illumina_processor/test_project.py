#!/usr/bin/env python
from test_util import *

class TestProjectData(TestIlluminaProcessorBase):
    """Main tests for ProjectData."""

    def setUp(self):
        self.setUpTmpdir()
        self.maxDiff = None
        self.run = Run(self.path_runs / "180101_M00000_0000_000000000-XXXXX")
        self.alignment = self.run.alignments[0]
        self.projs = ProjectData.from_alignment(self.alignment,
                self.path_exp,
                self.path_status,
                self.path_proc,
                self.path_pack)
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
                "STR": "2018-01-02-STR-Jesse",
                # The names are organized in the order presented in the
                # experiment metadata spreadsheet.  Any non-alphanumeric
                # characters are converted to _.
                "Something Else": "2018-01-02-Something_Else-Someone-Jesse"
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
                "pending": ['trim', 'package', 'upload'],
                "current": "",
                "completed": []
                }
        ts_se = {
                "pending": ['copy', 'package', 'upload'],
                "current": "",
                "completed": []
                }

        md_str["experiment_info"] = exp_info_str
        md_se["experiment_info"] = exp_info_se
        md_str["task_status"] = ts_str
        md_se["task_status"] = ts_se
        md_str["status"] = "complete"

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

class TestProjectDataOneTask(TestIlluminaProcessorBase):
    """Base class for one-project-one-task tests."""

    def setUp(self):
        self.setUpTmpdir()
        self.maxDiff = None
        # Expected and shared attributes
        self.sample_names = ["1086S1_01", "1086S1_02", "1086S1_03", "1086S1_04"]
        self.run = Run(self.path_runs / "180101_M00000_0000_000000000-XXXXX")
        self.alignment = self.run.alignments[0]
        self.exp_path = str(self.path_exp / "Partials_1_1_18" / "metadata.csv")
        self.project_name = "TestProject"
        self.tasks_run = [self.task, "package", "upload"]
        # modify project spreadsheet, then create ProjectData
        self.write_test_experiment()
        self.proj = ProjectData.from_alignment(self.alignment,
                self.path_exp,
                self.path_status,
                self.path_proc,
                self.path_pack).pop()

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

    def test_attrs(self):
        s = self.path_status / self.run.run_id / "0"
        self.assertEqual(self.proj.name, self.project_name)
        self.assertEqual(self.proj.alignment, self.alignment)
        self.assertEqual(self.proj.path, s / (self.project_name + ".yml"))

    def test_work_dir(self):
        self.assertEqual(self.proj.work_dir, "2018-01-02-TestProject-Name")

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


class TestProjectDataNoop(TestProjectDataOneTask):
    """ Test for single-task "noop".

    Here nothing should happen except for the defaults."""

    def setUp(self):
        self.task = "noop"
        super().setUp()


class TestProjectDataCopy(TestProjectDataOneTask):
    """ Test for single-task "copy".

    Here the whole run directory should be copied into the processing directory
    and zipped."""

    def setUp(self):
        self.task = "copy"
        super().setUp()

    def _fake_fastq_entry(seq):
        # TODO use this below.
        pass

    def test_process(self):
        # Let's set up a detailed example in one file pair, to make sure the
        # trimming itself worked.

        fastq_paths = self.alignment.sample_paths_for_num(1)
        # TODO  set up a very simple fake fastq and write it to the first file
        # pair.
        #with gzip.open(fastq_paths[0], "wb") as gz:
        #    gz.write()

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
        # In the zipfile we should also find those same files.
        with ZipFile(self.proj.path_pack, "r") as z:
            info = z.infolist()
            files = [i.filename for i in info]
            for f in files_exp:
                p = Path(self.proj.work_dir) / self.run.run_id / f
                self.assertIn(str(p), files)
            # While we're at it let's check that a copy of the YAML metadata
            # was put in the zip, too.  This is really more about task=package
            # though.
            p = Path(self.proj.work_dir) / ("." + self.proj.path.name)
            self.assertIn(str(p), files)


class TestProjectDataTrim(TestProjectDataOneTask):
    """ Test for single-task "trim".

    Here we should have a set of fastq files in a "trimmed" subdirectory."""

    def setUp(self):
        self.task = "trim"
        super().setUp()

    def test_process(self):
        """Test that the trim task completed as expected."""
        super().test_process()
        # We should have a subdirectory with the trimmed files.
        dirpath = self.proj.path_proc / "trimmed"
        # What trimmed files did we observe?
        fastq_obs = [x.name for x in dirpath.glob("*.trimmed.fastq")]
        fastq_obs = sorted(fastq_obs)
        # What trimmed files do we expect for the sample names we have?
        paths = []
        for sample in self.sample_names:
            i = self.alignment.sample_names.index(sample)
            p = self.alignment.sample_files_for_num(i+1) # (1-indexed)
            paths.extend(p)
        paths = [re.sub("\\.fastq\\.gz$", ".trimmed.fastq", p) for p in paths]
        fastq_exp = sorted(paths)
        # Now, do they match?
        self.assertEqual(fastq_obs, fastq_exp)
        # Was anything else in there?  Shouldn't be.
        files_all = [x.name for x in dirpath.glob("*")]
        files_all = sorted(files_all)
        self.assertEqual(files_all, fastq_exp)
        # And a log file too.
        logpath = self.proj.path_proc / "logs" / "log_trim.txt"
        self.assertTrue(logpath.exists())


class TestProjectDataMerge(TestProjectDataOneTask):
    # TODO test with tasks = merge
    pass


class TestProjectDataAssemble(TestProjectDataOneTask):
    # TODO test with tasks = assemble
    pass


class TestProjectDataPackage(TestProjectDataOneTask):
    # TODO test with tasks = package
    # move in the YAML check from the Copy test.  This is a more appropriate
    # location.
    pass


class TestProjectDataUpload(TestProjectDataOneTask):
    # TODO test with tasks = upload 
    # how to reasonably test this...?  Monkey-patch the uploader part to trick
    # it?
    pass


class TestProjectDataBlank(TestProjectDataOneTask):
    # TODO test with no tasks at all
    # this just needs to confim that the TASK_NULL code correctly inserts
    # "copy" when nothing else is specified.  put it in self.tasks_run.
    pass
        

# Other ProjectData test cases

class TestProjectDataFailure(TestIlluminaProcessorBase):
    # TODO test the case of a failure during processing.  An Exception should
    # be raised but also logged to the ProjectData object and updated on disk.
    pass


class TestProjectDataBlank(TestIlluminaProcessorBase):

    # TODO test the case of having a blank in the project column.  The run ID
    # should be used instead.
    pass


class TestProjectDataAlreadyProcessing(TestIlluminaProcessorBase):

    # TODO test the case of having a project whose existing metadata points to
    # an already-running process.  We should abort in that case.
    pass


if __name__ == '__main__':
    unittest.main()
