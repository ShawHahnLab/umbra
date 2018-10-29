from .test_common import *

class TestRun(unittest.TestCase):
    """Base test case for a Run."""

    def setUp(self):
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / RUN_IDS["MiSeq"]
        copy_tree(str(PATH_RUNS / RUN_IDS["MiSeq"]), str(self.path_run))
        self.run = Run(self.path_run)
        self.id_exp = RUN_IDS["MiSeq"]
        date = datetime.datetime(2018, 1, 1, 6, 21, 31, 705000)
        self.rta_exp = {"Date": date, "Version": "Illumina RTA 1.18.54"}
        self.t1_exp = "2018-01-02T06:48:22.0480092-04:00"
        self.t2_exp = "2018-01-02T06:48:32.608024-04:00"

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_attrs(self):
        # RunInfo.xml
        # Check the Run ID.
        id_obs = self.run.run_info.find("Run").attrib["Id"]
        self.assertEqual(id_obs, self.id_exp)
        # RTAComplete.txt
        # Check the full contents.
        rta_obs = self.run.rta_complete
        self.assertEqual(rta_obs, self.rta_exp)
        # CompletedJobInfo.xml
        # Check the job start/completion timestamps.
        t1_obs = self.run.completed_job_info.find("StartTime").text
        t2_obs = self.run.completed_job_info.find("CompletionTime").text
        self.assertEqual(self.t1_exp, t1_obs)
        self.assertEqual(self.t2_exp, t2_obs)

    def test_refresh(self):
        """Test that a run refresh works"""
        ### 1: Update run completion status
        # Starting without a RTAComplete.txt, run is marked incomplete.
        move(str(self.path_run / "RTAComplete.txt"), str(self.path_run / "tmp.txt"))
        self.run = Run(self.path_run)
        self.assertFalse(self.run.complete)
        self.assertEqual(self.run.rta_complete, None)
        # It doesn't update automatically.
        move(str(self.path_run / "tmp.txt"), str(self.path_run / "RTAComplete.txt"))
        self.assertFalse(self.run.complete)
        # On refresh, it is now seen as complete.
        self.run.refresh()
        self.assertTrue(self.run.complete)
        orig_als = self.run.alignments
        ### 2: refresh existing alignments
        path_checkpoint = self.run.alignments[0].path_checkpoint
        move(str(path_checkpoint), str(self.path_run / "tmp.txt"))
        self.run = Run(self.path_run)
        self.assertEqual(len(self.run.alignments), len(orig_als))
        self.assertFalse(self.run.alignments[0].complete)
        move(str(self.path_run / "tmp.txt"), str(path_checkpoint))
        self.assertFalse(self.run.alignments[0].complete)
        self.run.refresh()
        self.assertTrue(self.run.alignments[0].complete)
        ### 3: load any new alignments
        path_al = self.run.alignments[0].path
        move(str(path_al), str(self.path_run / "tmp"))
        self.run = Run(self.path_run)
        self.assertEqual(len(self.run.alignments), len(orig_als)-1)
        move(str(self.path_run / "tmp"), str(path_al))
        self.assertEqual(len(self.run.alignments), len(orig_als)-1)
        self.run.refresh()
        self.assertEqual(len(self.run.alignments), len(orig_als))

    def test_run_id(self):
        self.assertEqual(self.id_exp, self.run.run_id)


class TestRunSingle(TestRun):
    """Test Run with single-ended sequencing."""

    def setUp(self):
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / RUN_IDS["Single"]
        copy_tree(str(PATH_RUNS / RUN_IDS["Single"]), str(self.path_run))
        self.run = Run(self.path_run)
        self.id_exp = RUN_IDS["Single"]
        date = datetime.datetime(2018, 1, 6, 6, 20, 25, 841000)
        self.rta_exp = {"Date": date, "Version": "Illumina RTA 1.18.54"}
        self.t1_exp = "2018-01-05T13:38:15.2566992-04:00"
        self.t2_exp = "2018-01-05T13:38:45.3021522-04:00"


class TestRunMiniSeq(TestRun):

    def setUp(self):
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / RUN_IDS["MiniSeq"]
        copy_tree(str(PATH_RUNS / RUN_IDS["MiniSeq"]), str(self.path_run))
        self.run = Run(self.path_run)
        self.id_exp = RUN_IDS["MiniSeq"]
        date = datetime.datetime(2018, 1, 4, 11, 14, 00)
        self.rta_exp = {"Date": date, "Version": "RTA 2.8.6"}
        self.t1_exp = "2018-01-04T11:15:03.8237582-04:00"
        self.t2_exp = "2018-08-04T11:16:52.4989741-04:00"


class TestRunMisnamed(TestRun):
    """Test case for a directory whose name is not the Run ID."""

    def setUp(self):
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / RUN_IDS["misnamed"]
        copy_tree(str(PATH_RUNS / RUN_IDS["MiSeq"]), str(self.path_run))
        self.run = Run(self.path_run, strict=False)
        self.id_exp = RUN_IDS["MiSeq"]
        date = datetime.datetime(2018, 1, 1, 6, 21, 31, 705000)
        self.rta_exp = {"Date": date, "Version": "Illumina RTA 1.18.54"}
        self.t1_exp = "2018-01-02T06:48:22.0480092-04:00"
        self.t2_exp = "2018-01-02T06:48:32.608024-04:00"

    def test_init(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            Run(self.path_run, strict=True)
            self.assertEqual(1, len(w))
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            Run(self.path_run)
            self.assertEqual(0, len(w))
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            Run(self.path_run, strict=False)
            self.assertEqual(0, len(w))


class TestRunInvalid(unittest.TestCase):
    """Test case for a directory that is not an Illumina run."""

    def test_init(self):
        path = PATH_RUNS / RUN_IDS["not a run"]
        with self.assertRaises(ValueError):
            Run(path)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            run = Run(path, strict = False)
            self.assertEqual(1, len(w))
            self.assertTrue(run.invalid)
        path = PATH_RUNS / RUN_IDS["nonexistent"]
        with self.assertRaises(ValueError):
            Run(path)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            run = Run(path, strict = False)
            self.assertEqual(1, len(w))
            self.assertTrue(run.invalid)
