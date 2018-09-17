#!/usr/bin/env python

import unittest
from tempfile import TemporaryDirectory
from distutils.dir_util import copy_tree, remove_tree, mkpath
from distutils.file_util import copy_file
from pathlib import Path
import yaml
import re

# TODO fix this
import sys
sys.path.append(str((Path(__file__).parent/"..").resolve()))
import illumina_processor
from illumina_processor import ProjectData
from illumina_processor.illumina.run import Run
from illumina_processor.illumina.alignment import Alignment

PATH_ROOT = Path(__file__).parent / "testdata"

class TestIlluminaProcessorBase(unittest.TestCase):
    """Some setup/teardown shared with the real test classes."""

    def setUp(self):
        self.setUpTmpdir()

    def tearDown(self):
        self.tearDownTmpdir()

    def setUpTmpdir(self):
        # Make a full copy of the testdata to a temporary location
        self.tmpdir = TemporaryDirectory()
        copy_tree(PATH_ROOT, self.tmpdir.name)
        self.path = Path(self.tmpdir.name)
        self.path_runs   = Path(self.tmpdir.name) / "runs"
        self.path_exp    = Path(self.tmpdir.name) / "experiments"
        self.path_status = Path(self.tmpdir.name) / "status"
        self.path_proc   = Path(self.tmpdir.name) / "processed"
        self.path_pack   = Path(self.tmpdir.name) / "packaged"
        self.path_report = Path(self.tmpdir.name) / "report.csv"

    def tearDownTmpdir(self):
        self.tmpdir.cleanup()


class TestIlluminaProcessor(TestIlluminaProcessorBase):
    """Main tests for IlluminaProcessor."""

    def setUp(self):
        self.setUpTmpdir()
        # Create an IlluminaProcessor using the temp files
        self.proc = illumina_processor.IlluminaProcessor(self.path)
        self.num_runs = 4

    def _proj_names(self, category):
        return(sorted([p.name for p in self.proc.projects[category]]))

    def test_load(self):
        # Start with an empty set
        self.assertEqual(self.proc.runs, set([]))
        self.proc.load(wait=True)
        # Now we have loaded runs
        self.assertEqual(len(self.proc.runs), self.num_runs)
        # This is different from refresh() because it will fully load in the
        # current data.  If a run directory is gone, for example, it won't be
        # in the list anymore.
        path_run = self.path_runs/"180101_M00000_0000_000000000-XXXXX"
        remove_tree(str(path_run), verbose=True)
        #print(self.path_runs.glob("*"))
        self.proc.load(wait=True)
        self.assertEqual(len(self.proc.runs), self.num_runs-1)

    def test_refresh(self):
        """Basic scenario for refresh(): a new run directory appears."""
        # Note, not running load manually but it should be handled
        # automatically
        # Start with one run missing, stashed elsewhere
        run_id = "180101_M00000_0000_000000000-XXXXX"
        with TemporaryDirectory() as stash:
            run_orig = str(self.path_runs/run_id)
            run_stash = str(Path(stash)/run_id)
            copy_tree(run_orig, run_stash)
            remove_tree(run_orig)
            # Start with an empty set
            self.assertEqual(self.proc.runs, set())
            proj_exp = {"active": set(), "inactive": set(), "completed": set()}
            self.assertEqual(self.proc.projects, proj_exp)
            # Refresh loads two Runs
            self.proc.refresh()
            self.assertEqual(len(self.proc.runs), self.num_runs-1)
            self.assertEqual(self.proc.projects, proj_exp)
            # Still just two Runs
            self.proc.refresh()
            self.assertEqual(len(self.proc.runs), self.num_runs-1)
            self.assertEqual(self.proc.projects, proj_exp)
            # Copy run directory back
            copy_tree(run_stash, run_orig)
            # Now, we should load a new Run with refresh()
            self.assertEqual(self.proc.projects, proj_exp)
            self.proc.refresh(wait=True)
            # Nothing remains to be processed.
            self.assertEqual(len(self.proc.projects["active"]), 0)
            # STR was already complete.
            self.assertEqual(self._proj_names("inactive"), ["STR"])
            # We should have one new completed projectdata now.
            self.assertEqual(self._proj_names("completed"), ["Something Else"])
            self.assertEqual(len(self.proc.runs), self.num_runs)

    def test_refresh_new_alignment(self):
        """A new alignment directory appears for an existing Run.
        
        We'll step through each possible filesystem situation separately and
        make sure the loader can handle it."""
        run_id = "180102_M00000_0000_000000000-XXXXX"
        path_run = Path(self.tmpdir.name)/"runs"/run_id
        get_run = lambda: [r for r in self.proc.runs if r.path.name == run_id][0]
        get_al = lambda: get_run().alignments
        with TemporaryDirectory() as stash:
            align_orig = str(path_run/"Data"/"Intensities"/"BaseCalls"/"Alignment")
            align_stash = str(Path(stash)/"Alignment")
            copy_tree(align_orig, align_stash)
            remove_tree(align_orig)
            # Refresh loads all Runs to start with.
            #with self.assertWarns(Warning) as cm:
            #    self.proc.refresh()
            self.proc.refresh(wait=True)
            self.assertEqual(len(self.proc.runs), self.num_runs)
            # Third run has no alignments yet
            self.assertEqual(len(get_al()), 0)
            # Create empty Alignment directory, as if it's just starting off
            # and hasn't received any data yet
            mkpath(align_orig)
            with self.assertWarns(Warning) as cm:
                self.proc.refresh()
            # Third run still has no alignments since the sample sheet isn't
            # there yet, and that's a defining feature for an Alignment,
            # complete or no.
            self.assertEqual(len(get_al()), 0)
            # OK, now there's a sample sheet so the alignment should load.
            copy_file(Path(align_stash)/"SampleSheetUsed.csv", align_orig)
            self.proc.refresh(wait=True)
            # Now there's an incomplete alignment, right?
            self.assertEqual(len(get_al()), 1)
            self.assertTrue(not get_al()[0].complete)
            # Once Checkpoint.txt shows up, the alignment is presumed complete.
            copy_file(Path(align_stash)/"Checkpoint.txt", align_orig)
            self.proc.refresh(wait=True)
            self.assertEqual(len(get_al()), 1)
            self.assertTrue(get_al()[0].complete)


class TestIlluminaProcessorDuplicateRun(TestIlluminaProcessor):
    """Test case for a second run directory for an existing Run."""

    def setUp(self):
        self.setUpTmpdir()
        run_orig = str(self.path_runs/"180102_M00000_0000_000000000-XXXXX")
        run_dup = str(self.path_runs/"run-files-custom-name")
        copy_tree(run_orig, run_dup)
        # Create an IlluminaProcessor using the temp files
        self.proc = illumina_processor.IlluminaProcessor(self.path)
        # including one run that's a duplicate, but it should not become active
        # when the project data is loaded.
        self.num_runs = 5
        self.warn_msg = "Run directory does not match Run ID: "
        self.warn_msg += "run-files-custom-name / "
        self.warn_msg += "180102_M00000_0000_000000000-XXXXX"

    def test_load(self):
        # One run dir in particular is named oddly and is a duplicate of the
        # original run.
        with self.assertWarns(Warning) as cm:
            ws = cm.warnings
            self.proc.load(wait=True)
            self.assertEqual(len(ws), 1)
            self.assertEqual(str(ws[0].message), self.warn_msg)
        # Now we have loaded runs
        self.assertEqual(len(self.proc.runs), self.num_runs)

    def test_refresh(self):
        """Basic scenario for refresh(): a new run directory appears."""
        with self.assertWarns(Warning) as cm:
            super().test_refresh()
            ws = cm.warnings
            self.assertEqual(len(ws), 1)
            self.assertEqual(str(ws[0].message), self.warn_msg)

    def test_refresh_new_alignment(self):
        """A new alignment directory appears for an existing Run (with duplicate).
        
        This should be the same situation as the regular version, but with an
        extra warning about that mismatched run."""
        with self.assertWarns(Warning) as cm:
            super().test_refresh_new_alignment()
            ws = cm.warnings
            self.assertEqual(len(ws), 1)
            self.assertEqual(str(ws[0].message), self.warn_msg)


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
        self.assertEqual(len(self.projs), 2)
        self.assertEqual(sorted(self.projs.keys()), ["STR", "Something Else"])

    def test_attrs(self):
        self.assertEqual(self.projs["STR"].name, "STR")
        self.assertEqual(self.projs["STR"].alignment, self.alignment)
        self.assertEqual(self.projs["STR"].path, self.path_status / self.run.run_id / "0" / "STR.yml")
        self.assertEqual(self.projs["Something Else"].name, "Something Else")
        self.assertEqual(self.projs["Something Else"].alignment, self.alignment)
        self.assertEqual(self.projs["Something Else"].path, self.path_status / self.run.run_id / "0" / "Something_Else.yml")

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

    def test_normalize_tasks(self):
        # to test:
        #   invalid task triggers ValueError
        #   task order is set correctly
        #   dependencies get included
        #   defaults are included
        self.fail("test not yet implemented")

    def test_run_task(self):
        # test processing a single pending task
        self.fail("test not yet implemented")

    def test_process(self):
        # test processing all tasks
        # make sure exceptions are logged to metadata, too.
        # so far we have STR marked as complete, and Something Else with no
        # config yet.
        self.fail("test not yet implemented")

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
