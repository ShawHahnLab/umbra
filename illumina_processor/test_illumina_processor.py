#!/usr/bin/env python

import unittest
from tempfile import TemporaryDirectory
from distutils.dir_util import copy_tree, remove_tree, mkpath
from distutils.file_util import copy_file
from pathlib import Path

# TODO fix this
import sys
sys.path.append(str((Path(__file__).parent/"..").resolve()))
import illumina_processor

PATH_ROOT = Path(__file__).parent / "testdata"

class TestIlluminaProcessor(unittest.TestCase):
    """Main tests for IlluminaProcessor."""

    def setUp(self):
        # Make a full copy of the testdata to a temporary location
        self.tmpdir = TemporaryDirectory()
        copy_tree(PATH_ROOT, self.tmpdir.name)
        path_runs = Path(self.tmpdir.name) / "runs"
        path_exp = Path(self.tmpdir.name) / "experiments"
        path_al = Path(self.tmpdir.name) / "alignments"
        # Create an IlluminaProcessor using the temp files
        self.proc = illumina_processor.IlluminaProcessor(path_runs, path_exp, path_al)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_load_run_data(self):
        # Start with an empty list
        self.assertEqual(self.proc.runs, [])
        # One run dir in particular is named oddly
        warn_msg = "Run directory does not match Run ID: "
        warn_msg += "run-files-custom-name / "
        warn_msg += "180102_M00000_0000_000000000-XXXXX"
        with self.assertWarns(Warning) as cm:
            ws = cm.warnings
            self.proc.load_run_data()
            self.assertEqual(len(ws), 1)
            self.assertEqual(str(ws[0].message), warn_msg)
        # Now we have three loaded runs
        self.assertEqual(len(self.proc.runs), 3)
        # This is different from refresh() because it will fully load in the
        # current data.  If a run directory is gone, for example, it won't be
        # in the list anymore.
        path_run = Path(self.tmpdir.name)/"runs"/"run-files-custom-name"
        remove_tree(str(path_run))
        self.proc.load_run_data()
        self.assertEqual(len(self.proc.runs), 2)

    def test_refresh(self):
        """Basic scenario for refresh(): a new run directory appears."""
        # Note, not running load_run_data manually but it should be handled
        # automatically
        # Start with one run missing, stashed elsewhere
        run_id = "180101_M00000_0000_000000000-XXXXX"
        with TemporaryDirectory() as stash:
            run_orig = str(Path(self.tmpdir.name)/"runs"/run_id)
            run_stash = str(Path(stash)/run_id)
            copy_tree(run_orig, run_stash)
            remove_tree(run_orig)
            # Start with an empty list
            self.assertEqual(self.proc.runs, [])
            # Refresh loads two Runs
            with self.assertWarns(Warning) as cm:
                self.proc.refresh()
            self.assertEqual(len(self.proc.runs), 2)
            # Still just two Runs
            self.proc.refresh()
            self.assertEqual(len(self.proc.runs), 2)
            # Copy run directory back
            copy_tree(run_stash, run_orig)
            # Now, we should load a new Run with refresh()
            self.proc.refresh()
            self.assertEqual(len(self.proc.runs), 3)

    def test_refresh_new_alignment(self):
        """A new alignment directory appears for an existing Run.
        
        We'll step through each possible filesystem situation separately and
        make sure the loader can handle it."""
        run_id = "180102_M00000_0000_000000000-XXXXX"
        path_run = Path(self.tmpdir.name)/"runs"/run_id
        with TemporaryDirectory() as stash:
            align_orig = str(path_run/"Data"/"Intensities"/"BaseCalls"/"Alignment")
            align_stash = str(Path(stash)/"Alignment")
            copy_tree(align_orig, align_stash)
            remove_tree(align_orig)
            # Refresh loads three Runs to start with.
            with self.assertWarns(Warning) as cm:
                self.proc.refresh()
            self.assertEqual(len(self.proc.runs), 3)
            get_al = lambda: self.proc.runs[2].alignments
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
            self.proc.refresh()
            # Now there's an incomplete alignment, right?
            self.assertEqual(len(get_al()), 1)
            self.assertTrue(not get_al()[0].complete)
            # Once Checkpoint.txt shows up, the alignment is presumed complete.
            copy_file(Path(align_stash)/"Checkpoint.txt", align_orig)
            self.proc.refresh()
            self.assertEqual(len(get_al()), 1)
            self.assertTrue(get_al()[0].complete)


if __name__ == '__main__':
    unittest.main()
