#!/usr/bin/env python
"""
Tests for IlluminaProcessor objects.

These tests confirm that an IlluminaProcessor handles newly-arriving run data
correctly, dispatches processing to appropriate ProjectData objects, and
coordinates simultaneous processing between multiple projects.
"""

from test_common import *

class TestIlluminaProcessor(TestBase):
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


if __name__ == '__main__':
    unittest.main()
