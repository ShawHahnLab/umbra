#!/usr/bin/env python
"""
Tests for IlluminaProcessor objects.

These tests confirm that an IlluminaProcessor handles newly-arriving run data
correctly, dispatches processing to appropriate ProjectData objects, and
coordinates simultaneous processing between multiple projects.
"""

from .test_common import *
import copy
import io
import warnings

class TestIlluminaProcessor(TestBase):
    """Main tests for IlluminaProcessor."""

    def setUp(self):
        if not hasattr(self, "config"):
            self.config = CONFIG
        self.setUpTmpdir()
        self.setUpProcessor()
        self.setUpVars()

    def setUpProcessor(self):
        self.proc = illumina_processor.IlluminaProcessor(self.path, self.config)

    def setUpVars(self):
        self.num_runs = 4
        self.run_id = "180101_M00000_0000_000000000-XXXXX"
        self.path_run = self.path_runs/self.run_id
        self.warn_msg = ""
        # MD5 sum of the report CSV text, minus the RunPath column.
        self.report_md5 = "40c9bf2b0f99159c9cc061a58ca7bb91"
        # The header entries we expect to see in the CSV report text.
        self.report_fields = [
                "RunId",
                "RunPath",
                "Alignment",
                "Experiment",
                "AlignComplete",
                "Project",
                "Status",
                "NSamples",
                "NFiles",
                "Group"]

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
        remove_tree(str(self.path_run), verbose=True)
        self.proc.load(wait=True)
        self.assertEqual(len(self.proc.runs), self.num_runs-1)

    def test_refresh(self):
        """Basic scenario for refresh(): a new run directory appears."""
        # Note, not running load manually but it should be handled
        # automatically
        # Start with one run missing, stashed elsewhere
        with TemporaryDirectory() as stash:
            run_stash = str(Path(stash)/self.run_id)
            copy_tree(str(self.path_run), run_stash)
            remove_tree(self.path_run)
            # Start with an empty set
            self.assertEqual(self.proc.runs, set())
            proj_exp = {"active": set(), "inactive": set(), "completed": set()}
            self.assertEqual(self.proc.projects, proj_exp)
            # Refresh loads a number of Runs
            self.proc.refresh()
            self.assertEqual(len(self.proc.runs), self.num_runs-1)
            self.assertEqual(self.proc.projects, proj_exp)
            # Still just those Runs
            self.proc.refresh()
            self.assertEqual(len(self.proc.runs), self.num_runs-1)
            self.assertEqual(self.proc.projects, proj_exp)
            # Copy run directory back
            copy_tree(run_stash, str(self.path_run))
            # Now, we should load a new Run with refresh()
            self.assertEqual(self.proc.projects, proj_exp)
            self.proc.start()
            self.proc.refresh(wait=True)
            # Nothing remains to be processed.
            self.assertEqual(len(self.proc.projects["active"]), 0)
            # STR was already complete.
            self.assertEqual(self._proj_names("inactive"), ["STR"])
            # We should have one new completed projectdata now.
            self.assertEqual(self._proj_names("completed"), ["Something Else"])
            self.assertEqual(len(self.proc.runs), self.num_runs)

    def test_create_report(self):
        """Test that create_report() makes the expected list structure."""
        if self.warn_msg:
            with self.assertWarns(Warning) as cm:
                self.proc.load(wait=True)
        else:
            with warnings.catch_warnings():
                self.proc.load(wait=True)
        report = self.proc.create_report()
        # This is a clumsy way of producing a block of CSV text but should be
        # good enough for this simple case.  This should create the same string
        # that report() returns (again, in this simple case).
        # Excluding RunPath since it varies.
        fields = [f for f in self.report_fields if not f == "RunPath"]
        flatten = lambda r: ",".join([str(r[k]) for k in fields])
        txt = "\n".join([flatten(row) for row in report])
        try:
            self.assertEqual(md5(txt), self.report_md5)
        except AssertionError as e:
            print(txt)
            raise(e)

    def test_report(self):
        """Test that report() renders a report to CSV."""
        if self.warn_msg:
            with self.assertWarns(Warning) as cm:
                self.proc.load(wait=True)
        else:
            with warnings.catch_warnings():
                self.proc.load(wait=True)
        # https://stackoverflow.com/a/9157370
        txt = io.StringIO(newline=None)
        self.proc.report(txt)
        txt = txt.getvalue()
        lines = txt.split("\n")
        fields_txt = ",".join(self.report_fields)
        header = lines.pop(0)
        self.assertEqual(header, fields_txt)
        # Excluding RunPath since it varies.
        txt = re.sub(",[^,]+/runs/[^,]+,", ",", "\n".join(lines))
        txt = txt.strip()
        try:
            self.assertEqual(md5(txt), self.report_md5)
        except AssertionError as e:
            print(txt)
            raise(e)

    def test_refresh_new_alignment(self):
        """A new alignment directory appears for an existing Run.
        
        We'll step through each possible filesystem situation separately and
        make sure the loader can handle it."""
        run_id = "180102_M00000_0000_000000000-XXXXX"
        path_run = self.path_runs/run_id
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

    def setUpProcessor(self):
        # including one run that's a duplicate, but it should not become active
        # when the project data is loaded.
        run_orig = str(self.path_runs/"180102_M00000_0000_000000000-XXXXX")
        run_dup = str(self.path_runs/"run-files-custom-name")
        copy_tree(run_orig, run_dup)
        self.proc = illumina_processor.IlluminaProcessor(self.path, self.config)

    def setUpVars(self):
        super().setUpVars()
        self.num_runs = 5
        self.warn_msg = "Run directory does not match Run ID: "
        self.warn_msg += "run-files-custom-name / "
        self.warn_msg += "180102_M00000_0000_000000000-XXXXX"
        # There's an extra line in the report due to the duplicated run
        self.report_md5 = "ccf4d07a070b7f1ce5bf65e587721652"

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


class TestIlluminaProcessorReadonly(TestIlluminaProcessor):
    """Test case for a read-only instance of IlluminaProcessor.
    
    In this mode, IlluminaProcessor will still support the same methods as
    usual, but processing is never started on new ProjectData objects since the
    worker threads aren't run."""

    def setUp(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["readonly"] = True
        super().setUp()

    def setUpVars(self):
        super().setUpVars()
        # All projects inactive in this case
        self.report_md5 = "493730e3e0e7698f92bc5230fad2a25b"

    def test_refresh(self):
        """Basic scenario for refresh(): a new run directory appears.
        
        ProjectData objects are readonly since the processor is readonly, and
        they get marked inactive."""
        with TemporaryDirectory() as stash:
            run_stash = str(Path(stash)/self.run_id)
            copy_tree(str(self.path_run), run_stash)
            remove_tree(self.path_run)
            # Start with an empty set
            self.assertEqual(self.proc.runs, set())
            proj_exp = {"active": set(), "inactive": set(), "completed": set()}
            self.assertEqual(self.proc.projects, proj_exp)
            # Refresh loads a number of Runs
            self.proc.refresh()
            self.assertEqual(len(self.proc.runs), self.num_runs-1)
            self.assertEqual(self.proc.projects, proj_exp)
            # Still just those Runs
            self.proc.refresh()
            self.assertEqual(len(self.proc.runs), self.num_runs-1)
            self.assertEqual(self.proc.projects, proj_exp)
            # Copy run directory back
            copy_tree(run_stash, str(self.path_run))
            # Now, we should load a new Run with refresh()
            self.assertEqual(self.proc.projects, proj_exp)
            self.proc.refresh(wait=True)
            # All loaded runs are inactive since we're readonly.
            self.assertEqual(self._proj_names("inactive"), ["STR", "Something Else"])
            self.assertEqual(self._proj_names("completed"), [])
            self.assertEqual(self._proj_names("active"), [])
            self.assertEqual(len(self.proc.runs), self.num_runs)

if __name__ == '__main__':
    unittest.main()
