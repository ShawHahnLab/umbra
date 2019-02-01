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
import threading
from umbra.processor import IlluminaProcessor
from umbra.project import ProjectData

class TestIlluminaProcessor(TestBase):
    """Main tests for IlluminaProcessor."""

    def setUp(self):
        self.setUpTmpdir()
        self.setUpConfig()
        self.setUpProcessor()
        self.setUpVars()

    def setUpConfig(self):
        if not hasattr(self, "config"):
            self.config = CONFIG

    def setUpProcessor(self):
        self.proc = IlluminaProcessor(self.path, self.config)

    def setUpVars(self):
        self.num_runs = 4
        self.run_id = "180101_M00000_0000_000000000-XXXXX"
        self.path_run = self.path_runs/self.run_id
        self.warn_msg = ""
        self.mails = []
        # MD5 sum of the report CSV text, minus the RunPath column.
        # This is after fulling loading the default data, but before starting
        # processing.
        self.report_md5 = "a6288d49f223ad85b564069701bc8b6f"
        # Temporary path to use for a report
        self.report_path = Path(self.tmpdir.name) / "report.csv"
        # The header entries we expect to see in the CSV report text.
        self.report_fields = [
                "RunId",
                "RunPath",
                "Alignment",
                "Experiment",
                "AlignComplete",
                "Project",
                "WorkDir",
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
        self.assertEqual(len(self.proc.runs), max(0, self.num_runs-1))

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

    def _load_maybe_warning(self):
        if self.warn_msg:
            with self.assertWarns(Warning) as cm:
                self.proc.load(wait=True)
        else:
            with warnings.catch_warnings():
                self.proc.load(wait=True)

    def _watch_and_process_maybe_warning(self):
        t = threading.Timer(1, self.proc.finish_up)
        t.start()
        if self.warn_msg:
            with self.assertWarns(Warning) as cm:
                self.proc.watch_and_process(poll=1, wait=True)
        else:
            with warnings.catch_warnings():
                self.proc.watch_and_process(poll=1, wait=True)
        self.proc.wait_for_jobs()

    def test_create_report(self):
        """Test that create_report() makes the expected list structure."""
        self._load_maybe_warning()
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

    def _check_csv(self, txt):
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

    def test_report(self):
        """Test that report() renders a report to CSV."""
        self._load_maybe_warning()
        # https://stackoverflow.com/a/9157370
        txt = io.StringIO(newline=None)
        self.proc.report(txt)
        txt = txt.getvalue()
        self._check_csv(txt)

    def test_save_report(self):
        """Test that save_report() renders a report to a CSV file."""
        self._load_maybe_warning()
        self.proc.save_report(self.report_path)
        with open(self.report_path) as f:
            txt = f.read()
        self._check_csv(txt)

    def test_watch_and_process(self):
        self._watch_and_process_maybe_warning()
        # By default no report is generated.  It needs to be configured
        # explicitly.
        self.assertFalse(Path(self.report_path).exists())

    def test_refresh_new_alignment(self):
        """A new alignment directory appears for an existing Run.
        
        We'll step through each possible filesystem situation separately and
        make sure the loader can handle it."""
        if not self.num_runs:
            raise unittest.SkipTest("No run data expected; skipping test")
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
        self.proc = IlluminaProcessor(self.path, self.config)

    def setUpVars(self):
        super().setUpVars()
        self.num_runs = 5
        self.warn_msg = "Run directory does not match Run ID: "
        self.warn_msg += "run-files-custom-name / "
        self.warn_msg += "180102_M00000_0000_000000000-XXXXX"
        # There's an extra line in the report due to the duplicated run
        self.report_md5 = "6d3a716cc86a08f382e29474847f018b"

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

    def setUpConfig(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["readonly"] = True

    def setUpVars(self):
        super().setUpVars()
        # All projects inactive in this case
        self.report_md5 = "513fb89c2d3341352ad34e791eae5fdf"

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


class TestIlluminaProcessorReportConfig(TestIlluminaProcessor):

    def setUpConfig(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["save_report"] = {}
        path = Path(self.tmpdir.name) / "report.csv"
        self.config["save_report"]["path"] = path
        self.config["save_report"]["max_width"] = 60

    def test_watch_and_process(self):
        # watch_and_process will automatically call start(), and this wrapper
        # will wait, so we'll get a completed ProjectData for one case.
        # Need an MD5 for a slightly different report in that case
        self.report_md5 = "c815570da24ce8e556d8e50b454a8eb1"
        self._watch_and_process_maybe_warning()
        # If a report was configured, it should exist
        with open(self.report_path) as f:
            txt = f.read()
        self._check_csv(txt)


class TestIlluminaProcessorMinRunAge(TestIlluminaProcessor):
    """Test case for a required min run age for IlluminaProcessor.
    
    With this feature enabled, runs newer than a fixed age (by ctime on the run
    directory) will be skipped."""

    def setUpConfig(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["min_age"] = 60 # seconds

    def setUpVars(self):
        super().setUpVars()
        # Here we don't expect any runs to be loaded since they're too new.
        # The report should be empty.
        self.num_runs = 0
        self.report_md5 = md5("")

    def test_refresh(self):
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
            self.assertEqual(self.proc.runs, set())
            self.assertEqual(self.proc.projects, proj_exp)
            # Copy run directory back
            copy_tree(run_stash, str(self.path_run))
            # Now, we should load a new Run with refresh()
            self.assertEqual(self.proc.projects, proj_exp)
            self.proc.start()
            self.proc.refresh(wait=True)
            # Except we still haven't loaded any yet (too new)
            self.assertEqual(self.proc.projects, proj_exp)


class TestIlluminaProcessorMinRunAgeZero(TestIlluminaProcessor):
    """Test case #2 for a required min run age for IlluminaProcessor.
    
    This time runs should be loaded since they're old enough to pass the
    filter."""

    def setUpConfig(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["min_age"] = 0


class TestIlluminaProcessorMaxRunAgeZero(TestIlluminaProcessorMinRunAge):
    """Test case for a max allowed run age for IlluminaProcessor.
    
    With this feature enabled, runs older than a fixed age (by ctime on the run
    directory) will be skipped.  This inherits from the minimum-age test case
    since we can re-use the behavior of a high min-age to test a low
    max-age."""

    def setUpConfig(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["max_age"] = 0


class TestIlluminaProcessorMaxRunAge(TestIlluminaProcessorMinRunAgeZero):
    """Test case #2 for a max allowed run age for IlluminaProcessor.
    
    This time runs should be loaded since they're new enough to pass the
    filter."""

    def setUpConfig(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["max_age"] = 60


class TestIlluminaProcessorFailure(TestIlluminaProcessor):
    """Test case for a processing failure.
    
    When processing for a project fails, log messages and an email alert should
    be generated, and the processing status should be set to
    ProjectData.FAILED.  (The processor then moves on with no interruption.)"""

    def setUpConfig(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["mailer"]["to_addrs_on_error"] = ["admin@example.com"]

    def setUp(self):
        super().setUp()
        # Use the dummy storing mailer provided by TestBase.  We'll make sure
        # it's called with the right arguments when processing fails.
        self.proc.mailer = self.mailer
        # Tell the project to throw a ProjectError during processing.
        # Previously I used a write-protected file to cause it to fail, but now
        # we do more upfront checking so a contrived failure is the easiest
        # way.
        fp = self.path_exp / "Partials_1_1_18/metadata.csv"
        with open(fp) as f:
            lines = f.readlines()
        failify = lambda line: re.sub(",[A-Za-z]*$", ",fail", line)
        lines = [lines[0]] + [failify(line) for line in lines[1:]]
        with open(fp, "w") as f:
            f.writelines(lines)

    def test_refresh(self):
        """Test that project failure during refresh is logged as expected."""
        self.proj_str = "2018-01-01-Something_Else-Someone-Jesse"
        # On refresh, the processing failure should be caught and filed as a
        # log message.
        self.proc.start()
        with self.assertLogs(level = logging.ERROR) as cm:
            self.proc.refresh(wait=True)
        # A mail should have been "sent"
        self.assertEqual(len(self.mails), 1)
        self.assertEqual(self.mails[0]["to_addrs"], ["admin@example.com"])
        # Overall structure of the projects should be the same, but the
        # completed one should be marked as failed.
        self.assertEqual(self._proj_names("active"), [])
        self.assertEqual(self._proj_names("inactive"), ["STR"])
        self.assertEqual(self._proj_names("completed"), ["Something Else"])
        completed = self.proc.projects["completed"]
        self.assertEqual(completed.pop().status, ProjectData.FAILED)

    def _watch_and_process_maybe_warning(self):
        # watch_and_process() should log an error when it calls refresh(), as
        # tested above.
        t = threading.Timer(1, self.proc.finish_up)
        t.start()
        with self.assertLogs(level = logging.ERROR) as cm:
            if self.warn_msg:
                with self.assertWarns(Warning) as cm:
                    self.proc.watch_and_process(poll=1, wait=True)
            else:
                with warnings.catch_warnings():
                    self.proc.watch_and_process(poll=1, wait=True)
        self.proc.wait_for_jobs()

if __name__ == '__main__':
    unittest.main()
