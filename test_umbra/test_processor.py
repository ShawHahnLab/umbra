#!/usr/bin/env python
"""
Tests for IlluminaProcessor objects.

These tests confirm that an IlluminaProcessor handles newly-arriving run data
correctly, dispatches processing to appropriate ProjectData objects, and
coordinates simultaneous processing between multiple projects.
"""

import unittest
import copy
import io
import re
import warnings
import threading
import logging
from pathlib import Path
from distutils.dir_util import copy_tree, remove_tree, mkpath
from distutils.file_util import copy_file
from tempfile import TemporaryDirectory
import umbra.processor
from umbra.processor import IlluminaProcessor
from umbra.project import ProjectData
from .test_common import TestBase, CONFIG, md5, DumbLogHandler

class TestIlluminaProcessor(TestBase):
    """Main tests for IlluminaProcessor."""

    def setUp(self):
        self.set_up_tmpdir()
        self.set_up_config()
        self.set_up_processor()
        self.set_up_vars()

    def set_up_config(self):
        """Initialize config object to give to IlluminaProcessor."""
        if not hasattr(self, "config"):
            self.config = CONFIG

    def set_up_processor(self):
        """Initialize IlluminaProcessor for testing."""
        self.proc = IlluminaProcessor(self.paths["top"], self.config)

    def set_up_vars(self):
        self.mails = []
        # This covers the expected situation checked for in the tests.
        self.expected = {
            "num_runs": 5,
            "run_id": "180101_M00000_0000_000000000-XXXXX",
            "warn_msg": "",
            # MD5 sum of the report CSV text, minus the RunPath column.  This
            # is after fulling loading the default data, but before starting
            # processing.
            "report_md5": "03937ca84ebc70d670f3e4b9650f4a1a",
            # The header entries we expect to see in the CSV report text.
            "report_fields": [
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
            }
        self.path_run = self.paths["runs"] / self.expected["run_id"]
        # Temporary path to use for a report
        self.report_path = Path(self.tmpdir.name) / "report.csv"

    def _proj_names(self, category):
        return sorted([p.name for p in self.proc.seqinfo["projects"][category]])

    def test_load(self):
        """Test load method for loading directory of run data."""
        # Start with an empty set
        self.assertEqual(self.proc.seqinfo["runs"], set([]))
        self.proc.load(wait=True)
        # Now we have loaded runs
        self.assertEqual(len(self.proc.seqinfo["runs"]), self.expected["num_runs"])
        # This is different from refresh() because it will fully load in the
        # current data.  If a run directory is gone, for example, it won't be
        # in the list anymore.
        remove_tree(str(self.path_run), verbose=True)
        self.proc.load(wait=True)
        self.assertEqual(
            len(self.proc.seqinfo["runs"]),
            max(0, self.expected["num_runs"] - 1))

    def test_refresh(self):
        """Basic scenario for refresh(): a new run directory appears."""
        # Note, not running load manually but it should be handled
        # automatically
        # Start with one run missing, stashed elsewhere
        with TemporaryDirectory() as stash:
            run_stash = str(Path(stash)/self.expected["run_id"])
            copy_tree(str(self.path_run), run_stash)
            remove_tree(self.path_run)
            # Start with an empty set
            self.assertEqual(self.proc.seqinfo["runs"], set())
            proj_exp = {"active": set(), "inactive": set(), "completed": set()}
            self.assertEqual(self.proc.seqinfo["projects"], proj_exp)
            # Refresh loads a number of Runs
            self.proc.refresh()
            self.assertEqual(len(self.proc.seqinfo["runs"]), self.expected["num_runs"] - 1)
            self.assertEqual(self.proc.seqinfo["projects"], proj_exp)
            # Still just those Runs
            self.proc.refresh()
            self.assertEqual(len(self.proc.seqinfo["runs"]), self.expected["num_runs"] - 1)
            self.assertEqual(self.proc.seqinfo["projects"], proj_exp)
            # Copy run directory back
            copy_tree(run_stash, str(self.path_run))
            # Now, we should load a new Run with refresh()
            self.assertEqual(self.proc.seqinfo["projects"], proj_exp)
            self.proc.start()
            self.proc.refresh(wait=True)
            # Nothing remains to be processed.
            self.assertEqual(len(self.proc.seqinfo["projects"]["active"]), 0)
            # STR was already complete.
            self.assertEqual(self._proj_names("inactive"), ["STR"])
            # We should have one new completed projectdata now.
            self.assertEqual(self._proj_names("completed"), ["Something Else"])
            self.assertEqual(len(self.proc.seqinfo["runs"]), self.expected["num_runs"])

    def _load_maybe_warning(self):
        if self.expected["warn_msg"]:
            with self.assertWarns(Warning) as _:
                self.proc.load(wait=True)
        else:
            with warnings.catch_warnings():
                self.proc.load(wait=True)

    def _watch_and_process_maybe_warning(self):
        timer = threading.Timer(1, self.proc.finish_up)
        timer.start()
        if self.expected["warn_msg"]:
            with self.assertWarns(Warning) as _:
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
        fields = [f for f in self.expected["report_fields"] if not f == "RunPath"]
        flatten = lambda r: ",".join([str(r[k]) for k in fields])
        txt = "\n".join([flatten(row) for row in report])
        try:
            self.assertEqual(md5(txt), self.expected["report_md5"])
        except AssertionError as err:
            print(txt)
            raise err

    def _check_csv(self, txt, report_md5=None):
        if not report_md5:
            report_md5 = self.expected["report_md5"]
        lines = txt.split("\n")
        fields_txt = ",".join(self.expected["report_fields"])
        header = lines.pop(0)
        self.assertEqual(header, fields_txt)
        # Excluding RunPath since it varies.
        txt = re.sub(",[^,]+/runs/[^,]+,", ",", "\n".join(lines))
        txt = txt.strip()
        try:
            self.assertEqual(md5(txt), report_md5)
        except AssertionError as err:
            print(txt)
            raise err

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
        with open(self.report_path) as f_in:
            txt = f_in.read()
        self._check_csv(txt)

    def test_watch_and_process(self):
        """Test the main watch_and_process loop."""
        self._watch_and_process_maybe_warning()
        # By default no report is generated.  It needs to be configured
        # explicitly.
        self.assertFalse(Path(self.report_path).exists())

    def test_refresh_new_alignment(self):
        """A new alignment directory appears for an existing Run.

        We'll step through each possible filesystem situation separately and
        make sure the loader can handle it."""
        if not self.expected["num_runs"]:
            raise unittest.SkipTest("No run data expected; skipping test")
        run_id = "180102_M00000_0000_000000000-XXXXX"
        path_run = self.paths["runs"]/run_id
        get_run = lambda: [r for r in self.proc.seqinfo["runs"] if r.path.name == run_id][0]
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
            self.assertEqual(len(self.proc.seqinfo["runs"]), self.expected["num_runs"])
            # Third run has no alignments yet
            self.assertEqual(len(get_al()), 0)
            # Create empty Alignment directory, as if it's just starting off
            # and hasn't received any data yet
            mkpath(align_orig)
            with self.assertWarns(Warning) as _:
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

    def set_up_processor(self):
        # including one run that's a duplicate, but it should not become active
        # when the project data is loaded.
        run_orig = str(self.paths["runs"]/"180102_M00000_0000_000000000-XXXXX")
        run_dup = str(self.paths["runs"]/"run-files-custom-name")
        copy_tree(run_orig, run_dup)
        self.proc = IlluminaProcessor(self.paths["top"], self.config)

    def set_up_vars(self):
        super().set_up_vars()
        self.expected["num_runs"] = 6
        self.expected["warn_msg"] = "Run directory does not match Run ID: "
        self.expected["warn_msg"] += "run-files-custom-name / "
        self.expected["warn_msg"] += "180102_M00000_0000_000000000-XXXXX"
        # There's an extra line in the report due to the duplicated run
        self.expected["report_md5"] = "4aea937d44b81dfc92307999b032ce64"

    def test_load(self):
        # One run dir in particular is named oddly and is a duplicate of the
        # original run.
        with self.assertWarns(Warning) as warning_context:
            warn_list = warning_context.warnings
            self.proc.load(wait=True)
            self.assertEqual(len(warn_list), 1)
            self.assertEqual(
                str(warn_list[0].message),
                self.expected["warn_msg"])
        # Now we have loaded runs
        self.assertEqual(len(self.proc.seqinfo["runs"]), self.expected["num_runs"])

    def test_refresh(self):
        """Basic scenario for refresh(): a new run directory appears."""
        with self.assertWarns(Warning) as warning_context:
            super().test_refresh()
            warn_list = warning_context.warnings
            self.assertEqual(len(warn_list), 1)
            self.assertEqual(
                str(warn_list[0].message),
                self.expected["warn_msg"])

    def test_refresh_new_alignment(self):
        """A new alignment directory appears for an existing Run (with duplicate).

        This should be the same situation as the regular version, but with an
        extra warning about that mismatched run."""
        with self.assertWarns(Warning) as warning_context:
            super().test_refresh_new_alignment()
            warn_list = warning_context.warnings
            self.assertEqual(len(warn_list), 1)
            self.assertEqual(
                str(warn_list[0].message),
                self.expected["warn_msg"])


class TestIlluminaProcessorReadonly(TestIlluminaProcessor):
    """Test case for a read-only instance of IlluminaProcessor.

    In this mode, IlluminaProcessor will still support the same methods as
    usual, but processing is never started on new ProjectData objects since the
    worker threads aren't run."""

    def set_up_config(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["readonly"] = True

    def set_up_vars(self):
        super().set_up_vars()
        # All projects inactive in this case
        self.expected["report_md5"] = "1fb1fce577c9bf7b0124ed56dac43fd6"

    def test_refresh(self):
        """Basic scenario for refresh(): a new run directory appears.

        ProjectData objects are readonly since the processor is readonly, and
        they get marked inactive."""
        with TemporaryDirectory() as stash:
            run_stash = str(Path(stash)/self.expected["run_id"])
            copy_tree(str(self.path_run), run_stash)
            remove_tree(self.path_run)
            # Start with an empty set
            self.assertEqual(self.proc.seqinfo["runs"], set())
            proj_exp = {"active": set(), "inactive": set(), "completed": set()}
            self.assertEqual(self.proc.seqinfo["projects"], proj_exp)
            # Refresh loads a number of Runs
            self.proc.refresh()
            self.assertEqual(len(self.proc.seqinfo["runs"]), self.expected["num_runs"] - 1)
            self.assertEqual(self.proc.seqinfo["projects"], proj_exp)
            # Still just those Runs
            self.proc.refresh()
            self.assertEqual(len(self.proc.seqinfo["runs"]), self.expected["num_runs"] - 1)
            self.assertEqual(self.proc.seqinfo["projects"], proj_exp)
            # Copy run directory back
            copy_tree(run_stash, str(self.path_run))
            # Now, we should load a new Run with refresh()
            self.assertEqual(self.proc.seqinfo["projects"], proj_exp)
            self.proc.refresh(wait=True)
            # All loaded runs are inactive since we're readonly.
            self.assertEqual(self._proj_names("inactive"), ["STR", "Something Else"])
            self.assertEqual(self._proj_names("completed"), [])
            self.assertEqual(self._proj_names("active"), [])
            self.assertEqual(len(self.proc.seqinfo["runs"]), self.expected["num_runs"])


class TestIlluminaProcessorReportConfig(TestIlluminaProcessor):
    """Test customization of the report configuration."""

    def set_up_config(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["save_report"] = {}
        path = Path(self.tmpdir.name) / "report.csv"
        self.config["save_report"]["path"] = path
        self.config["save_report"]["max_width"] = 60

    def test_watch_and_process(self):
        # watch_and_process will automatically call start(), and the wrapper
        # used in the test below will wait, so we'll get a completed
        # ProjectData for one case.  Need an MD5 for a slightly different
        # report in that case.
        report_md5 = "16f00124545186c2070e8adee26d1aad"
        self._watch_and_process_maybe_warning()
        # If a report was configured, it should exist
        with open(self.report_path) as f_in:
            txt = f_in.read()
        self._check_csv(txt, report_md5)


class TestIlluminaProcessorMinRunAge(TestIlluminaProcessor):
    """Test case for a required min run age for IlluminaProcessor.

    With this feature enabled, runs newer than a fixed age (by ctime on the run
    directory) will be skipped."""

    def set_up_config(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["min_age"] = 60 # seconds

    def set_up_vars(self):
        super().set_up_vars()
        # Here we don't expect any runs to be loaded since they're too new.
        # The report should be empty.
        self.expected["num_runs"] = 0
        self.expected["report_md5"] = md5("")

    def test_refresh(self):
        """Test that an age setting prevents loading too new or old runs.

        Also, once a skipped run is logged it should not be logged again.
        """
        with TemporaryDirectory() as stash:
            run_stash = str(Path(stash)/self.expected["run_id"])
            copy_tree(str(self.path_run), run_stash)
            remove_tree(self.path_run)
            # Start with an empty set
            self.assertEqual(self.proc.seqinfo["runs"], set())
            proj_exp = {"active": set(), "inactive": set(), "completed": set()}
            self.assertEqual(self.proc.seqinfo["projects"], proj_exp)
            logger = umbra.processor.LOGGER
            # Refresh loads a number of Runs
            handler = DumbLogHandler()
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
            self.proc.refresh()
            self.assertTrue(
                handler.has_message_text("skipping run; timestamp"),
                "Run skipped but not logged as expected")
            handler.records = []
            self.assertEqual(self.proc.seqinfo["runs"], set())
            self.assertEqual(self.proc.seqinfo["projects"], proj_exp)
            # Copy run directory back
            copy_tree(run_stash, str(self.path_run))
            # Now, we should load a new Run with refresh()
            self.assertEqual(self.proc.seqinfo["projects"], proj_exp)
            self.proc.start()
            self.proc.refresh(wait=True)
            self.assertFalse(
                handler.has_message_text("skipping run; timestamp"),
                "Run already skipped but incorrectly logged again")
            # Except we still haven't loaded any yet (too new)
            self.assertEqual(self.proc.seqinfo["projects"], proj_exp)
            logger.removeHandler(handler)
            logger.setLevel(logging.NOTSET)


class TestIlluminaProcessorMinRunAgeZero(TestIlluminaProcessor):
    """Test case #2 for a required min run age for IlluminaProcessor.

    This time runs should be loaded since they're old enough to pass the
    filter."""

    def set_up_config(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["min_age"] = 0


class TestIlluminaProcessorMaxRunAgeZero(TestIlluminaProcessorMinRunAge):
    """Test case for a max allowed run age for IlluminaProcessor.

    With this feature enabled, runs older than a fixed age (by ctime on the run
    directory) will be skipped.  This inherits from the minimum-age test case
    since we can re-use the behavior of a high min-age to test a low
    max-age."""

    def set_up_config(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["max_age"] = 0


class TestIlluminaProcessorMaxRunAge(TestIlluminaProcessorMinRunAgeZero):
    """Test case #2 for a max allowed run age for IlluminaProcessor.

    This time runs should be loaded since they're new enough to pass the
    filter."""

    def set_up_config(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["max_age"] = 60


class TestIlluminaProcessorFailure(TestIlluminaProcessor):
    """Test case for a processing failure.

    When processing for a project fails, log messages and an email alert should
    be generated, and the processing status should be set to
    ProjectData.FAILED.  (The processor then moves on with no interruption.)"""

    def set_up_config(self):
        self.config = copy.deepcopy(CONFIG)
        self.config["mailer"]["to_addrs_on_error"] = ["admin@example.com"]

    def setUp(self):
        super().setUp()
        # Use the dummy storing mailer provided by TestBase.  We'll make sure
        # it's called with the right arguments when processing fails.
        self.proc.mailerobj = lambda: None
        self.proc.mailerobj.mail = self.mailer
        # Tell the project to throw a ProjectError during processing.
        # Previously I used a write-protected file to cause it to fail, but now
        # we do more upfront checking so a contrived failure is the easiest
        # way.
        fp_md = self.paths["exp"] / "Partials_1_1_18/metadata.csv"
        with open(fp_md) as f_in:
            lines = f_in.readlines()
        failify = lambda line: re.sub(",[A-Za-z]*$", ",fail", line)
        lines = [lines[0]] + [failify(line) for line in lines[1:]]
        with open(fp_md, "w") as f_out:
            f_out.writelines(lines)

    def test_refresh(self):
        """Test that project failure during refresh is logged as expected."""
        # On refresh, the processing failure should be caught and filed as a
        # log message.
        self.proc.start()
        with self.assertLogs(level=logging.ERROR):
            self.proc.refresh(wait=True)
        # A mail should have been "sent"
        self.assertEqual(len(self.mails), 1)
        self.assertEqual(self.mails[0]["to_addrs"], ["admin@example.com"])
        # Overall structure of the projects should be the same, but the
        # completed one should be marked as failed.
        self.assertEqual(self._proj_names("active"), [])
        self.assertEqual(self._proj_names("inactive"), ["STR"])
        self.assertEqual(self._proj_names("completed"), ["Something Else"])
        completed = self.proc.seqinfo["projects"]["completed"]
        self.assertEqual(completed.pop().status, ProjectData.FAILED)

    def _watch_and_process_maybe_warning(self):
        # watch_and_process() should log an error when it calls refresh(), as
        # tested above.
        timer = threading.Timer(1, self.proc.finish_up)
        timer.start()
        with self.assertLogs(level=logging.ERROR):
            if self.expected["warn_msg"]:
                with self.assertWarns(Warning):
                    self.proc.watch_and_process(poll=1, wait=True)
            else:
                with warnings.catch_warnings():
                    self.proc.watch_and_process(poll=1, wait=True)
        self.proc.wait_for_jobs()

if __name__ == '__main__':
    unittest.main()
