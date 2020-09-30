#!/usr/bin/env python
"""
Tests for ProjectData objects.

These tests confirm that the portions of a run specific to a single project are
processed as expected, including the trimming/merging/assembling tasks and
calling external uploading/emailing functions (but not the outcome of those
calls).  This does not handle anything to do with multithreading between
multiple simultaneous projects, either; see test_umbra.py for that.
"""

import unittest
import warnings
import datetime
import logging
from pathlib import Path
from unittest.mock import Mock
import yaml
from umbra.illumina.run import Run
from umbra.project import ProjectData, ProjectError
from .test_common import TestBase

DEFAULT_TASKS = ["metadata", "package", "upload", "email"]

class TestProjectData(TestBase):
    """Main tests for ProjectData.

    This became unwieldy pretty fast.  Merge into TestProjectDataOneTask,
    maybe."""

    def set_up_vars(self):
        super().set_up_vars()
        # pylint: disable=invalid-name
        # (don't blame me, pylint, I didn't make unittest)
        self.maxDiff = None
        self.runobj = Run(self.paths["runs"] / "180101_M00000_0000_000000000-XXXXX")
        self.alignment = self.runobj.alignments[0]
        self.projs = ProjectData.from_alignment(
            self.alignment,
            self.paths["exp"],
            self.paths["status"],
            self.paths["proc"],
            self.paths["pack"],
            self.uploader,
            self.mailer)
        # switch to dictionary to make these easier to work with
        self.projs = {p.name: p for p in self.projs}
        self.exp_path = str(self.paths["exp"] / "Partials_1_1_18" / "metadata.csv")
        # Make sure we have what we expect before the real tests
        self.assertEqual(
            sorted(self.projs.keys()),
            ["STR", "Something Else"])

    def test_attrs(self):
        """Test various ProjectData properties."""
        path_stat = self.paths["status"] / self.runobj.run_id / "0"
        self.assertEqual(self.projs["STR"].name, "STR")
        self.assertEqual(self.projs["STR"].alignment, self.alignment)
        self.assertEqual(self.projs["STR"].path, path_stat / "STR.yml")
        self.assertEqual(self.projs["Something Else"].name, "Something Else")
        self.assertEqual(self.projs["Something Else"].alignment, self.alignment)
        self.assertEqual(
            self.projs["Something Else"].path,
            path_stat / "Something_Else.yml")

    def test_work_dir(self):
        """Test the work_dir property string."""
        # make sure it gives "date-project-names"
        # test any edge cases that may come up for those components
        works = {
            # The date stamp is by completion date of the alignment, not
            # start date of the run (i.e., the date encoded in the Run ID).
            "STR": "2018-01-01-STR-Jesse-XXXXX",
            # The names are organized in the order presented in the
            # experiment metadata spreadsheet.  Any non-alphanumeric
            # characters are converted to _.
            "Something Else": "2018-01-01-Something_Else-Someone-Jesse-XXXXX"
            }
        for key in works:
            self.assertEqual(works[key], self.projs[key].work_dir)

    def test_metadata(self):
        """Test that the project metadata is set up as expected.

        These are edging toward private attributes but it's easier here to just
        check them all in one go, at least until that's more cleaned up."""

        mdata = {}
        mdata["status"] = "none"
        mdata["run_info"] = {"path": str(self.runobj.path)}
        mdata["sample_paths"] = {}
        mdata["alignment_info"] = {"path": str(self.alignment.path)}
        mdata["run_info"]["path"] = str(self.alignment.run.path)

        md_str = dict(mdata)
        md_se = dict(mdata)

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
        md_se["work_dir"] = "2018-01-01-Something_Else-Someone-Jesse-XXXXX"
        md_str["work_dir"] = "2018-01-01-STR-Jesse-XXXXX"

        fastq = self.paths["runs"]/"180101_M00000_0000_000000000-XXXXX/Data/Intensities/BaseCalls"
        fps = {
            "1086S1_01": [str(fastq/"1086S1-01_S1_L001_R1_001.fastq.gz"),
                          str(fastq/"1086S1-01_S1_L001_R2_001.fastq.gz")],
            "1086S1_02": [str(fastq/"1086S1-02_S2_L001_R1_001.fastq.gz"),
                          str(fastq/"1086S1-02_S2_L001_R2_001.fastq.gz")]
            }
        md_str["sample_paths"] = fps
        fps = {
            "1086S1_03": [str(fastq/"1086S1-03_S3_L001_R1_001.fastq.gz"),
                          str(fastq/"1086S1-03_S3_L001_R2_001.fastq.gz")],
            "1086S1_04": [str(fastq/"1086S1-04_S4_L001_R1_001.fastq.gz"),
                          str(fastq/"1086S1-04_S4_L001_R2_001.fastq.gz")]
            }
        md_se["sample_paths"] = fps

        self.assertEqual(self.projs["STR"]._metadata, md_str)
        self.assertEqual(self.projs["Something Else"]._metadata, md_se)

    def test_readonly(self):
        """Test the readonly property."""
        self.assertTrue(self.projs["STR"].readonly)
        self.assertFalse(self.projs["Something Else"].readonly)

    def test_status(self):
        """Test the status property."""
        # Is the status what we expect from the initial metadata on disk?
        self.assertEqual(self.projs["STR"].status, "complete")
        self.assertEqual(self.projs["Something Else"].status, "none")
        # Is the setter protecting against invalid values?
        with self.assertRaises(ValueError):
            self.projs["STR"].status = "invalid status"
        # is the setter magically keeping the data on disk up to date?
        self.projs["Something Else"].status = "processing"
        with open(self.projs["Something Else"].path) as f_in:
            # https://stackoverflow.com/a/1640777
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=DeprecationWarning)
                data = yaml.safe_load(f_in)
            self.assertEqual(data["status"], "processing")

    def test_process(self):
        """Test the process method to actually run all tasks.

        Make sure exceptions are logged to metadata, too.  So far we have STR
        marked as complete, and Something Else with no config yet.  A read-only
        ProjectData (including previously-complete ones) cannot be
        re-processed.
        """
        with self.assertRaises(ProjectError):
            self.projs["STR"].process()
        proj = self.projs["Something Else"]
        proj.process()
        self.assertEqual(proj.status, "complete")



@unittest.skip("not yet implemented")
class TestProjectDataBlank(TestBase):
    """Test with no tasks at all.

    This just needs to confim that the TASK_NULL code correctly inserts "copy"
    when nothing else is specified.  put it in self.expected["tasks"].
    """


@unittest.skip("not yet implemented")
class TestProjectDataAlreadyProcessing(TestBase):
    """Test project whose existing metadata points to an existent process.

    We should abort in that case.
    """


class TestProjectDataFromAlignment(TestBase):
    """Tests for ProjectData.from_alignment."""

    def set_up_vars(self):
        super().set_up_vars()
        fqgz_path = (
            Path(self.tmpdir.name).resolve() /
            "runs/180101_M00000_0000_000000000-XXXXX/Data/Intensities/BaseCalls")
        sample_paths = {
            "1086S1_01": [
                fqgz_path / "1086S1-01_S1_L001_R1_001.fastq.gz",
                fqgz_path / "1086S1-01_S1_L001_R2_001.fastq.gz"],
            "1086S1_02": [
                fqgz_path / "1086S2-01_S2_L001_R1_001.fastq.gz",
                fqgz_path / "1086S2-01_S2_L001_R2_001.fastq.gz"],
            "1086S1_03": [
                fqgz_path / "1086S3-01_S3_L001_R1_001.fastq.gz",
                fqgz_path / "1086S3-01_S3_L001_R2_001.fastq.gz"],
            "1086S1_04": [
                fqgz_path / "1086S4-01_S4_L001_R1_001.fastq.gz",
                fqgz_path / "1086S4-01_S4_L001_R2_001.fastq.gz"]
            }
        self.alignment = Mock(
            experiment="Partials_1_1_18",
            index=0,
            path=fqgz_path / "Alignment",
            sample_paths=lambda: sample_paths,
            run=Mock(
                run_id="runid",
                flowcell="000000000-XXXXX",
                rta_complete={"Date": datetime.datetime(2018, 1, 1)}))

    def test_from_alignment(self):
        """Basic alignment situation."""
        projs = ProjectData.from_alignment(
            self.alignment,
            self.paths["exp"],
            self.paths["status"],
            self.paths["proc"],
            self.paths["pack"],
            self.uploader,
            self.mailer)
        self.assertEqual(len(projs), 2)

    def test_from_alignment_iso8859(self):
        """Experiment metadata.csv has weird characters.

        Here the weird characters should be removed and a warning should be
        logged.
        """
        self.alignment.experiment = "iso8859"
        with self.assertLogs(level=logging.WARNING) as log_cm:
            projs = ProjectData.from_alignment(
                self.alignment,
                self.paths["exp"],
                self.paths["status"],
                self.paths["proc"],
                self.paths["pack"],
                self.uploader,
                self.mailer)
            self.assertEqual(len(log_cm.output), 1)
            self.assertIn("Unrecognized character", log_cm.output[0])
        self.assertEqual(len(projs), 2)


if __name__ == '__main__':
    unittest.main()
