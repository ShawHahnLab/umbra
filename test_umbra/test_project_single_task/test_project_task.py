"""
ProjectData test classes built around individual tasks.

These are not tidy self-contained units as they should be, since in each case a
real Task is instantiated and there's a lot of overlap with test_task.  Ideally
these parts should be refactored out and what's strictly ProjectData-related
should be under test here.
"""

import re
import csv
import gzip
import zipfile
import warnings
import logging
from pathlib import Path
import yaml
from umbra import illumina, util
from umbra.illumina.run import Run
from umbra.project import ProjectData, ProjectError
from ..test_common import TestBase, md5
from ..test_project import DEFAULT_TASKS

# Single-task ProjectData tests.

class TestProjectDataOneTask(TestBase):
    """Base class for one-project-one-task tests.

    This handles the noop case and can be subclassed for other cases.
    """

    def setUp(self):
        self.task = "noop"
        self.set_up_tmpdir()
        self.set_up_vars()
        self.set_up_run()
        self.write_test_experiment()
        self.set_up_proj()

    def set_up_vars(self):
        # pylint: disable=invalid-name
        self.maxDiff = None
        super().set_up_vars()
        self.config = None
        # Note the write_test_experiment below using this name and contacts_str.
        self.project_name = "TestProject"
        self.contacts_str = "Name Lastname <name@example.com>"
        # Expected and shared attributes
        if self.task in DEFAULT_TASKS:
            tasks = DEFAULT_TASKS[:]
        else:
            tasks = [self.task] + DEFAULT_TASKS
        self.expected = {
            "experiment_name": "Partials_1_1_18",
            "experiment_path": str(
                self.paths["exp"] /
                "Partials_1_1_18" /
                "metadata.csv"),
            "contacts": {"Name Lastname": "name@example.com"},
            "work_dir": "2018-01-01-TestProject-Name-XXXXX",
            "tasks": tasks,
            "sample_names": [
                "1086S1_01",
                "1086S1_02",
                "1086S1_03",
                "1086S1_04"],
            "task_output": {t: {} for t in tasks},
            "initial_status": "none",
            "final_status": "complete"
            }
        self.rundir = "180101_M00000_0000_000000000-XXXXX"

    def set_up_run(self):
        """Set up run object to use for ProjectData tested."""
        self.runobj = Run(self.paths["runs"] / self.rundir)
        self.alignment = self.runobj.alignments[0]

    def write_test_experiment(self):
        """Helper to create an experiment metadata spreadsheet."""
        fieldnames = ["Sample_Name", "Project", "Contacts", "Tasks"]
        exp_row = lambda sample_name: {
            "Sample_Name": sample_name,
            "Project": self.project_name,
            "Contacts": self.contacts_str,
            "Tasks": self.task
            }
        with open(self.expected["experiment_path"], "w", newline="") as f_out:
            writer = csv.DictWriter(f_out, fieldnames)
            writer.writeheader()
            for sample_name in self.expected["sample_names"]:
                writer.writerow((exp_row(sample_name)))

    def set_up_proj(self):
        """Set up ProjectData object to be tested."""
        # Pull out the project we want (to support possibility of additional
        # projects under this run+alignment)
        projs = ProjectData.from_alignment(
            self.alignment,
            self.paths["exp"],
            self.paths["status"],
            self.paths["proc"],
            self.paths["pack"],
            self.uploader,
            self.mailer,
            conf=self.config)
        for proj in projs:
            if proj.name == self.project_name:
                self.proj = proj

    def fake_fastq(self, seq_pair, readlen=100):
        """Helper to create a fake FASTQ file pair for one sample."""
        fastq_paths = self.alignment.sample_paths_for_num(1)
        for proj in fastq_paths:
            proj.chmod(0o644)
        adp = illumina.util.ADAPTERS["Nextera"]
        fills = ("G", "A")
        for path, seq, adpt, fill in zip(fastq_paths, seq_pair, adp, fills):
            read = (seq + adpt).ljust(readlen, fill)
            read = read[0:min(readlen, len(read))]
            qual = ("@" * len(seq)) + ("!" * (len(read)-len(seq)))
            with gzip.open(path, "wt") as f_gz:
                f_gz.write(self._fake_fastq_entry(read, qual))

    @staticmethod
    def _fake_fastq_entry(seq, qual):
        name = md5(seq)
        txt = "@%s\n%s\n+\n%s\n" % (name, seq, qual)
        return txt

    def expected_paths(self, suffix=".trimmed.fastq", r1only=False):
        """Helper to predict the expected FASTQ file paths."""
        paths = []
        for sample in self.expected["sample_names"]:
            i = self.alignment.sample_names.index(sample)
            fps = self.alignment.sample_paths_for_num(i+1) # (1-indexed)
            fps = [path.name for path in fps]
            if r1only:
                paths.append(re.sub("_R1_", "_R_", fps[0]))
            else:
                paths.extend(fps)
        paths = [re.sub("\\.fastq\\.gz$", suffix, p) for p in paths]
        paths = sorted(paths)
        return paths

    def check_log(self):
        """Does the log file exist?

        It always should if a task was run for a project.
        """
        logpath = self.proj.path_proc / "logs" / ("log_"+self.task+".txt")
        self.assertTrue(logpath.exists())

    def check_zipfile(self, files_exp):
        """Check that filenames are present in the zipfile.

        This will also check for the YAML metadata file in the expected hidden
        location."""
        with zipfile.ZipFile(self.proj.path_pack, "r") as f_zip:
            info = f_zip.infolist()
            # Check compression status.  It should actually be compressed, and
            # with the expected method.
            item = info[0]
            self.assertTrue(item.compress_size < item.file_size)
            self.assertEqual(item.compress_type, zipfile.ZIP_DEFLATED)
            # Check that the expected files are all present in the zipfile.
            files = [i.filename for i in info]
            for fp_exp in files_exp:
                path = Path(self.proj.work_dir) / self.runobj.run_id / fp_exp
                with self.subTest(path=path):
                    self.assertIn(str(path), files)

    def test_attrs(self):
        """Test various ProjectData properties."""
        path_stat = self.paths["status"] / self.runobj.run_id / "0"
        self.assertEqual(self.proj.name, self.project_name)
        self.assertEqual(self.proj.alignment, self.alignment)
        self.assertEqual(
            self.proj.path,
            path_stat / (self.project_name + ".yml"))

    def test_task_output(self):
        """Test that the task output dictionary is as expected.

        If processing is expected to complete successfully, we should see a
        specific set of tasks named in the dictionary aftward.
        """
        self.assertEqual(self.proj.task_output, {})
        self.test_process()
        # Just checking for the same set of keys, by default.
        if self.expected["final_status"] == "complete":
            self.assertEqual(
                sorted(self.proj.task_output.keys()),
                sorted(self.expected["task_output"].keys()))
        else:
            self.assertEqual(self.proj.task_output, {})

    def test_work_dir(self):
        """Test the work_dir property string."""
        self.assertEqual(self.proj.work_dir, self.expected["work_dir"])

    def test_contacts(self):
        """Test the user contact info property dict."""
        self.assertEqual(self.proj.contacts, self.expected["contacts"])

    def test_readonly(self):
        """Test the readonly property."""
        self.assertFalse(self.proj.readonly)

    def test_status(self):
        """Test the status property."""
        # Here, we started from scratch.
        self.assertEqual(self.proj.status, self.expected["initial_status"])
        # Is the setter protecting against invalid values?
        with self.assertRaises(ValueError):
            self.proj.status = "invalid status"
        # is the setter magically keeping the data on disk up to date?
        self.proj.status = "processing"
        with open(self.proj.path) as f_in:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=DeprecationWarning)
                data = yaml.safe_load(f_in)
            self.assertEqual(data["status"], "processing")

    def test_experiment_info(self):
        """Test the experiment_info dict property."""
        if hasattr(self, "tasks_listed"):
            # pylint: disable=no-member
            tasks = self.tasks_listed
        else:
            tasks = [self.task]
        exp_info = {
            "name": self.expected["experiment_name"],
            "sample_names": self.expected["sample_names"],
            "tasks": tasks,
            "contacts": self.expected["contacts"],
            "path": self.expected["experiment_path"]
            }
        self.assertEqual(self.proj.experiment_info, exp_info)

    # Test task properties, at start

    def test_tasks_pending(self):
        """Test the tasks_pending list property."""
        self.assertEqual(self.proj.tasks_pending, self.expected["tasks"])

    def test_tasks_completed(self):
        """Test the tasks_completed list property."""
        self.assertEqual(self.proj.tasks_completed, [])

    def test_task_current(self):
        """Test the task_current property."""
        self.assertEqual(self.proj.task_current, "")

    # After processing, we should see the change in each task property.

    def test_process(self):
        """Test that task status reported correctly after process()."""
        # test processing all tasks
        self.proj.process()
        self.assertEqual(self.proj.status, self.expected["final_status"])
        self.assertEqual(self.proj.tasks_pending, [])
        self.assertEqual(self.proj.tasks_completed, self.expected["tasks"])
        self.assertEqual(self.proj.task_current, "")
        self.check_log()


# Other ProjectData test cases

class TestProjectDataFailure(TestProjectDataOneTask):
    """What should happen if an exception is raised during processing?

    The exception should reach the caller but the ProjectData object should do
    some cleanup of its own, trying to update its status to "failed" before
    re-raising the exception.  See also: TestProjectDataFail.
    """

    def setUp(self):
        super().setUp()
        # Processing won't like that there's already something using the path
        # it wants to use.  Let's make sure it fails, but in the way we expect.
        util.mkparent(self.proj.path_proc)
        self.proj.path_proc.touch(mode=0o000)
        self.expected["final_status"] = "failed"

    def test_process(self):
        """Test that process() fails in the expected way.

        It should raise an exception, but still set its status attribute in the
        process and also record the exception details."""
        with self.assertRaises(FileExistsError):
            self.proj.process()
        self.assertEqual(self.proj.status, self.expected["final_status"])
        self.assertEqual(self.proj.tasks_pending, self.expected["tasks"])
        self.assertEqual(self.proj.tasks_completed, [])
        self.assertEqual(self.proj.task_current, "")
        self.assertTrue("failure_exception" in self.proj._metadata.keys())


class TestProjectDataFilesExist(TestProjectDataOneTask):
    """What should happen when there are already files in the processing dir?

    We should log a warning about it and mark the ProjectData as readonly.
    """

    def set_up_vars(self):
        super().set_up_vars()
        self.expected["initial_status"] = "none"
        self.expected["final_status"] = "none"

    def set_up_proj(self):
        # Here we'll get a warning from within __init__ about the unexpected
        # subdirectory.  We'll check other attributes in the tests.
        (self.paths["proc"] /
         self.expected["work_dir"] /
         "subdir").mkdir(parents=True)
        with self.assertLogs(level=logging.WARNING):
            super().set_up_proj()

    def test_readonly(self):
        """Test that readonly=True."""
        self.assertTrue(self.proj.readonly)

    def test_status(self):
        """Test that status attribute works but file is not written."""
        self.assertEqual(self.proj.status, self.expected["initial_status"])
        # We won't touch anything on disk in this case due to the readyonly
        # flag.
        self.assertFalse(self.proj.path.exists())

    def test_process(self):
        """Test that processing throws exception."""
        with self.assertRaises(ProjectError):
            self.proj.process()


class TestProjectDataMissingSamples(TestProjectDataOneTask):
    """What if samples listed in metadata.csv are not in the sample sheet?

    Aside from typos, this could come up if one experiment name and metadata
    spreadsheet matches multiple runs.  This is a bit weird and warrants a
    warning but is allowed.
    """

    def set_up_vars(self):
        super().set_up_vars()
        # Include an extra sample in the experiment metadata spreadsheet that
        # won't be in the run data.
        self.expected["sample_names"] = [
            "1086S1_01", "1086S1_02", "1086S1_03", "1086S1_04", "somethingelse"]

    def set_up_proj(self):
        # The project should be initialized fine, but with a warning logged
        # about the sample name mismatch.
        with self.assertLogs(level=logging.WARNING):
            super().set_up_proj()


class TestProjectDataMissingFiles(TestProjectDataOneTask):
    """What if samples listed in the sample sheet are not on disk?

    This should fail the project.
    """

    def set_up_vars(self):
        super().set_up_vars()
        self.expected["initial_status"] = "failed"
        self.expected["final_status"] = "failed"

    def set_up_tmpdir(self):
        super().set_up_tmpdir()
        for item in (
                Path(self.tmpdir.name) /
                "runs/180101_M00000_0000_000000000-XXXXX" /
                "Data/Intensities/BaseCalls").glob("*.fastq.gz"):
            item.unlink()

    def set_up_proj(self):
        with self.assertLogs(level=logging.ERROR):
            with self.assertWarns(Warning):
                super().set_up_proj()
        self.assertEqual(self.proj.status, self.expected["final_status"])
        self.assertEqual(self.proj.tasks_pending, self.expected["tasks"])
        self.assertEqual(self.proj.tasks_completed, [])
        self.assertEqual(self.proj.task_current, "")

    def test_process(self):
        msg = "ProjectData status already defined as \"failed\""
        with self.assertRaisesRegex(ProjectError, msg):
            super().test_process()


class TestProjectDataNoSamples(TestProjectDataOneTask):
    """What if no samples listed in metadata.csv are in the sample sheet?

    If no samples at all are found it should be an error.  For simplicity in
    setting up multiple projects, we'll log an error and mark the project
    failed, but won't raise an exception.
    """

    def set_up_vars(self):
        super().set_up_vars()
        # Include an extra sample in the experiment metadata spreadsheet that
        # won't be in the run data.
        self.expected["sample_names"] = ["somethingelse1", "somethingelse2"]
        self.expected["initial_status"] = "failed"
        self.expected["final_status"] = "failed"

    def set_up_proj(self):
        with self.assertLogs(level=logging.ERROR):
            super().set_up_proj()

    def test_status(self):
        self.assertEqual(self.proj.status, self.expected["initial_status"])

    def test_process(self):
        """Test that process()ing a failed project is not allowed."""
        with self.assertRaises(ProjectError):
            self.proj.process()
        self.assertEqual(self.proj.status, self.expected["final_status"])
        self.assertEqual(self.proj.tasks_pending, self.expected["tasks"])
        self.assertEqual(self.proj.tasks_completed, [])
        self.assertEqual(self.proj.task_current, "")


class TestProjectDataPathConfig(TestProjectDataOneTask):
    """Test custom output paths for logs and implicitly-dependent tasks.

    This should enable us to have just task directories we explicitly requested
    at the top level of the processed directory, stashing any implicit ones
    (dependencies or defaults) in a single subdirectory, and logs in a specific
    subdirectory.  We can also override that implicit task setting on a
    per-task basis; see TestProjectDataExplicitTasks.
    """

    def set_up_vars(self):
        self.task = "merge"
        super().set_up_vars()
        # merge needs trim.  This way we'll check both implicit-via-defaults
        # and implicit-via-dependencies.
        self.expected["tasks"] = ["trim", self.task] + DEFAULT_TASKS
        self.expected["task_output"] = {t: {} for t in self.expected["tasks"]}
        self.config = {
            "log_path": "RunDiagnostics/logs",
            "implicit_tasks_path": "RunDiagnostics/ImplicitTasks"
            }

    def test_process(self):
        super().test_process()
        metadata_path = (
            self.proj.path_proc /
            self.config["implicit_tasks_path"] /
            "Metadata")
        self.assertTrue(metadata_path.exists())
        trim_path_default = self.proj.path_proc / "trimmed"
        trim_path = (
            self.proj.path_proc /
            self.config["implicit_tasks_path"] /
            "trimmed")
        merge_path = self.proj.path_proc / "PairedReads"
        # Trim path should have changed.  Merge path should have been left the
        # same.
        self.assertFalse(trim_path_default.exists())
        self.assertTrue(trim_path.exists())
        self.assertTrue(merge_path.exists())

    def check_log(self):
        """Override default log path when checking to match config."""
        logpath = (
            self.proj.path_proc /
            self.config["log_path"] /
            ("log_"+self.task+".txt"))
        self.assertTrue(logpath.exists())


class TestProjectDataImplicitTasks(TestProjectDataOneTask):
    """Test custom output paths for other implicit (non-dependency) tasks.

    In live usage processing failed because a late-running task (metadata)
    assumed the parent directories were already created.
    """

    def set_up_vars(self):
        self.task = "trim"
        super().set_up_vars()
        self.config = {
            "implicit_tasks_path": "RunDiagnostics/ImplicitTasks"
            }

    def test_process(self):
        super().test_process()
        metadata_path = (
            self.proj.path_proc /
            self.config["implicit_tasks_path"] /
            "Metadata")
        self.assertTrue(metadata_path.exists())
        # Trim path should not have changed.
        trim_path = self.proj.path_proc / "trimmed"
        self.assertTrue(trim_path.exists())


class TestProjectDataExplicitTasks(TestProjectDataOneTask):
    """Test custom output paths for "always explicit" tasks.

    If we listed a task in always_explicit_tasks it should not be affected by
    implicit_tasks_path, no matter if it was implicitly or explicitly required.
    """

    def set_up_vars(self):
        self.task = "trim"
        super().set_up_vars()
        self.config = {
            "implicit_tasks_path": "RunDiagnostics/ImplicitTasks",
            "always_explicit_tasks": ["metadata"]
            }

    def test_process(self):
        super().test_process()
        # Despite the implicit tasks path given, we should have overridden that
        # here and kept Metadata at the top.
        metadata_path = self.proj.path_proc / "Metadata"
        self.assertTrue(metadata_path.exists())
        # Trim path should not have changed.
        trim_path = self.proj.path_proc / "trimmed"
        self.assertTrue(trim_path.exists())
