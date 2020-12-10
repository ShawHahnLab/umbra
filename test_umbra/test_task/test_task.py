#!/usr/bin/env python
"""
Tests for Task objects (and classes!)

This tests general behavior of the class and a hypothetical instance.  See the
other test_task_* modules for specific cases.
"""

import unittest
import unittest.mock
import subprocess
import tempfile
import copy
import logging
import importlib
from pathlib import Path
from umbra import task
from ..test_common import log_start, log_stop


class TestTaskModule(unittest.TestCase):
    """Tests on the task module."""

    setUpClass = classmethod(lambda cls: log_start(cls.__module__ + "." + cls.__name__))
    tearDownClass = classmethod(lambda cls: log_stop(cls.__module__ + "." + cls.__name__))

    def setUp(self):
        self.tasks_expected = set((
            "noop",
            "fail",
            "copy",
            "trim",
            "merge",
            "spades",
            "assemble",
            "manual",
            "geneious",
            "metadata",
            "package",
            "email",
            "upload"))

    def tearDown(self):
        # We've made a mess of the task module itself during these tests.
        # Clean up for the next test.
        for name in dir(task):
            if name.startswith("Task"):
                delattr(task, name)
        importlib.reload(importlib.import_module("umbra.task"))

    def test_task_classes(self):
        """Check the dict of task classes for the module.

        The module should bring Task* clases from each task_* child module up
        to the top level, and task_classes should present them in dictionary
        form by name.
        """
        cls_dict = task.task_classes()
        self.assertEqual(set(cls_dict.keys()), self.tasks_expected)

    def test_task_classes_extra(self):
        """Check that loading additional Task classes works."""
        path = "test_umbra/data/other/tasks/task_extra.py"
        with self.assertLogs(level=logging.DEBUG) as logging_context:
            task.load_extra_task_classes(path)
            # 1 file, 1 class
            self.assertEqual(len(logging_context.output), 2)
        cls_dict = task.task_classes()
        self.assertEqual(
            set(cls_dict.keys()),
            self.tasks_expected | set(["extra"]))

    def test_task_classes_extra_dir(self):
        """Check that loading additional Task classes works from dir.

        In this case a second file and task class should be loaded.
        """
        path = "test_umbra/data/other/tasks"
        with self.assertLogs(level=logging.DEBUG) as logging_context:
            task.load_extra_task_classes(path)
            # 2 files, 2 classes
            self.assertEqual(len(logging_context.output), 4)
        cls_dict = task.task_classes()
        self.assertEqual(
            set(cls_dict.keys()),
            self.tasks_expected | set(["extra", "other"]))

    def test_task_classes_missing(self):
        """Check that a missing path for extra task classes is handled."""
        # Simple way to make a nonexistant filename
        tmp = tempfile.NamedTemporaryFile()
        tmp.close()
        path = tmp.name
        # An error should be logged, but no exception raised.
        with self.assertLogs(level=logging.ERROR) as logging_context:
            task.load_extra_task_classes(path)
            self.assertEqual(len(logging_context.output), 1)
        cls_dict = task.task_classes()
        self.assertEqual(set(cls_dict.keys()), self.tasks_expected)


class TestTaskClass(unittest.TestCase):
    """Tests on the Task class itself."""

    setUpClass = classmethod(lambda cls: log_start(cls.__module__ + "." + cls.__name__))
    tearDownClass = classmethod(lambda cls: log_stop(cls.__module__ + "." + cls.__name__))

    def setUp(self):
        self.thing = task.Task

    def test_name(self):
        """Test that the name property is defined."""
        # pylint: disable=no-member
        self.assertEqual(self.thing.name, "task")

    def test_source_path(self):
        """Test that source_path is a Path pointing to the source code."""
        # pylint: disable=no-member
        self.assertEqual(self.thing.source_path, Path("__init__.py"))

    def test_order(self):
        """Test that the numeric order attribute is defined."""
        self.assertEqual(self.thing.order, 100)

    def test_dependencies(self):
        """Test that the dependency list is defined."""
        self.assertEqual(self.thing.dependencies, [])

    def test_summary(self):
        """Test the string summary method."""
        summary_expected = [
            "name: task",
            "order: 100",
            "dependencies: ",
            "source_path: __init__.py"]
        summary_expected = "\n".join(summary_expected)
        self.assertEqual(self.thing.summary, summary_expected)

    def test_sort(self):
        """Test sorting on tasks and all related operators.

        These should work on Tasks as instances or classes."""
        # pylint: disable=abstract-method
        class Task2(task.Task):
            """Another Task class that should come later."""
            order = self.thing.order + 100
        thing2 = Task2({}, None)
        # < > <= >= for class and instance cases
        self.assertTrue(self.thing < Task2)
        self.assertFalse(self.thing > Task2)
        self.assertTrue(self.thing <= Task2)
        self.assertFalse(self.thing >= Task2)
        self.assertTrue(self.thing < thing2)
        self.assertFalse(self.thing > thing2)
        self.assertTrue(self.thing <= thing2)
        self.assertFalse(self.thing >= thing2)
        ## If we make a jumbled list it'll sort properly
        vec = [Task2, self.thing]
        self.assertEqual(vec[::-1], sorted(vec))
        vec = [thing2, self.thing]
        self.assertEqual(vec[::-1], sorted(vec))


class TestTask(TestTaskClass):
    """Tests on any Task instance.

    Everything that works on a Task class should work on a Task instance, and
    more."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        # set up a mock project object for testing
        self.proj = unittest.mock.Mock(
            path_proc=Path(self.tmpdir.name) / "proc",
            nthreads=1,
            conf={},
            sample_paths={"sample_name": ["R1.fastq.gz", "R2.fastq.gz"]},
            work_dir="work_dir_name",
            experiment_info={"tasks": []}
            )
        # Expected values during tests
        self.expected = {
            "nthreads": 1,
            "log_path": self.proj.path_proc / "logs/log_task.txt",
            "sample_paths": copy.deepcopy(self.proj.sample_paths),
            "work_dir_name": "work_dir_name",
            "task_dir_parent": self.proj.path_proc
            }
        self.thing = task.Task({}, self.proj)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_work_dir_name(self):
        """Test work_dir_name attribute that should come from the project."""
        self.assertEqual(
            self.thing.work_dir_name,
            self.expected["work_dir_name"])

    def test_log_path(self):
        """Test log path attribute."""
        self.assertEqual(self.thing.log_path, self.expected["log_path"])

    def test_sample_paths(self):
        """Check dict mapping sample names to sample file paths."""
        self.assertEqual(self.thing.sample_paths, self.expected["sample_paths"])

    def test_nthreads(self):
        """Check number of threads configured for processing."""
        self.assertEqual(self.thing.nthreads, self.expected["nthreads"])

    def test_run(self):
        """Test that the run method is left unimplemented by default."""
        with self.assertRaises(NotImplementedError):
            self.thing.run()

    def test_runwrapper(self):
        """Test task run wrapper method.

        Is the log file setup as expected?  Do exceptions get logged as
        expected?
        """
        # We start with no log yet
        self.check_log_setup(before=True)
        # The parent Task class has no run method implemented yet
        with self.assertRaises(NotImplementedError):
            self.thing.runwrapper()
        # Since we encountered an exception here, the log should be closed and
        # the exception logged.
        self.assertTrue(self.expected["log_path"].exists())
        self.assertFalse(self.is_log_open())
        with open(self.expected["log_path"]) as f_in:
            self.assertTrue("NotImplementedError" in f_in.read())

    def test_runcmd(self):
        """Test wrapper for simple process calls.

        Are commands logged as expected?  Do failing commands rais an exception
        as expected?
        """
        # Commands are executed and logged
        with self.assertLogs(task.LOGGER, level="DEBUG") as logging_context:
            self.thing.runcmd(["true"])
            self.assertEqual(
                logging_context.output,
                ["DEBUG:umbra.task:runcmd: ['true']"])
        self.assertTrue(self.expected["log_path"].exists())
        # Nonzero exit code triggers exception
        with self.assertRaises(subprocess.CalledProcessError):
            self.thing.runcmd(["false"])

    def test_log(self):
        """Check log file path and object.

        Does it start None and is open for writing after setup?  Closed after
        object cleanup?
        """
        self.assertEqual(self.thing.logf, None)
        self.assertFalse(
            self.is_log_open(),
            msg="log file open before use")
        self.thing.log_setup()
        self.assertTrue(
            self.is_log_open(),
            msg="log file not open after setup")
        self.assertFalse(self.thing.logf.closed)
        del self.thing
        self.assertFalse(
            self.is_log_open(),
            msg="log file not closed after cleanup")

    def test_read_file_product(self):
        """Check conversion from read file name to alternate names."""
        # With string
        self.assertEqual(
            "somesample_S1_L001_R1_001.trimmed.fastq",
            self.thing.read_file_product(
                "somesample_S1_L001_R1_001.fastq.gz",
                suffix=".trimmed.fastq",
                merged=False)
            )
        # With path object
        self.assertEqual(
            "somesample_S1_L001_R1_001.trimmed.fastq",
            self.thing.read_file_product(
                Path("somesample_S1_L001_R1_001.fastq.gz"),
                suffix=".trimmed.fastq",
                merged=False)
            )
        # removing R1/R2 segment
        self.assertEqual(
            "somesample_S1_L001_R_001.merged.fastq",
            self.thing.read_file_product(
                "somesample_S1_L001_R1_001.fastq.gz",
                suffix=".merged.fastq")
            )

    def test_task_dir_parent(self):
        """Check parent directory for task outputs."""
        # pylint: disable=no-member
        parent = self.thing.task_dir_parent(self.thing.name)
        self.assertEqual(parent, self.expected["task_dir_parent"])

    def test_log_setup(self):
        """Test log setup helper."""
        self.check_log_setup(before=True)
        self.thing.log_setup()
        self.check_log_setup()

    ### Helper methods for tests above

    def is_log_open(self):
        """True if any process has the log file path open."""
        out = subprocess.run(
            args=["fuser", str(self.expected["log_path"])],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False)
        return out.stdout != b""

    def check_log_setup(self, before=False):
        """Check that log file is created and open for writing."""
        if before:
            # Log is not yet set up; path does not exist and member hasn't been
            # set to a file object.
            self.assertFalse(self.expected["log_path"].exists())
            self.assertTrue(self.thing.logf is None)
        else:
            # Log has been set up.  file exists and member file object is
            # opened for writing.
            self.assertTrue(self.expected["log_path"].exists())
            self.assertTrue(self.thing.logf.writable())


class TestTaskImplicit(TestTask):
    """Tests on a Task implicity included in processing.

    The only thing that changes in this case is that the output files go to the
    ProjectData's implicit tasks path if defined.
    """

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        # set up a mock project object for testing
        self.proj = unittest.mock.Mock(
            path_proc=Path(self.tmpdir.name) / "proc",
            nthreads=1,
            conf={
                "implicit_tasks_path": Path(self.tmpdir.name)/"proc/implicit"},
            sample_paths={"sample_name": ["R1.fastq.gz", "R2.fastq.gz"]},
            work_dir="work_dir_name",
            # Note, no tasks are requested but we're using one named "task"
            # in the tests here, so it should be considered implicit.
            experiment_info={"tasks": []}
            )
        # Expected values during tests
        self.expected = {
            "nthreads": 1,
            "log_path": self.proj.path_proc / "logs/log_task.txt",
            "sample_paths": copy.deepcopy(self.proj.sample_paths),
            "work_dir_name": "work_dir_name",
            "task_dir_parent": self.proj.path_proc / "implicit"
            }
        self.thing = task.Task({}, self.proj)
