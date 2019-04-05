#!/usr/bin/env python
"""
Tests for Task objects (and classes!)

Right now this mostly just tests the weird Python inheritance and other aspects
of tasks, and test_project.py has more "live" testing for actual task code.
"""

import unittest
from pathlib import Path
from umbra import task

class TestTaskModule(unittest.TestCase):
    """Tests on the task module."""

    def test_task_classes(self):
        """Check the dict of task classes for the module.

        The module should bring Task* clases from each task_* child module up
        to the top level, and task_classes should present them in dictionary
        form by name.
        """
        cls_dict = task.task_classes()
        keys_expected = set((
            "noop",
            "fail",
            "copy",
            "trim",
            "assemble",
            "merge",
            "manual",
            "geneious",
            "metadata",
            "package",
            "email",
            "upload"))
        self.assertEqual(set(cls_dict.keys()), keys_expected)


class TestTaskClass(unittest.TestCase):
    """Tests on the Task class itself."""

    def setUp(self):
        self.thing = task.Task

    def test_name(self):
        """Test that the name property is defined."""
        self.assertEqual(self.thing.name, "task")

    def test_source_path(self):
        """Test that source_path is a Path pointing to the source code."""
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
            order = 200
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
        self.thing = task.Task({}, None)

    def test_run(self):
        """Test that the run method is left unimplemented by default."""
        with self.assertRaises(NotImplementedError):
            self.thing.run()
