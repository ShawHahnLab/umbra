"""
Test for single-task "package".
"""

from .test_project_task import TestProjectDataOneTask

class TestProjectDataPackage(TestProjectDataOneTask):
    """ Test for single-task "package".

    This will barely do anything at all since no files get included in the zip
    file. (TestProjectDataCopy actually makes for a more thorough test.)
    """

    def set_up_vars(self):
        self.task = "package"
        super().set_up_vars()

    def test_process(self):
        # The basic checks
        super().test_process()
        self.check_zipfile([])
