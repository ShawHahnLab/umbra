"""
Test for single-task "upload".
"""

from .test_project_task import TestProjectDataOneTask

class TestProjectDataUpload(TestProjectDataOneTask):
    """ Test for single-task "upload".

    The uploader here is a stub that just returns a fake URL, so this doesn't
    test much, just that the URL is recorded as expected.
    """

    def set_up_vars(self):
        self.task = "upload"
        super().set_up_vars()

    def test_process(self):
        # The basic checks
        super().test_process()
        # After processing, there should be a URL recorded for the upload task.
        url_obs = self.proj.task_output["upload"]["url"]
        self.assertEqual(url_obs[0:8], "https://")
