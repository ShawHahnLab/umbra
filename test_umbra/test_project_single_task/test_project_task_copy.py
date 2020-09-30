"""
Test for single-task "copy".
"""

from .test_project_task import TestProjectDataOneTask

class TestProjectDataCopy(TestProjectDataOneTask):
    """ Test for single-task "copy".

    Here the whole run directory should be copied into the processing directory
    and zipped."""

    def set_up_vars(self):
        self.task = "copy"
        super().set_up_vars()

    def test_process(self):
        # The basic checks
        super().test_process()
        # Here we should have a copy of the raw run data inside of the work
        # directory.
        # The top-level work directory should contain the run directory and the
        # default Metadata directory.
        dirpath = self.proj.path_proc
        dir_exp = sorted(["Metadata", "logs", self.runobj.run_id])
        dir_obs = sorted([x.name for x in dirpath.glob("*")])
        self.assertEqual(dir_obs, dir_exp)
        # The files in the top-level of the run directory should match, too.
        files_in = lambda d, s: [x.name for x in d.glob(s) if x.is_file()]
        files_exp = sorted(files_in(self.runobj.path, "*"))
        files_obs = sorted(files_in(dirpath / self.runobj.run_id, "*"))
        self.assertEqual(files_obs, files_exp)
        self.check_zipfile(files_exp)
