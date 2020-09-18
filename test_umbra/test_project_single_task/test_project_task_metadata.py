"""
Test for single-task "metadata".
"""

import csv
from pathlib import Path
from .test_project_task import TestProjectDataOneTask
from ..test_common import md5

class TestProjectDataMetadata(TestProjectDataOneTask):
    """ Test for single-task "metadata".

    Test that a ProjectData gets the expected metadata files copied over to the
    working directory.
    """

    def set_up_vars(self):
        self.task = "metadata"
        super().set_up_vars()

    def test_process(self):
        # The basic checks
        super().test_process()
        # Also make sure that the expected files are there
        # SampleSheetUsed.csv from Alignment
        # metadata.csv for just this project
        # YAML for this project
        path_root = Path(self.proj.path_proc) / "Metadata"
        paths = {
            "sample_sheet": path_root / "SampleSheetUsed.csv",
            "metadata":     path_root / "metadata.csv",
            "yaml":         path_root / self.proj.path.name
            }
        path_md5s = {
            "sample_sheet": "1fbbfa008ef3038560e9904f3d8579a2",
            "metadata":     "5ef2284fced0e08105c0670518ae2eae"
            }
        # First off, they should all be there.
        for path in paths.values():
            self.assertTrue(path.exists())
        # They should also have the expected contents.  (YAML has paths so it'd
        # be random, though.  We'll skip that one.)
        for key, md5_exp in path_md5s.items():
            with self.subTest(md_file=key):
                path = paths[key]
                with open(path, 'rb') as f_in:
                    contents = f_in.read().decode("ASCII")
                md5_obs = md5(contents)
                self.assertEqual(md5_obs, md5_exp)

    def write_test_experiment(self):
        # We need a more nuanced experiment metadata spreadsheet here to test
        # that only this project's rows are copied.
        fieldnames = ["Sample_Name", "Project", "Contacts", "Tasks"]
        exp_row = lambda sample_name, pname: {
            "Sample_Name": sample_name,
            "Project": pname,
            "Contacts": self.contacts_str,
            "Tasks": self.task
            }
        sample_names_extra = ["1086S1_05", "1086S2_01", "1086S2_02", "1086S2_03"]
        with open(self.expected["experiment_path"], "w", newline="") as f_out:
            writer = csv.DictWriter(f_out, fieldnames)
            writer.writeheader()
            for sample_name in self.expected["sample_names"]:
                writer.writerow((exp_row(sample_name, self.project_name)))
            for sample_name in sample_names_extra:
                writer.writerow((exp_row(sample_name, "ZZ_Another")))
