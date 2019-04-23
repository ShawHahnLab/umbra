"""Copy metadata spreadsheets and YAML into working directory."""

import shutil
import csv
from umbra import task

class TaskMetadata(task.Task):
    """Copy metadata spreadsheets and YAML into working directory."""

    # pylint: disable=no-member
    order = 1000

    def run(self):
        dest = self.task_dir_parent(self.name) / "Metadata"
        dest.mkdir(parents=True, exist_ok=True)
        paths = []
        paths.append(self.proj.alignment.paths["sample_sheet"]) # Sample Sheet
        paths.append(self.proj.path) #  Project metadata YAML file (as it currently stands)
        for path in paths:
            shutil.copy(path, dest)
        # metadata is special: need to filter out other projects
        path_md_out = dest / self.proj.exp_path.name
        with open(self.proj.exp_path) as f_in, open(path_md_out, "w") as f_out:
            # Read in all the rows in the original metadata spreadsheet
            reader = csv.DictReader(f_in)
            data = list(reader)
            # Here, write out the experiment spreadsheet but only include our
            # rows
            writer = csv.DictWriter(f_out, fieldnames=data[0].keys())
            writer.writeheader()
            for row in data:
                if row["Project"] == self.proj.name:
                    writer.writerow(row)
