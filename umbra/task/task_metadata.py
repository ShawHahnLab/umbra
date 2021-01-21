"""Copy metadata spreadsheets and YAML into working directory."""

import shutil
import csv
from umbra import task
from umbra.illumina.util import load_csv

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
        # Read in all the rows in the original metadata spreadsheet.  We'll
        # ignore anything non-unicode here since it should have already been
        # complained about when first parsed.  (There's also the already-parsed
        # data structure within self.proj but the original line-by-line CSV is
        # already gone at that point.)  Ideally the non_unicode behavior would
        # be defined in one place instead of both here and in ProjectData.
        data = load_csv(self.proj.exp_path, csv.DictReader, non_unicode="strip")
        with open(path_md_out, "w") as f_out:
            # Write out the experiment spreadsheet but only include our rows
            writer = csv.DictWriter(f_out, fieldnames=data[0].keys())
            writer.writeheader()
            for row in data:
                if row["Project"] == self.proj.name:
                    writer.writerow(row)
