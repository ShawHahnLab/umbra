from util import *
import yaml

class ProjectData:
    """The subset of files for a Run and Alignment specific to one project.
    
    This references the data files within a specific run relevant to a single
    project, tracks the associated additional metadata provided via the
    "experiment" identified in the sample sheet, and handles post-processing.

    The same project may span many separate runs, but a ProjectData object
    refers only to a specific portion of a single Run.
    """
    # TODO pass in experiment metadata for relevant samples
    # track run/alignment_num/project_name at this point.

    def __init__(self, name, alignment=None):
        self.name = name
        self.alignment = alignment
        self.processing_status = {"status": "none"}
        self.status_fp = None
        self.sample_paths = []
        self.experiment_info = {
                "Sample_Names": set(),
                "Tasks": set(),
                "Contacts": dict()
                }

    def process():
        """Run all tasks."""
        # TODO see illumina's python task library on github maybe?
        pass

    def load_processing_status(self, fp=None, dp_align=None):
        if fp is None and dp_align is None:
            raise ValueError
        elif fp is None:
            al = self.alignment
            al_idx = str(al.run.alignments.index(al))
            proj_file = slugify(self.name) + ".yml"
            fp = Path(dp_align) / al.run.run_id / al_idx / proj_file
        self.status_fp = fp
        try:
            with open(fp) as f:
                data = yaml.safe_load(f)
        except FileNotFoundError:
            pass
        else:
            self.processing_status = data

    def set_sample_paths(self, sample_paths):
        self.sample_paths = []
        for sample_name in self.experiment_info["Sample_Names"]:
            self.sample_paths.append(sample_paths[sample_name])

    def from_alignment(alignment):
        """Make dict of ProjectData objects from alignment/experiment table."""
        # Row by row, build up a dict for each unique project.  Even though
        # we're reading it in as a spreadsheet we'll treat most of this as
        # an unordered sets for each project.
        exp_info = alignment.experiment_info
        projects = {}
        for row in exp_info:
            name = row["Project"]
            if not name in projects.keys():
                projects[name] = ProjectData(name, alignment)
            projects[name]._add_exp_row(row) 
        return(projects)

    def _add_exp_row(self, row):
        self.experiment_info["Sample_Names"].add(row["Sample_Name"])
        self.experiment_info["Contacts"].update(row["Contacts"])
        self.experiment_info["Tasks"].update(set(row["Tasks"]))
