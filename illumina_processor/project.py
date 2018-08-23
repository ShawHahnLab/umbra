from .util import *
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


    # ProjectData processing status enumerated type.
    NONE          = "none"
    PROCESSING    = "processing"
    PACKAGE_READY = "package-ready"
    COMPLETE      = "complete"
    STATUS = [NONE, PROCESSING, PACKAGE_READY, COMPLETE]

    def __init__(self, name, alignment=None, run=None):
        self.name = name
        self.alignment = alignment
        self.run = run
        self.metadata = {"status": ProjectData.NONE}
        self.status_fp = None
        #self.sample_paths = None
        exp_info = {
                "sample_names": [],
                "tasks": [],
                "contacts": dict()
                }
        self.metadata["alignment_info"] = {}
        self.metadata["experiment_info"] = exp_info
        self.metadata["run_info"] = {}
        self.metadata["sample_paths"] = {}
        if self.alignment:
            self.metadata["alignment_info"]["path"] = str(self.alignment.path)
            # TODO these don't always exist, even if alignment does.
            self.metadata["experiment_info"]["path"] = str(self.alignment.experiment_path)
            self.metadata["experiment_info"]["name"] = self.alignment.experiment
        if self.run:
            self.metadata["run_info"]["path"] = str(self.run.path)

    @property
    def status(self):
        return(self.metadata["status"])

    @status.setter
    def status(self, value):
        if not value in ProjectData.STATUS:
            raise ValueError
        self.metadata["status"] = value
        self.save_metadata()

    def process(self):
        """Run all tasks."""
        # TODO see illumina's python task library on github maybe?
        pass

    def load_metadata(self, fp=None, dp_align=None):
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
            self.metadata.update(data)

    def save_metadata(self):
        with open(self.status_fp, "w") as f:
            f.write(yaml.dump(self.metadata))

    @property
    def sample_paths(self):
        paths = self.metadata["sample_paths"]
        if not paths: 
            return({})
        paths2 = {}
        for k in paths:
            paths2[k] = [Path(p) for p in paths[k]]
        return(paths2)

    def set_sample_paths(self, sample_paths):
        if sample_paths:
            self.metadata["sample_paths"] = {}
            for sample_name in self.metadata["experiment_info"]["sample_names"]:
                paths = [str(p) for p in sample_paths[sample_name]]
                self.metadata["sample_paths"][sample_name] = paths
        else:
            self.metadata["sample_paths"] = None

    def from_alignment(alignment):
        """Make dict of ProjectData objects from alignment/experiment table."""
        # Row by row, build up a dict for each unique project.  Even though
        # we're reading it in as a spreadsheet we'll treat most of this as
        # an unordered sets for each project.
        projects = {}
        for row in alignment.experiment_info:
            name = row["Project"]
            if not name in projects.keys():
                projects[name] = ProjectData(name, alignment, alignment.run)
            projects[name]._add_exp_row(row) 
        return(projects)

    def _add_exp_row(self, row):
        exp_info = self.metadata["experiment_info"]
        sample_name = row["Sample_Name"].strip()
        if not sample_name in exp_info["sample_names"]:
            exp_info["sample_names"].append(sample_name)
        exp_info["contacts"].update(row["Contacts"])
        for task in row["Tasks"]:
            if not task in exp_info["tasks"]:
                exp_info["tasks"].append(task)
