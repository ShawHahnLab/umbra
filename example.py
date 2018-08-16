#!/usr/bin/env python

import sys
from pathlib import Path
import csv
import re
import yaml
import illumina.run

PATH_ROOT  = Path(__file__).parent / "testdata"
PATH_RUNS  = PATH_ROOT / "runs"
PATH_EXP   = PATH_ROOT / "experiments"
PATH_ALIGN = PATH_ROOT / "alignments"
PATH_PROC  = PATH_ROOT / "processed"
PATH_PACK  = PATH_ROOT / "packaged"


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


def _parse_contacts(text):
    """Create a dictionary of name/email pairs from contact text.
    
    For example:
    "Name <email@example.com>, Someone Else <user@site.gov>"
    is parsed into:
    {'Name': 'email@example.com', 'Someone Else': 'user@site.gov'}
    """

    chunks = re.split("[,;]+", text)
    contacts = {}
    for chunk in chunks:
        # There's a horrible rabbit hole to go down trying to figure out
        # parsing email addresses with regular expressions.  I don't care.
        # This is enough for us.
        m = re.match(" *([\w ]* *[\w]+) *<(.*@.*)>", chunk)
        name = m.group(1)
        email = m.group(2)
        contacts[name] = email
    return(contacts)

def load_experiment_info(path):
    """Load an Experiment metadata spreadsheet."""
    info = illumina.load_csv(path, csv.DictReader)
    for row in info:
        row["Tasks"] = row["Tasks"].split()
        row["Contacts"] = _parse_contacts(row["Contacts"])
    return(info)

def match_alignment_to_projects(al, dp_exp, dp_align):
    """Add Experiment and Project information to an Alignment."""

    # First off, preload all the sample names and paths for the below.
    # TODO rearrange this to only check if there are unfinished projects.
    try:
        sample_paths = al.sample_paths_by_name()
    except FileNotFoundError as e:
        sys.stderr.write("\n")
        sys.stderr.write("FASTQ file not found:\n")
        sys.stderr.write("Run:       %s\n" % al.run.path)
        sys.stderr.write("Alignment: %s\n" % al.path)
        sys.stderr.write("File:      %s\n" % e.filename)
        sys.stderr.write("\n")
        # These three runs really are missing FASTQ files.
        if not al.run.run_id in ("170724_M00281_0249_000000000-G1BK4", "170725_M00281_0250_000000000-G1BWR", "170828_M00281_0267_000000000-G1D24"):
            raise(e)
    # TODO try loading the associated sample sheet too.  If it can't be
    # found or if it doesn't match the previous sample sheet, throw a
    # warning.
    path = Path(dp_exp) / al.experiment / "metadata.csv"
    al.experiment_info = None
    al.projects = None
    try:
        # Load the spreadsheet of per-sample project information
        al.experiment_info = load_experiment_info(path)
    except FileNotFoundError:
        pass
    else:
        # If that was found, do some extra processing to link up sample and
        # project data.
        al.projects = ProjectData.from_alignment(al)
        for proj_key in al.projects:
            proj = al.projects[proj_key]
            proj.load_processing_status(dp_align = dp_align)
            proj.set_sample_paths(sample_paths)

def slugify(text, mask="_"):
    pat = "[^A-Za-z0-9-_]"
    safe_text = re.sub(pat, mask, text)
    return(safe_text)


def run_setup_with_checks(run_dir):
    """Create a Run object for the given path, or None if no run is found."""
    try:
        run = illumina.run.Run(run_dir)
    except Exception as e:
        # ValueError for unrecognized directories
        if type(e) is ValueError:
            run = None
        else:
            sys.stderr.write("Error while loading run %s\n" % run_dir)
            raise e
    return(run)

def load_run_data(dp, dp_exp, dp_align):
    """Match up Run directories with per-experiment metadata."""
    run_dirs = [d for d in dp.glob("*") if d.is_dir()]
    runs = [run_setup_with_checks(run_dir) for run_dir in run_dirs]
    # Ignore unrecognized (None) entries
    runs = [run for run in runs if run]
    for run in runs:
        for al in run.alignments:
            match_alignment_to_projects(al, dp_exp, dp_align)
    return(runs)

def _fmt_report_entry(entry, N=60):
    entry2 = entry
    for key in entry2:
        data = str(entry2[key])
        if len(data) > N:
            data = data[0:(N-3)] + "..."
        entry2[key] = data
    return(entry2)

def report_entries(runs, out_file=sys.stdout):
    """Write summary of all project data per run to CSV."""
    fieldnames = [
            "RunId",         # Illumina Run ID
            "RunPath",       # Directory path
            "Alignment",     # Alignment number in current set
            "Experiment",    # Name of experiment from sample sheet
            "Project",       # Name of project from extra metadata
            "Status",        # Project data processing status
            "NSamples",      # Num samples in project data
            "NFiles"]        # Num files in project data
    writer = csv.DictWriter(out_file, fieldnames)
    writer.writeheader()
    for run in runs:
        entry = {"RunId": run.run_id,
                 "RunPath": run.path}
        if run.alignments:
            for idx, al in zip(range(len(run.alignments)), run.alignments):
                entry["Alignment"] = idx
                entry["Experiment"] = al.experiment
                if al.projects:
                    for proj_key in al.projects:
                        proj = al.projects[proj_key]
                        entry["Project"] = proj.name
                        entry["Status"] = proj.processing_status["status"]
                        entry["NSamples"] = len(proj.experiment_info["Sample_Names"])
                        entry["NFiles"] = sum([len(x) for x in proj.sample_paths])
                        writer.writerow(_fmt_report_entry(entry))
                else:
                    writer.writerow(_fmt_report_entry(entry))
        else:
            writer.writerow(_fmt_report_entry(entry))

if __name__ == "__main__":
    PATH_RUNS = Path("/seq/runs")
    runs = load_run_data(PATH_RUNS, PATH_EXP, PATH_ALIGN)
    try:
        report_entries(runs)
    except BrokenPipeError:
        pass
