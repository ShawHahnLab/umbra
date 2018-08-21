#!/usr/bin/env python

from illumina_processor import *

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
                        entry["Status"] = proj.status
                        entry["NSamples"] = len(proj.metadata["experiment_info"]["sample_names"])
                        entry["NFiles"] = sum([len(x) for x in proj.sample_paths.values()])
                        writer.writerow(_fmt_report_entry(entry))
                else:
                    writer.writerow(_fmt_report_entry(entry))
        else:
            writer.writerow(_fmt_report_entry(entry))

if __name__ == "__main__":
    PATH_RUNS = Path("/seq/runs")
    proc = IlluminaProcessor(PATH_RUNS, PATH_EXP, PATH_ALIGN)
    proc.load_run_data()
    proc.watch_and_process()
    #try:
    #    report_entries(proc.runs)
    #except BrokenPipeError:
    #    pass
