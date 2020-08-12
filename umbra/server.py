"""
A REST-like web interface for IlluminaProcessor.
"""

import os
import threading
import asyncio
from pathlib import Path
import yaml
import tornado.ioloop
import tornado.web


class Handler(tornado.web.RequestHandler):
    """An abstract class to link the Server class to tornado.

    Nothing much to see here.  Don't instantiate this directly.
    """
    # pylint: disable=abstract-method

    def get_main(self):
        """Get details on the processor."""
        server = getattr(self, "server", None)
        if server:
            output = server.info_processor()
        else:
            output = {"error": "no server"}
        self.write(yaml.dump(output))

    def get_runs(self, rundir=None):
        """Get a list of run directory names, or info for one run."""
        server = getattr(self, "server", None)
        if server:
            output = server.info_runs(rundir)
        else:
            output = {"error": "no server"}
        self.write(yaml.dump(output))

    def get_datasets(self, dataset=None):
        """Get a list of dataset names, or info for one dataset."""
        server = getattr(self, "server", None)
        if server:
            output = server.info_datasets(dataset)
        else:
            output = {"error": "no server"}
        self.write(yaml.dump(output))

    def get_experiments(self, experiment=None):
        """Get a list of experiment names, or info for one experiment."""
        server = getattr(self, "server", None)
        if server:
            output = server.info_experiments(experiment)
        else:
            output = {"error": "no server"}
        self.write(yaml.dump(output))


class Server:
    """Web server hosting information about an IlluminaProcessor."""

    def __init__(self, address, port, processor):
        self.address = address
        self.port = port
        self.processor = processor

        mkhandler = lambda func: type(
            "Handler",
            (Handler,),
            {"server": self, "get": getattr(Handler, func)})

        self.app = tornado.web.Application([
            (r"/", mkhandler("get_main")),
            (r"/runs/?(.*)", mkhandler("get_runs")),
            (r"/datasets/?(.*)", mkhandler("get_datasets")),
            (r"/experiments/?(.*)", mkhandler("get_experiments"))
            ])

        loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, daemon=True, args=(loop,))

    def start(self):
        """Start the server."""
        self.thread.start()

    def info_processor(self):
        """Get status details for the processor."""
        details = {
            "itemname": "",
            "itemtype": "",
            "message": "",
            "details": {}
            }
        procstatus = self.processor.procstatus
        seqinfo = self.processor.seqinfo

        workers = {}
        for thread, proj in procstatus["projects_by_thread"].items():
            if proj:
                workers[thread.name] = proj.work_dir
            else:
                workers[thread.name] = ""
        details["itemtype"] = "processor"
        details["details"] = {
            "PID": os.getpid(),
            "running": procstatus["running"],
            "runs": len(seqinfo["runs"]),
            "projectdatas": {key: len(val) for key, val in seqinfo["projects"].items()},
            "queued_jobs": procstatus["queue_jobs"].qsize(),
            "completed_jobs": procstatus["queue_completion"].qsize(),
            "workers": workers}
        return details

    def info_runs(self, rundir=None):
        """Get a list of runs or status details for one run."""
        details = {
            "itemname": rundir,
            "itemtype": "run" if rundir else "runs",
            "message": "",
            "details": {}
            }
        if rundir:
            alignment_to_projects = {}
            for key in self.processor.seqinfo["projects"]:
                for proj in self.processor.seqinfo["projects"][key]:
                    if not proj.alignment in alignment_to_projects.keys():
                        alignment_to_projects[proj.alignment] = []
                    alignment_to_projects[proj.alignment].append(proj)
            for run in self.processor.seqinfo["runs"]:
                if run.path.name == rundir:
                    details["details"] = {
                        "run_id": run.run_id,
                        "flowcell": run.flowcell,
                        "complete": run.complete}
                    details["details"]["alignments"] = []
                    if run.alignments:
                        for aln in run.alignments:
                            sheet = aln.sample_sheet
                            projs = alignment_to_projects.get(aln, [])
                            alndata = {
                                "experiment": aln.experiment,
                                "complete": aln.complete,
                                "reads": sheet.get("Reads", []),
                                "sample_count": len(sheet.get("Data", "")),
                                "investigator_name": sheet["Header"].get("Investigator_Name", ""),
                                "projdirs": [p.work_dir for p in projs]}
                            details["details"]["alignments"].append(alndata)
                    break

        else:
            details["details"] = [run.path.name for run in self.processor.seqinfo["runs"]]
        return details

    def info_datasets(self, dataset=None):
        """Get a list of datasets or status details for one dataset."""
        details = {
            "itemname": dataset,
            "itemtype": "dataset" if dataset else "datasets",
            "message": "",
            "details": []
            }
        if dataset:
            for key in self.processor.seqinfo["projects"]:
                for proj in self.processor.seqinfo["projects"][key]:
                    if proj.work_dir == dataset:
                        aln = str(Path(proj.alignment.run.path.name) / str(proj.alignment.index))
                        details["details"] = {
                            "name": proj.name,
                            "readonly": proj.readonly,
                            "tasks_pending": proj.tasks_pending,
                            "tasks_completed": proj.tasks_completed,
                            "task_current": proj.task_current,
                            "task_output": proj.task_output,
                            "work_dir": proj.work_dir,
                            "experiment": proj.experiment_info["name"],
                            "contacts": proj.experiment_info["contacts"].copy(),
                            "sample_names": proj.experiment_info["sample_names"][:],
                            "status": proj.status,
                            "alignment": aln
                            }
        else:
            for key in self.processor.seqinfo["projects"]:
                for proj in self.processor.seqinfo["projects"][key]:
                    details["details"].append(proj.work_dir)
        return details

    def info_experiments(self, experiment=None):
        """Get a list of experiments or status details for one experiment."""
        details = {
            "itemname": experiment,
            "itemtype": "experiment" if experiment else "experiments",
            "message": "",
            "details": {}
            }
        if experiment:
            details["details"]["datasets"] = []
            details["details"]["alignments"] = []
            for key in self.processor.seqinfo["projects"]:
                for proj in self.processor.seqinfo["projects"][key]:
                    if proj.experiment_info["name"] == experiment:
                        details["details"]["datasets"].append(proj.work_dir)
            for run in self.processor.seqinfo["runs"]:
                for aln in run.alignments:
                    if aln.experiment == experiment:
                        details["details"]["alignments"] = str(
                            Path(run.path.name) / str(aln.index))
        else:
            details["details"] = []
            for key in self.processor.seqinfo["projects"]:
                for proj in self.processor.seqinfo["projects"][key]:
                    details["details"].append(proj.experiment_info["name"])
            details["details"] = list(set(details["details"]))
        return details

    def _run(self, loop):
        """Start the event handler using an asyncio loop."""
        # https://stackoverflow.com/a/48726076
        asyncio.set_event_loop(loop)
        self.app.listen(self.port, self.address)
        tornado.ioloop.IOLoop.current().start()
