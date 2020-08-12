"""
A REST-like web interface for IlluminaProcessor.
"""

import os
import threading
import asyncio
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

    def get_sub(self, itemname):
        """Generic get-a-thing method (should split this up)."""
        server = getattr(self, "server", None)
        if server:
            info = server.info_item(itemname)
        else:
            info = {"error": "no server"}
        self.write(yaml.dump(info))

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
            (r"/status/(.+)", mkhandler("get_sub"))
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

    def info_item(self, itemname):
        """Get status details for an individual item."""
        details = {
            "itemname": itemname,
            "itemtype": "",
            "message": "",
            "details": {}
            }
        seqinfo = self.processor.seqinfo

        alignment_to_projects = {}
        for key in seqinfo["projects"]:
            for proj in seqinfo["projects"][key]:
                if not proj.alignment in alignment_to_projects.keys():
                    alignment_to_projects[proj.alignment] = []
                alignment_to_projects[proj.alignment].append(proj)

        for run in self.processor.seqinfo["runs"]:
            if itemname == run.run_id:
                alignments = []
                if run.alignments:
                    for aln in run.alignments:
                        sheet = aln.sample_sheet
                        alndata = {
                            "Experiment": aln.experiment,
                            "Complete": aln.complete,
                            "Reads": sheet.get("Reads", []),
                            "SampleCount": len(sheet.get("Data", "")),
                            "Investigator_Name": sheet["Header"].get("Investigator_Name", ""),
                            "projdirs": [p.work_dir for p in alignment_to_projects.get(aln, [])]}
                        alignments.append(alndata)
                details["itemtype"] = "run"
                details["details"] = {
                    "Complete": run.complete,
                    "alignments": alignments
                    }
                return details
        return details

    def _run(self, loop):
        """Start the event handler using an asyncio loop."""
        # https://stackoverflow.com/a/48726076
        asyncio.set_event_loop(loop)
        self.app.listen(self.port, self.address)
        tornado.ioloop.IOLoop.current().start()
