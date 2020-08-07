#!/usr/bin/env python

import os
import threading
import asyncio
import yaml
import tornado.ioloop
import tornado.web


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        server = getattr(self, "server", None)
        if server:
            info = {"blah": "baz"}
        else:
            info = {"error": "no server"}
        self.write(yaml.dump(info))

class StatusHandler(tornado.web.RequestHandler):
    def get(self):
        server = getattr(self, "server", None)
        if server:
            info = server.get_status()
        else:
            info = {"error": "no server"}
        self.write(yaml.dump(info))

class SubStatusHandler(tornado.web.RequestHandler):
    def get(self, itemname):
        server = getattr(self, "server", None)
        if server:
            info = server.get_status(itemname)
        else:
            info = {"error": "no server"}
        self.write(yaml.dump(info))

class Server:

    def __init__(self, address, port, processor):
        self.address = address
        self.port = port
        self.processor = processor

        main_handler = type("Handler", (MainHandler,), {})
        status_handler = type("Handler", (StatusHandler,), {})
        sub_status_handler = type("Handler", (SubStatusHandler,), {})
        main_handler.server = self
        status_handler.server = self
        sub_status_handler.server = self

        self.app = tornado.web.Application([
            (r"/", main_handler),
            (r"/status/*", status_handler),
            (r"/status/(.+)", sub_status_handler)
            ])

        loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, daemon=True, args=(loop,))

    def start(self):
        self.thread.start()

    def get_status(self, itemname=None):
        details = {
            "itemname": itemname or "",
            "itemtype": "",
            "message": "",
            "details": {}
            }
        procstatus = self.processor.procstatus
        seqinfo = self.processor.seqinfo

        if itemname is None:
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
                        alndata = {
                            "Experiment": aln.experiment,
                            "Complete": aln.complete,
                            "Reads": aln.sample_sheet.get("Reads", []),
                            "SampleCount": len(aln.sample_sheet.get("Data", "")),
                            "Investigator_Name": aln.sample_sheet["Header"].get("Investigator_Name", ""),
                            "projdirs": [p.work_dir for p in alignment_to_projects.get(aln, [])]}
                        alignments.append(alndata)
                details["itemtype"] = "run"
                details["details"] = {
                    "Complete": run.complete,
                    "alignments": alignments
                    }
                return details


    def _run(self, loop):
        # https://stackoverflow.com/a/48726076
        asyncio.set_event_loop(loop)
        self.app.listen(self.port, self.address)
        tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    server = Server("127.0.0.1", 8888, None)
    server.start()
    server.thread.join()
