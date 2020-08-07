#!/usr/bin/env python

import threading
import asyncio
import yaml
import tornado.ioloop
import tornado.web

##########
# mockups

# projectdata, run, experiment, etc.
ITEMS = {
    "200807_M00281_0618_000000000-G63CJ": "run",
    "SHIV_T250": "project",
    "2020-08-07-SHIV_T250-Shuyi-D97YJ": "projectdata"}

class StatusInfo:


    # is this helpful?
    TYPES = ["processor", "run", "project", "projectdata"]

    def __init__(self, itemname=None):
        self.details = {
            "itemname": itemname or "",
            "itemtype": "",
            "message": "",
            "details": {}
            }
        if itemname is None:
            self.details["itemtype"] = "processor"
            self.details["details"] = {
                "PID": 5792,
                # To figure out who's doing what, I think I need to pull
                # information from IlluminaProcessor.procstatus["queue_jobs"]
                # but are the in-progress ones visible between .get() and .task_done()?
                # Doesn't look like it.
                "workers": {
                    }}
        elif itemname in ITEMS:
            self.details["itemtype"] = ITEMS[itemname]
        else:
            self.details["itemtype"] = "error"
            self.details["message"] = "item not found"

    @property
    def yaml(self):
        return yaml.dump(self.details)

##########

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        server = getattr(self, "server", None)
        if server:
            info = server.get_main()
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
            info = server.get_sub_status(itemname)
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

    def get_sub_status(self, itemname):
        return {"thing": "sub_status", "item": itemname}

    def get_status(self):
        return {"thing": "status"}

    def get_main(self):
        return {"thing": "main"}

    def _run(self, loop):
        # https://stackoverflow.com/a/48726076
        asyncio.set_event_loop(loop)
        self.app.listen(self.port, self.address)
        tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    server = Server("127.0.0.1", 8888, None)
    server.start()
    server.thread.join()
