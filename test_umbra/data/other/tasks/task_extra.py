"Example of an extra Task."

from umbra import task

class TaskExtra(task.Task):
    "Only override the run method, all else is defaults."

    def run(self):
        "Do nothing."
