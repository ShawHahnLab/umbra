"""Upload zipfile to Box."""

from umbra import task
class TaskUpload(task.Task):
    """Upload zipfile to Box."""

    order = 1002
    dependencies = ["package"]

    def run(self):
        # TODO reorganize uploader
        url = self.proj.uploader(path=self.proj.path_pack)
        return {"url": url}
