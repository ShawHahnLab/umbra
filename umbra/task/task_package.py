"""Create zipfile of processing directory and metadata."""

from pathlib import Path
import zipfile
import os
from umbra import task
from umbra.util import mkparent
class TaskPackage(task.Task):
    """Create zipfile of processing directory and metadata."""

    order = 1001
    dependencies = ["metadata"]

    def run(self):
        # TODO reorganize path_pack
        mkparent(self.proj.path_pack)
        # By default ZipFile will not actually compress!  We need to specify a
        # compression method explicitly for that.
        with zipfile.ZipFile(self.proj.path_pack, "x", zipfile.ZIP_DEFLATED) as zipper:
            # Archive everything in the processing directory
            for root, dummy, files in os.walk(self.proj.path_proc):
                for fname in files:
                    # Archive the file but trim the name so it's relative to
                    # the processing directory.
                    filename = os.path.join(root, fname)
                    arcname = Path(filename).relative_to(self.proj.path_proc.parent)
                    zipper.write(filename, arcname)
