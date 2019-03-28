"""Trim adapters from raw fastq.gz files."""

import subprocess
from pathlib import Path
from umbra.illumina.util import ADAPTERS
from umbra.util import ProjectError
from umbra import task

class TaskTrim(task.Task):
    """Trim adapters from raw fastq.gz files."""

    order = 10

    def run(self):
        # For each sample, separately process the one or more associated files.
        with open(self.log_path, "w") as fout:
            for samp in self.sample_paths.keys():
                paths = self.sample_paths[samp]
                if len(paths) > 2:
                    raise ProjectError("trimming can't handle >2 files per sample")
                for i, path in enumerate(paths):
                    adapter = ADAPTERS["Nextera"][i]
                    fastq_in = str(path)
                    fastq_out = self.task_path(
                        readfile=path,
                        taskname=self.name,
                        subdir="trimmed",
                        suffix=".trimmed.fastq",
                        r1only=False)
                    args = ["cutadapt", "-a", adapter, "-o", fastq_out, fastq_in]
                    # Call cutadapt with each file.  If the exit status is
                    # nonzero or if the expected output file is missing, raise
                    # an exception.
                    # TODO make helper method for this pattern, used also in task_assemble
                    subprocess.run(args, stdout=fout, stderr=fout, check=True)
                    if not Path(fastq_out).exists():
                        msg = "missing output file %s" % fastq_out
                        raise ProjectError(msg)
