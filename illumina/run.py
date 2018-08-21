from .util import *
from .alignment import Alignment
import warnings

class Run:
    """A single Illumina sequencing run, based on a directory tree."""

    def load_rta_complete(self, path):
        """Parse an RTAComplete.txt file.
        
        Creates a dictionary with the Date and Illumina Real-Time Analysis
        software version.
        """
        data = load_csv(path)[0]
        date_pad = lambda txt: "/".join([x.zfill(2) for x in txt.split("/")])
        time_pad = lambda txt: ":".join([x.zfill(2) for x in txt.split(":")])
        # MiniSeq (RTA 2x?)
        # RTA 2.8.6 completed on 3/17/2017 8:19:33 AM
        if len(data) == 1:
            m = re.match("(RTA [0-9.]+) completed on ([^ ]+) (.+)", data[0])
            version = m.group(1)
            date_str_date = date_pad(m.group(2))
            date_str_time = time_pad(m.group(3))
            date_str = date_str_date + " " + date_str_time
            fmt = '%m/%d/%Y %I:%M:%S %p'
            date_obj = datetime.datetime.strptime(date_str, fmt)
        # MiSeq (RTA 1x?)
        # 11/2/2017,03:08:24.972,Illumina RTA 1.18.54 
        else:
            date_str_date = date_pad(data[0])
            date_str = date_str_date + " " + data[1] 
            fmt = '%m/%d/%Y %H:%M:%S.%f'
            date_obj = datetime.datetime.strptime(date_str, fmt)
            version = data[2]
        return({"Date": date_obj, "Version": version})

    def __init__(self, path):
        # Setup run path
        path = Path(path).resolve()
        self.path = path
        # Top-level metadata

        # RunInfo.xml is one of the first files to show up in a run directory,
        # so we'll use that to define a Run (finished or not).
        try:
            self.run_info = load_xml(path/"RunInfo.xml")
        except FileNotFoundError:
            raise(ValueError('Not a recognized Illumina run: "%s"' % path))
        info_run_id = self.run_info.find('Run').attrib["Id"]
        if info_run_id != path.name:
            warnings.warn('Run directory does not match Run ID: %s / %s' %
                    (path.name, info_run_id))

        # These ones might or might not exist depending on the run status.
        # RTAComplete.txt should be there if real-time analysis that does
        # basecalling and generates BCL files has finished.
        try:
            self.rta_complete = self.load_rta_complete(path/"RTAComplete.txt")
        except FileNotFoundError:
            self.rta_complete = None
        # CompletedJobInfo.xml should be there if a workflow (job) completed,
        # like GenerateFASTQ.  It looks like this file is just copied over at
        # the end of the most recent job from the Alignment sub-folder.
        try:
            self.completed_job_info = load_xml(path/"CompletedJobInfo.xml")
        except FileNotFoundError:
            self.completed_job_info = None
        # Alignment paths, using patterns for MiSeq and MiniSeq
        al_loc1 = path.glob("Data/Intensities/BaseCalls/Alignment*")
        al_loc2 = path.glob("Alignment*")
        al = [Alignment(p, self) for p in al_loc1]
        al = al + [Alignment(p, self) for p in al_loc2]
        self.alignments = al

    @property
    def run_id(self):
        return(self.run_info.find('Run').attrib["Id"])

