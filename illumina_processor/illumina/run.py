from .util import *
from .alignment import Alignment
import warnings

class Run:
    """A single Illumina sequencing run, based on a directory tree."""

    def __init__(self, path, strict=None, alignment_callback=None):
        # Setup run path
        path = Path(path).resolve()
        self.path = path
        self.alignment_callback = alignment_callback

        # RunInfo.xml is one of the first files to show up in a run directory,
        # so we'll use that to define a Run (finished or not).
        try:
            self.run_info = load_xml(path/"RunInfo.xml")
        except FileNotFoundError:
            msg = 'Not a recognized Illumina run: "%s"' % path
            # strict behavior: raise an error
            if strict or strict is None:
                raise(ValueError(msg))
            else:
                warnings.warn(msg)
                return
        info_run_id = self.run_info.find('Run').attrib["Id"]
        if info_run_id != path.name:
            msg = 'Run directory does not match Run ID: %s / %s' % (path.name, info_run_id)
            warnings.warn(msg)
            ## strict behavior: raise an error
            #if strict:
            #    raise(ValueError(msg))
            ## default behavior: raise a warning
            #elif strict is None:
            #    warnings.warn(msg)
            ## False behavior: ignore

        # Load in RTA completion status and available alignment directories.
        self.rta_complete = None
        self.alignments = []
        self.refresh()
        # CompletedJobInfo.xml should be there if a workflow (job) completed,
        # like GenerateFASTQ.  It looks like this file is just copied over at
        # the end of the most recent job from the Alignment sub-folder.
        try:
            self.completed_job_info = load_xml(path/"CompletedJobInfo.xml")
        except FileNotFoundError:
            self.completed_job_info = None

    @property
    def valid(self):
        return(hasattr(self, "run_info"))

    def refresh(self):
        """Check for run completion and any new or completed alignments.
        
        Aside from RTAComplete.txt and the Alignment directories, nothing else
        is checked.  If other files may have changed, instatiate a new Run
        object."""
        if not self.rta_complete:
            fp = self.path/"RTAComplete.txt"
            self.rta_complete = self._load_rta_complete(fp)
        self._refresh_alignments()

    def _refresh_alignments(self):
        # First refresh any existing alignments
        [al.refresh() for al in self.alignments]
        # Load from expected paths, using patterns for MiSeq and MiniSeq
        al_loc1 = self.path.glob("Data/Intensities/BaseCalls/Alignment*")
        al_loc2 = self.path.glob("Alignment*")
        al_loc = list(al_loc1) + list(al_loc2)
        # Filter out those already loaded and process new ones
        al_loc_known = [al.path for al in self.alignments]
        is_new = lambda d: not d in al_loc_known
        al_loc = [d for d in al_loc if is_new(d)]
        al = [self._alignment_setup(d) for d in al_loc]
        # Filter out any blanks that failed to load
        al = [a for a in al if a]
        # Merge new ones into existing list
        self.alignments += al

    def _alignment_setup(self, path):
        # Try loading an alignment directory, but just throw a warning and
        # return None if it doesn't look like an Alignment.  This should handle
        # not-yet-complete Alignment directories on disk.
        try:
            al = Alignment(path, self, self.alignment_callback)
        except ValueError as e:
            warnings.warn("Alignment not recognized: %s" % path)
            return(None)
        else:
            return(al)

    def _load_rta_complete(self, path):
        """Parse an RTAComplete.txt file.
        
        Creates a dictionary with the Date and Illumina Real-Time Analysis
        software version.  This file should exist if real-time analysis that
        does basecalling and generates BCL files has finished.
        """
        try:
            data = load_csv(path)[0]
        except FileNotFoundError:
            return(None)
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

    @property
    def run_id(self):
        return(self.run_info.find('Run').attrib["Id"])

    @property
    def complete(self):
        """Is the run complete?"""
        return(self.rta_complete is not None)
