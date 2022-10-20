"""
Custom logging with tracking of data processing context and handlers to send
messsages via email and/or wall commands.
"""

import logging
import subprocess


class UmbraLoggerAdapter(logging.LoggerAdapter):
    """A logger adapter to automatically add contextual information to log records.

    This uses LoggerAdapter's default implementation of the "process" method to
    use the "extra" argument in the logging calls, which in turn makes these
    extra key/value pairs show up as attributes of the log records.
    """

    def __init__(self, logger, extra=None):
        if extra is None:
            extra = {}
        super().__init__(logger, extra)
        self._parse(extra)

    def _parse(self, extra):
        # for each recognized object type, coerce it to text identifier, and
        # grab whatever filesystem paths or other identifiers are present.
        # Explicitly given objects override indirect ones, like kwargs["run"]
        # versus kwargs["aln"].run.
        self._parse_proj(extra)
        self._parse_aln(extra)
        self._parse_run(extra)

    def _parse_run(self, extra, obj=None):
        obj = extra.get("run") or obj
        if obj:
            try:
                extra["run"] = str(obj.run_id)
            except AttributeError:
                extra["run"] = str(obj)
            else:
                try:
                    extra["run_path"] = str(obj.path)
                except AttributeError:
                    pass

    def _parse_aln(self, extra, obj=None):
        obj = extra.get("aln") or obj
        if obj:
            try:
                extra["aln_path"] = str(obj.path)
            except AttributeError:
                extra["aln"] = str(obj)
            else:
                try:
                    extra["aln"] = str(obj.index)
                    extra["exp"] = str(obj.experiment)
                    self._parse_run(extra, obj.run)
                except AttributeError:
                    pass

    def _parse_proj(self, extra, obj=None):
        obj = extra.get("proj") or obj
        if obj:
            try:
                extra["proj"] = str(obj.work_dir)
            except AttributeError:
                extra["proj"] = str(obj)
            else:
                try:
                    self._parse_aln(extra, obj.alignment)
                except AttributeError:
                    pass


class WallLogHandler(logging.Handler):
    """Log handler for sending log messages via wall command."""

    def __init__(self, level=logging.CRITICAL):
        super().__init__(level)
        formatter = logging.Formatter(
            "Sequencing run processing error "
            "(level %(levelname)s in module %(module)s)\n%(message)s")
        self.setFormatter(formatter)

    def emit(self, record):
        msg = self.format(record)
        subprocess.run(["wall", msg], check=False)


class MailerLogHandler(logging.Handler):
    """Log handler for sending log messages via a Mailer object."""

    def __init__(self, mailerobj, contacts, level=logging.ERROR):
        super().__init__(level)
        formatter = logging.Formatter(
            "Sequencing run processing error "
            "(level %(levelname)s in module %(module)s)\n%(message)s")
        self.setFormatter(formatter)
        self.contacts = contacts
        self.mailerobj = mailerobj

    def emit(self, record):
        self.mailerobj.mail(
            to_addrs=self.contacts,
            subject="Sequencing run processing error",
            msg_body=self.format(record))
