"""
Test umbra.logging

We have some special context-handling (to note ProjectData, Run, etc.) and
special log record handlers (to email and/or wall messages).
"""

import logging
from unittest.mock import Mock
import umbra.logging
from umbra.logging import UmbraLoggerAdapter, MailerLogHandler, WallLogHandler
from umbra.project import ProjectData
from umbra.mailer import Mailer
from umbra.illumina.run import Run
from umbra.illumina.alignment import Alignment
from .test_common import TestBase


class TestUmbraLoggerAdapter(TestBase):
    """Test the Umbra-specific loggger adapter.

    This should optionally add context-specific information to log records,
    otherwise passing through logging calls to the standard logging functions.
    """

    def setUp(self):
        self.aln = Mock(Alignment)
        self.aln.index = 0
        self.aln.path = "ALNPATH"
        self.aln.experiment = "EXPERIMENT"
        self.run = Mock(Run)
        self.run.run_id = "RUNID"
        self.run.path = "RUNPATH"
        self.run.alignments = [self.aln]
        self.aln.run = self.run
        self.proj = Mock(ProjectData)
        self.proj.work_dir = "workdir"
        self.proj.alignment = self.aln
        # an unrelated run
        self.run2 = Mock(Run)
        self.run2.run_id = "RUNID2"
        self.run2.path = "RUNPATH2"

    def test_basic(self):
        """Test that the logger adapter does all the basic logger stuff."""
        logger = Mock(logging.Logger)
        adapter = UmbraLoggerAdapter(logger)
        for lvl in ["debug", "info", "warning", "error", "critical"]:
            with self.subTest(level=lvl):
                # via log() method
                lvlnum = getattr(logging, lvl.upper())
                adapter.log(lvlnum, f"log message {lvl}")
                logger.log.assert_called_with(lvlnum, f"log message {lvl}", extra={})
                logger.reset_mock()
                # via log level name method
                getattr(adapter, lvl)(("log message"))
                # huh, apparently it doesn't actualy call the per-level methods
                # under the hood; it just funnels it all to log().  I guess
                # that makes sense.
                logger.log.assert_called_with(lvlnum, "log message", extra={})
                logger.reset_mock()

    def test_context_unrelated(self):
        """Test that unknown context values are passed through."""
        logger = Mock(logging.Logger)
        extra = {"key": "val", "key2": 5}
        adapter = UmbraLoggerAdapter(logger, extra=extra)
        adapter.info("log message")
        logger.log.assert_called_with(logging.INFO, "log message", extra=extra)

    def test_context_run(self):
        """Test that the adapter can handle a Run context."""
        logger = Mock(logging.Logger)
        # as string
        extra_in = {"run": "RUNDIR"}
        extra_out = {"run": "RUNDIR"}
        adapter = UmbraLoggerAdapter(logger, extra=extra_in)
        adapter.info("log message")
        logger.log.assert_called_with(logging.INFO, "log message", extra=extra_out)
        logger.reset_mock()
        # as object
        extra_in = {"run": self.run}
        extra_out = {
            "run": "RUNID",
            "run_path": "RUNPATH"}
        adapter = UmbraLoggerAdapter(logger, extra=extra_in)
        adapter.info("log message")
        logger.log.assert_called_with(logging.INFO, "log message", extra=extra_out)

    def test_context_project(self):
        """Test that the adapter can handle a ProjectData context.

        This should automatically fill in details about the associated
        project, alignment, and run.
        """
        logger = Mock(logging.Logger)
        # as string
        extra_in = {"proj": "workdir"}
        extra_out = {"proj": "workdir"}
        adapter = UmbraLoggerAdapter(logger, extra=extra_in)
        adapter.info("log message")
        logger.log.assert_called_with(logging.INFO, "log message", extra=extra_out)
        logger.reset_mock()
        # as object
        extra_in = {"proj": self.proj}
        extra_out = {
            "proj": "workdir",
            "aln": "0",
            "aln_path": "ALNPATH",
            "exp": "EXPERIMENT",
            "run": "RUNID",
            "run_path": "RUNPATH"}
        adapter = UmbraLoggerAdapter(logger, extra=extra_in)
        adapter.info("log message")
        logger.log.assert_called_with(logging.INFO, "log message", extra=extra_out)
        logger.reset_mock()
        # with other objects included
        extra_in = {"proj": self.proj, "run": self.run2}
        extra_out = {
            "proj": "workdir",
            "aln": "0",
            "aln_path": "ALNPATH",
            "exp": "EXPERIMENT",
            "run": "RUNID2",
            "run_path": "RUNPATH2"}
        adapter = UmbraLoggerAdapter(logger, extra=extra_in)
        adapter.info("log message")
        logger.log.assert_called_with(logging.INFO, "log message", extra=extra_out)
        logger.reset_mock()


class TestLogHandler(TestBase):
    """Supporting logic for the Test*LogHandler classes below."""

    def setUp(self):
        self.set_up_logger()

    def set_up_logger(self):
        """Set up (or reset) a logger for this class."""
        # That I need to jump through these hoops is probably a sign that I'm
        # doing it wrong and should really isolate these tests further (more
        # Mocks?) rather than using real Logger objects.
        #
        # https://github.com/python/cpython/issues/78380
        #
        # But, Mocks won't magically behave like the real thing and this will,
        # so, this isn't so bad.
        logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            logger.removeHandler(handler)
        self.logger = logger


class TestMailerLogHandler(TestLogHandler):
    """Test the email-sending log record handler."""

    def test_handle(self):
        """Test that log records at or above ERROR are handled."""
        # We'll use a real logger but a mock Mailer object
        # no adapter in this case
        mailer = Mock(Mailer)
        handler = MailerLogHandler(mailer, ["admin@example.com"])
        self.logger.addHandler(handler)
        # lower-level records shouldn't be handled
        self.logger.debug("debug message")
        mailer.mail.assert_not_called()
        # error and above should be, though
        self.logger.error("error message")
        mailer.mail.assert_called_with(
            to_addrs=["admin@example.com"],
            subject="Sequencing run processing error",
            msg_body="Sequencing run processing error "
                "(level ERROR in module test_logging)\nerror message")

    def test_handle_custom_level(self):
        """Test that log records are handled, specifying the level."""
        mailer = Mock(Mailer)
        handler = MailerLogHandler(mailer, ["admin@example.com"], level=logging.DEBUG)
        self.logger.addHandler(handler)
        mailer.reset_mock()
        mailer.mail.assert_not_called()
        self.logger.debug("debug message")
        mailer.mail.assert_called()

    def test_handle_with_adapter(self):
        """Test that the handler can use adapter-provided information"""
        mailer = Mock(Mailer)
        handler = MailerLogHandler(mailer, ["admin@example.com"])
        self.logger.addHandler(handler)
        adapter = UmbraLoggerAdapter(self.logger, extra={"proj": "WORKDIR"})
        adapter.debug("debug message")
        mailer.mail.assert_not_called()
        adapter.error("error message")
        self.skipTest("not yet implemented")


class TestWallLogHandler(TestLogHandler):
    """Test the message-to-all-server-users log record handler."""

    def setUp(self):
        super().setUp()
        self.set_up_run_mock()

    def tearDown(self):
        self.tear_down_run_mock()

    def set_up_run_mock(self):
        """Mock the run() used to call the wall cmd for log handling"""
        self.runmock = Mock(umbra.logging.subprocess.run)
        self.runmock.orig = umbra.logging.subprocess.run
        umbra.logging.subprocess.run = self.runmock

    def tear_down_run_mock(self):
        """Put the real subprocess.run back in place for umbra.logging"""
        umbra.logging.subprocess.run = self.runmock.orig

    def test_handle(self):
        """Test that log records are handled."""
        # lower-level records shouldn't be handled
        handler = WallLogHandler()
        self.logger.addHandler(handler)
        self.logger.debug("debug message")
        self.runmock.assert_not_called()
        # critical should be, though
        self.logger.critical("error message")
        msg = "Sequencing run processing error " \
            "(level CRITICAL in module test_logging)\nerror message"
        self.runmock.assert_called_with(["wall", msg], check=False)

    def test_handle_custom_level(self):
        """Test that log records are handled, specifying the level."""
        handler = WallLogHandler(level=logging.DEBUG)
        self.logger.addHandler(handler)
        self.logger.debug("debug message")
        self.runmock.assert_called()

    def test_handle_with_adapter(self):
        """Test that the handler can use adapter-provided information"""
        handler = WallLogHandler()
        self.logger.addHandler(handler)
        adapter = UmbraLoggerAdapter(self.logger, extra={"proj": "WORKDIR"})
        adapter.debug("debug message")
        self.runmock.assert_not_called()
        adapter.critical("error message")
        self.skipTest("not yet implemented")
