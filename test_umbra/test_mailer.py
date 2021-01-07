"""
Tests for Mailer objects.

This uses a temporary SMTP server to receive and check "sent" messages.
"""

import pwd
import socket
import os
import re
import smtpd
import smtplib
import asyncore
import threading
from umbra.mailer import Mailer
from .test_common import TestBase


class StubSMTP(smtpd.SMTPServer):
    """Fake SMTP server to receive test messages."""

    @property
    def message_parsed(self):
        """Received message parsed into a dict."""
        data = self.message.decode("UTF-8")
        return StubSMTP.parse(data)

    @property
    def message_pretty(self):
        """Received message pretty-printed as a string."""
        return self._prettify(self.message_parsed)

    @staticmethod
    def parse(data):
        """Parse a message section from a string into a dict.

        This calls itself recursively on multipart meessages, creating a nested
        dictionary structure corresponding to the message structure.
        """
        # locate double-newline between header and body of message
        i = data.find('\n\n')
        # split
        header = data[0:i]
        body = data[(i+2):len(data)]
        header = StubSMTP.parse_header(header)
        if "multipart" in header.get("Content-Type", ""):
            msg = re.search('boundary="(.*)"', header["Content-Type"])
            boundary = msg.group(1)
            body = re.split("\n?--"+boundary+"(?:--)?\n?", body)
            body = [StubSMTP.parse(b) for b in body if b]
        else:
            body = [body]
        return {"header": header, "body": body}

    @staticmethod
    def parse_header(header):
        """Parse the header from message text into a simple dictionary."""
        # unwrap
        header = re.sub("\n ", " ", header)
        # split keys/vals
        header = header.split("\n")
        # This is a little roundabout to handle empty fields.
        header = [h.split(":") for h in header]
        header = [[h[0], h[1].lstrip()] for h in header]
        header = {h[0]: h[1] for h in header}
        return header

    def _prettify(self, data=None, indent=""):
        output = ""
        if not data:
            data = self.message_parsed
        try:
            for key in data["header"]:
                output += "%s%s: %s\n" % (indent, key, data["header"][key])
            for chunk in data["body"]:
                output += self._prettify(chunk, indent + "  ")
        except TypeError:
            data = "\n".join([indent + c for c in data.split("\n")])
            output += data + "\n"
        return output

    def process_message(self, peer, mailfrom, rcpttos, data, **kwargs):
        # pylint: disable=attribute-defined-outside-init
        self.message = data
        self.rcpttos = rcpttos


class TestMailer(TestBase):
    """ Test Mailer with a typical use case."""

    def setUp(self):
        self.set_up_smtp()
        conf = {"host": self.host, "port": self.port}
        self.mailer = Mailer(conf)
        self.set_up_vars()

    def set_up_vars(self):
        """Initialize used and expected values for testing."""
        # These are the arguments given to the mailer.
        self.mail_args_sent = {
            "from_addr": "user1@example.com",
            "to_addrs": "user2@example.com",
            "subject": "Hi There",
            "msg_body": "A few words\nin a plaintext message",
            "msg_html": "<b><i>Ooooh HTML</i></b>"
            }
        # These are the values to test against.
        self.expected = {
            # A dict of attributes for the message received.  Basically should
            # match that sent.
            "mail_args": dict(self.mail_args_sent),
            # The message was actually sent, right?  In certain situations we
            # can toggle this off.
            "sent": True
            }
        # Modify expected received dict to always use a list for to_addrs.
        exp_args = self.expected["mail_args"]
        exp_args["to_addrs"] = [exp_args["to_addrs"]]

    def tearDown(self):
        # we need to explicitly clean up the socket or we'll get OSError:
        # [Errno 98] Address already in use
        asyncore.close_all()
        if hasattr(self, "smtpd"):
            self.smtpd.close()
        if hasattr(self, "thread"):
            self.thread.join()

    def set_up_smtp(self):
        """Initialize fake SMTP server to receive messages."""
        self.host = "127.0.0.1"
        # select an arbitrary open port for the temporary SMTP server.
        self.port = 0
        self.smtpd = StubSMTP((self.host, self.port), (None, None))
        self.port = self.smtpd.socket.getsockname()[1]
        kwargs = {"timeout": 0}
        self.thread = threading.Thread(
            target=asyncore.loop,
            kwargs=kwargs,
            daemon=True)
        self.thread.start()

    def check_mail(self):
        """Compare SMTPD-received message to expected message."""
        # Test recipients
        # This should be a list of the To addresses and CC addresses (if
        # present)
        exp = self.expected["mail_args"]
        to_addrs = exp.get("to_addrs", [])
        cc_addrs = exp.get("cc_addrs", [])
        msg = None
        # If it looks like no message was received, just return here.  But fail
        # if that was unexpected.
        if not hasattr(self.smtpd, "rcpttos"):
            if self.expected["sent"]:
                self.fail("SMTPD did not receive a message")
            return msg
        recipients = to_addrs + cc_addrs
        self.assertEqual(self.smtpd.rcpttos, recipients)
        # Test message attributes
        msg = self.smtpd.message_parsed
        self.assertEqual(msg["header"]["Subject"], exp["subject"])
        self.assertEqual(msg["header"]["From"], exp["from_addr"])
        if to_addrs:
            self.assertEqual(msg["header"].get("To"), ", ".join(to_addrs))
        if cc_addrs:
            self.assertEqual(msg["header"].get("CC"), ", ".join(cc_addrs))
        if "msg_html" in self.mail_args_sent:
            self.assertEqual(msg["body"][0]["body"][0], exp["msg_body"])
            self.assertEqual(msg["body"][1]["body"][0], exp["msg_html"])
        else:
            self.assertEqual(msg["body"][0], exp["msg_body"])
            self.assertEqual(len(msg["body"]), 1)
        return msg

    def test_mail(self):
        """Test sending a message.

        If self.expected["sent"] is True, the test will expect that a message
        was successfully sent.  Otherwise an error is expected to have been
        logged.
        """
        failure = None
        message = None
        try:
            if self.expected["sent"]:
                self.mailer.mail(**self.mail_args_sent)
            else:
                # There should be a complaint in this case
                with self.assertLogs(level="ERROR") as logging_context:
                    self.mailer.mail(**self.mail_args_sent)
                self.assertEqual(len(logging_context.output), 1)
            message = self.check_mail()
        except smtplib.SMTPException:
            failure = "SMTP Failure"
        if failure:
            self.fail(failure)
        return message


class TestMailerDefaultFrom(TestMailer):
    """Test Mailer without specifying the from_addr.

    In this case the local username and hostname will be used to construct a
    From address."""

    def setUp(self):
        super().setUp()
        del self.mail_args_sent["from_addr"]
        user = pwd.getpwuid(os.getuid())[0]
        host = socket.getfqdn()
        self.expected["mail_args"]["from_addr"] = "%s@%s" % (user, host)


class TestMailerNoHTML(TestMailer):
    """Test Mailer with a plaintext message only, no HTML.

    The message in this case should not be multipart."""

    def setUp(self):
        super().setUp()
        del self.mail_args_sent["msg_html"]


class TestMailerMultipleRecipients(TestMailer):
    """ Test Mailer giving a list of to_addrs.

    If a list is given, it should show up as a single header entry in the
    received message."""

    def setUp(self):
        super().setUp()
        self.mail_args_sent["to_addrs"] = ["user2@example.com", "user3@example.com"]
        self.expected["mail_args"]["to_addrs"] = self.mail_args_sent["to_addrs"]


class TestMailerCCAddrs(TestMailer):
    """ Test Mailer giving a single address for cc_addrs."""

    def setUp(self):
        self.set_up_smtp()
        cc_addrs = "admin@example.com"
        conf = {"host": self.host, "port": self.port, "cc_addrs": cc_addrs}
        self.mailer = Mailer(conf)
        self.set_up_vars()
        self.expected["mail_args"]["cc_addrs"] = [cc_addrs]


class TestMailerCCAddrsMulti(TestMailer):

    """ Test Mailer giving multiple addresses for cc_addrs."""

    def setUp(self):
        self.set_up_smtp()
        cc_addrs = ["admin@example.com", "office@example.com"]
        conf = {"host": self.host, "port": self.port, "cc_addrs": cc_addrs}
        self.mailer = Mailer(conf)
        self.set_up_vars()
        self.expected["mail_args"]["cc_addrs"] = cc_addrs


class TestMailerNoTo(TestMailer):
    """ Test Mailer giving empty to_addrs.

    In this case there are no recipients at all, and the message is created but
    not sent.  An error should be logged."""

    def setUp(self):
        super().setUp()
        self.mail_args_sent["to_addrs"] = []
        self.expected["mail_args"]["to_addrs"] = []
        self.expected["sent"] = False


class TestMailerOnlyCC(TestMailer):
    """ Test Mailer giving empty to_addrs but with cc_addrs.

    In this case there are still technically recipients but only CC addresses.
    This could come up if the mailer is configured for CC but a particular
    message has no specific recipients.  The message should be sent without a
    "To:" field and with a warning logged."""

    def setUp(self):
        self.set_up_smtp()
        cc_addrs = "admin@example.com"
        conf = {"host": self.host, "port": self.port, "cc_addrs": cc_addrs}
        self.mailer = Mailer(conf)
        self.set_up_vars()
        self.mail_args_sent["to_addrs"] = []
        self.expected["mail_args"]["to_addrs"] = []
        self.expected["mail_args"]["cc_addrs"] = [cc_addrs]

    def test_mail(self):
        # There should be a complaint about the lack of to_addrs
        with self.assertLogs(level="WARNING") as logging_context:
            self.mailer.mail(**self.mail_args_sent)
        self.assertEqual(len(logging_context.output), 1)
        self.check_mail()


class TestMailerReplyTo(TestMailer):
    """ Test Mailer giving a Reply-To address."""

    def setUp(self):
        self.set_up_smtp()
        reply_to = "technician@example.com"
        conf = {"host": self.host, "port": self.port, "reply_to": reply_to}
        self.mailer = Mailer(conf)
        self.set_up_vars()
        self.expected["mail_args"]["reply_to"] = reply_to

    def test_mail(self):
        message = super().test_mail()
        self.assertEqual(
            message["header"].get("Reply-To"),
            self.expected["mail_args"]["reply_to"])
