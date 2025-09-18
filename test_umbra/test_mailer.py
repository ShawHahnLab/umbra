"""
Tests for Mailer objects.
"""

import pwd
import socket
import os
import smtplib
from email import message_from_string
from unittest.mock import patch
from umbra.mailer import Mailer
from .test_common import TestBase


class TestMailer(TestBase):
    """ Test Mailer with a typical use case."""

    def setUp(self):
        self.mailer = Mailer({"host": "127.0.0.1", "port": 0})
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

    def check_mail(self, mock_smtp):
        """Compare intercepted message to expected message."""
        # The mock equivalent of the `... as smtp` as used in Mailer
        smtp_mgr = mock_smtp.return_value.__enter__.return_value
        # Test recipients
        # This should be a list of the To addresses and CC addresses (if
        # present)
        exp = self.expected["mail_args"]
        to_addrs = exp.get("to_addrs", [])
        cc_addrs = exp.get("cc_addrs", [])
        msg = None
        # If it looks like no message was received, just return here.  But fail
        # if that was unexpected.
        try:
            smtp_mgr.sendmail.assert_called()
        except AssertionError as err:
            if self.expected["sent"]:
                raise err
            return msg
        recipients = to_addrs + cc_addrs
        obs_from_addr, obs_recipients, obs_msg = smtp_mgr.sendmail.call_args[0]
        self.assertEqual(obs_recipients, recipients)
        # Test message attributes
        msg = message_from_string(obs_msg)
        self.assertEqual(msg["Subject"], exp["subject"])
        self.assertEqual(msg["From"], exp["from_addr"])
        self.assertEqual(obs_from_addr, exp["from_addr"])
        if to_addrs:
            self.assertEqual(msg.get("To"), ", ".join(to_addrs))
        if cc_addrs:
            self.assertEqual(msg.get("CC"), ", ".join(cc_addrs))
        body = {item.get_content_type(): item.get_payload() for item in msg.walk()}
        if "msg_html" in self.mail_args_sent:
            self.assertEqual(body["text/plain"], exp["msg_body"])
            self.assertEqual(body["text/html"], exp["msg_html"])
        else:
            self.assertEqual(body["text/plain"], exp["msg_body"])
            self.assertEqual(len(body), 1)
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
            with patch("smtplib.SMTP", autospec=True) as mock_smtp:
                if self.expected["sent"]:
                    self.mailer.mail(**self.mail_args_sent)
                else:
                    # There should be a complaint in this case
                    with self.assertLogs(level="ERROR") as logging_context:
                        self.mailer.mail(**self.mail_args_sent)
                    self.assertEqual(len(logging_context.output), 1)
                message = self.check_mail(mock_smtp)
        except smtplib.SMTPException:
            failure = "SMTP Failure"
        if failure:
            self.fail(failure)


class TestMailerDefaultFrom(TestMailer):
    """Test Mailer without specifying the from_addr.

    In this case the local username and hostname will be used to construct a
    From address."""

    def setUp(self):
        super().setUp()
        del self.mail_args_sent["from_addr"]
        user = pwd.getpwuid(os.getuid())[0]
        host = socket.getfqdn()
        self.expected["mail_args"]["from_addr"] = f"{user}@{host}"


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
        cc_addrs = "admin@example.com"
        conf = {"host": "127.0.0.1", "port": 0, "cc_addrs": cc_addrs}
        self.mailer = Mailer(conf)
        self.set_up_vars()
        self.expected["mail_args"]["cc_addrs"] = [cc_addrs]


class TestMailerCCAddrsMulti(TestMailer):

    """ Test Mailer giving multiple addresses for cc_addrs."""

    def setUp(self):
        cc_addrs = ["admin@example.com", "office@example.com"]
        conf = {"host": "127.0.0.1", "port": 0, "cc_addrs": cc_addrs}
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
        cc_addrs = "admin@example.com"
        conf = {"host": "127.0.0.1", "port": 0, "cc_addrs": cc_addrs}
        self.mailer = Mailer(conf)
        self.set_up_vars()
        self.mail_args_sent["to_addrs"] = []
        self.expected["mail_args"]["to_addrs"] = []
        self.expected["mail_args"]["cc_addrs"] = [cc_addrs]

    def test_mail(self):
        # There should be a complaint about the lack of to_addrs
        with patch("smtplib.SMTP", autospec=True) as mock_smtp:
            with self.assertLogs(level="WARNING") as logging_context:
                self.mailer.mail(**self.mail_args_sent)
            self.assertEqual(len(logging_context.output), 1)
            self.check_mail(mock_smtp)


class TestMailerReplyTo(TestMailer):
    """ Test Mailer giving a Reply-To address."""

    def setUp(self):
        reply_to = "technician@example.com"
        conf = {"host": "127.0.0.1", "port": 0, "reply_to": reply_to}
        self.mailer = Mailer(conf)
        self.set_up_vars()
        self.expected["mail_args"]["reply_to"] = reply_to

    def check_mail(self, mock_smtp):
        message = super().check_mail(mock_smtp)
        self.assertEqual(
            message.get("Reply-To"),
            self.expected["mail_args"]["reply_to"])
