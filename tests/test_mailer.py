#!/usr/bin/env python
"""
Tests for Mailer objects.

This uses a temporary SMTP server to receive and check "sent" messages.
"""

from .test_common import *
import pwd
import socket
import os
import smtpd
import asyncore
import threading
from umbra.mailer import Mailer


class StubSMTP(smtpd.SMTPServer):

    """Fake SMTP server to receive test messages."""

    @property
    def message_parsed(self):
        data = self.message.decode("UTF-8")
        return(self._parse(data))

    @property
    def message_pretty(self):
        return(self._prettify(self.message_parsed))

    def _parse(self, data):
        # locate double-newline between header and body of message
        i = data.find('\n\n')
        # split
        header = data[0:i]
        body = data[(i+2):len(data)]
        # unwrap
        header = re.sub("\n ", " ", header)
        # split keys/vals
        header = header.split("\n")
        header = [h.split(": ") for h in header]
        header = {h[0]: h[1] for h in header}
        if "multipart" in header.get("Content-Type", ""):
            m = re.search('boundary="(.*)"', header["Content-Type"])
            boundary = m.group(1)
            body = re.split("\n?--"+boundary+"(?:--)?\n?", body)
            body = [self._parse(b) for b in body if b]
        else:
            body = [body]
        return({"header": header, "body": body})

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
        return(output)

    def process_message(self, peer, mailfrom, rcpttos, data, **kwargs):
        self.message = data
        self.rcpttos = rcpttos


class TestMailer(unittest.TestCase):

    """ Test Mailer with a typical use case."""

    def setUp(self):
        self.setUpSMTP()
        self.mailer = Mailer(self.host, self.port)
        self.setUpVars()

    def setUpVars(self):
        self.kwargs = {
                "from_addr": "user1@example.com",
                "to_addrs": "user2@example.com",
                "subject": "Hi There",
                "msg_body": "A few words\nin a plaintext message",
                "msg_html": "<b><i>Ooooh HTML</i></b>"
                }
        self.expected = dict(self.kwargs)
        self.expected["to_addrs"] = [self.expected["to_addrs"]]

    def tearDown(self):
        # we need to explicitly clean up the socket or we'll get OSError:
        # [Errno 98] Address already in use
        asyncore.close_all()
        if hasattr(self, "smtpd"):
            self.smtpd.close()
        if hasattr(self, "thread"):
            self.thread.join()

    def setUpSMTP(self):
        self.host = "127.0.0.1"
        # select an arbitrary open port for the temporary SMTP server.
        self.port = 0
        self.smtpd = StubSMTP((self.host, self.port), (None, None))
        self.port = self.smtpd.socket.getsockname()[1]
        kwargs = {"timeout": 0}
        self.thread = threading.Thread(target=asyncore.loop,
                kwargs=kwargs,
                daemon=True)
        self.thread.start()

    def test_mail(self):
        self.mailer.mail(**self.kwargs)
        # Test recipients
        # This should be a list of the To addresses and CC addresses (if
        # present)
        exp = self.expected
        to = exp.get("to_addrs", [])
        cc = exp.get("cc_addrs", [])
        recipients = to + cc
        self.assertEqual(self.smtpd.rcpttos, recipients)
        # Test message attributes
        m = self.smtpd.message_parsed
        self.assertEqual(m["header"]["To"],      ", ".join(exp["to_addrs"]))
        self.assertEqual(m["header"]["Subject"], exp["subject"])
        self.assertEqual(m["header"]["From"],    exp["from_addr"])
        if cc:
            self.assertEqual(m["header"].get("CC"),  ", ".join(cc))
        if "msg_html" in self.kwargs:
            self.assertEqual(m["body"][0]["body"][0], exp["msg_body"])
            self.assertEqual(m["body"][1]["body"][0], exp["msg_html"])
        else:
            self.assertEqual(m["body"][0], exp["msg_body"])
            self.assertEqual(len(m["body"]), 1)


class TestMailerDefaultFrom(TestMailer):

    """Test Mailer without specifying the from_addr.
    
    In this case the local username and hostname will be used to construct a
    From address."""

    def setUp(self):
        super().setUp()
        del self.kwargs["from_addr"]
        user = pwd.getpwuid(os.getuid())[0]
        host = socket.getfqdn()
        self.expected["from_addr"] = "%s@%s" % (user, host)


class TestMailerNoHTML(TestMailer):

    """Test Mailer with a plaintext message only, no HTML.
    
    The message in this case should not be multipart."""

    def setUp(self):
        super().setUp()
        del self.kwargs["msg_html"]


class TestMailerMultipleRecipients(TestMailer):

    """ Test Mailer giving a list of to_addrs.
    
    If a list is given, it should show up as a single header entry in the
    received message."""

    def setUp(self):
        super().setUp()
        self.kwargs["to_addrs"] = ["user2@example.com", "user3@example.com"]
        self.expected["to_addrs"] = self.kwargs["to_addrs"]


class TestMailerCCAddrs(TestMailer):

    """ Test Mailer giving a single address for cc_addrs."""

    def setUp(self):
        self.setUpSMTP()
        cc = "admin@example.com"
        self.mailer = Mailer(self.host, self.port, cc_addrs=cc)
        self.setUpVars()
        self.expected["cc_addrs"] = [cc]


class TestMailerCCAddrsMulti(TestMailer):

    """ Test Mailer giving multiple addresses for cc_addrs."""

    def setUp(self):
        self.setUpSMTP()
        cc = ["admin@example.com", "office@example.com"]
        self.mailer = Mailer(self.host, self.port, cc_addrs=cc)
        self.setUpVars()
        self.expected["cc_addrs"] = cc

if __name__ == '__main__':
    unittest.main()
