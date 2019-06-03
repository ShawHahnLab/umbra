"""
A simple interface for sending email.

See the Mailer class for usage.
"""

import ssl
import pwd
import socket
import smtplib
import os
import logging
import copy
from email.message import EmailMessage

# https://stackoverflow.com/a/2899055
def get_username():
    """Get the current OS username."""
    entry = pwd.getpwuid(os.getuid())
    username = entry[0]
    return username

LOGGER = logging.getLogger(__name__)

class Mailer:
    """A simple interface for sending email.

    With no arguments it will set up an SMTP connection to localhost on port
    25, and will send messages as user@fully.qualified.domain.name of
    localhost.  Connection details, SSL, auth, and the apparent From address
    can be customized."""

    # pylint: disable=too-few-public-methods
    # (What's wrong with just having one public method?  A Mailer instance
    # mails.  That's it.)

    def __init__(self, conf):
        """Configure connection details for sending mail over SMTP."""
        self.conf = copy.deepcopy(conf)
        if isinstance(self.conf.get("cc_addrs"), str):
            self.conf["cc_addrs"] = [self.conf.get("cc_addrs")]
        if not self.conf.get("cc_addrs"):
            self.conf["cc_addrs"] = []
        LOGGER.debug("Mailer initialized.")

    def _resolve_from_addr(self, addr, addr_default):
        addr = addr or addr_default
        if not addr:
            name = self.conf.get("user") or get_username()
            if "@" in name:
                addr = name
            else:
                srv = self.conf.get("host")
                if socket.getfqdn(srv) == "localhost":
                    srv = socket.getfqdn()
                addr = name + "@" + srv
        return addr

    def mail(self, to_addrs, subject, msg_body, msg_html=None, from_addr=None,
             reply_to=None):
        """Send a message.

        This will connect to the SMTP server (with authentication if enabled
        for the object), format and send a single message, and disconnect."""
        # pylint: disable=too-many-arguments
        LOGGER.debug('Preparing message: "%s"', subject)
        if isinstance(to_addrs, str):
            to_addrs = [to_addrs]
        # From address can be set already or given here.  Either way, if it was
        # not defined, construct a From address from user and network details.
        from_addr = self._resolve_from_addr(from_addr, self.conf.get("from_addr"))
        reply_to = reply_to or self.conf.get("reply_to")
        # Construct message
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_addr
        if reply_to:
            msg["Reply-To"] = reply_to
        if to_addrs:
            msg["To"] = ", ".join(to_addrs)
        LOGGER.debug('Connecting over SMTP for message: "%s"', subject)
        if self.conf.get("cc_addrs"):
            msg["CC"] = ", ".join(self.conf.get("cc_addrs"))
        if msg_html:
            msg.set_type("multipart/alternative")
            msg1 = EmailMessage()
            msg2 = EmailMessage()
            msg1.set_type('text/plain')
            msg2.set_type('text/html')
            msg1.set_payload(msg_body)
            msg2.set_payload(msg_html)
            msg.set_payload([msg1, msg2])
        else:
            msg.set_payload(msg_body)
        recipients = to_addrs + self.conf.get("cc_addrs")
        # If there are no receipients, don't actually try to send.  If there
        # are recipients but no "To:" addresses, just warn.
        if not recipients:
            LOGGER.error(
                'No receipients given, skipping message: "%s"', subject)
            return msg
        if not to_addrs:
            LOGGER.warning(
                'No "To:" addresses given for message: "%s"', subject)
        LOGGER.debug('Connecting over SMTP for message: "%s"', subject)
        with smtplib.SMTP(self.conf.get("host"), port=self.conf.get("port")) as smtp:
            if self.conf.get("ssl"):
                context = ssl.create_default_context()
                smtp.starttls(context=context)
            if self.conf.get("auth"):
                smtp.login(self.conf.get("user"), self.conf.get("password"))
            # Send and quit
            LOGGER.debug('Sending message: "%s"', subject)
            # "Note: The from_addr and to_addrs parameters are used to construct
            # the message envelope used by the transport agents. sendmail does
            # not modify the message headers in any way."
            # In other words, we need to have the same information present
            # inside the msg object, even though we're also giving it
            # explicitly to smtp.sendmail.
            smtp.sendmail(from_addr, recipients, msg.as_string())
        LOGGER.info('Message sent: "%s"', subject)
        return msg
