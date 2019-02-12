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
from email.message import EmailMessage

# https://stackoverflow.com/a/2899055
def get_username():
    """Get the current OS username."""
    entry = pwd.getpwuid(os.getuid())
    username = entry[0]
    return username

class Mailer:
    """A simple interface for sending email.

    With no arguments it will set up an SMTP connection to localhost on port
    25, and will send messages as user@fully.qualified.domain.name of
    localhost.  Connection details, SSL, auth, and the apparent From address
    can be customized."""

    # pylint: disable=too-few-public-methods
    # (What's wrong with just having one public method?  A Mailer instance
    # mails.  That's it.)

    def __init__(
            self, host="localhost", port=25,
            use_ssl=False, auth=False, user=None, password=None, from_addr=None,
            cc_addrs=None, reply_to=None, **kwargs):
        """Configure connection details for sending mail over SMTP."""
        self.logger = logging.getLogger(__name__)
        self.host = host
        self.port = port
        self.ssl = use_ssl
        self.auth = auth
        self.__user = user
        self.__password = password
        self.from_addr = from_addr
        self.reply_to = reply_to
        self.kwargs = kwargs
        if isinstance(cc_addrs, str):
            cc_addrs = [cc_addrs]
        if not cc_addrs:
            cc_addrs = []
        self.cc_addrs = cc_addrs
        self.logger.debug("Mailer initialized.")

    def _resolve_from_addr(self, addr, addr_default):
        addr = addr or addr_default
        if not addr:
            name = self.__user or get_username()
            if "@" in name:
                addr = name
            else:
                srv = self.host
                if socket.getfqdn(srv) == "localhost":
                    srv = socket.getfqdn()
                addr = name + "@" + srv
        return addr

    def mail(self, to_addrs, subject, msg_body, msg_html=None, from_addr=None,
             reply_to=None):
        """Send a message.

        This will connect to the SMTP server (with authentication if enabled
        for the object), format and send a single message, and disconnect."""
        self.logger.debug('Preparing message: "%s"', subject)
        if isinstance(to_addrs, str):
            to_addrs = [to_addrs]
        # From address can be set already or given here.  Either way, if it was
        # not defined, construct a From address from user and network details.
        from_addr = self._resolve_from_addr(from_addr, self.from_addr)
        reply_to = reply_to or self.reply_to
        # Construct message
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_addr
        if reply_to:
            msg["Reply-To"] = reply_to
        if to_addrs:
            msg["To"] = ", ".join(to_addrs)
        self.logger.debug('Connecting over SMTP for message: "%s"', subject)
        if self.cc_addrs:
            msg["CC"] = ", ".join(self.cc_addrs)
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
        recipients = to_addrs + self.cc_addrs
        # If there are no receipients, don't actually try to send.  If there
        # are recipients but no "To:" addresses, just warn.
        if not recipients:
            self.logger.error(
                'No receipients given, skipping message: "%s"', subject)
            return msg
        if not to_addrs:
            self.logger.warning(
                'No "To:" addresses given for message: "%s"', subject)
        self.logger.debug('Connecting over SMTP for message: "%s"', subject)
        with smtplib.SMTP(self.host, port=self.port) as smtp:
            if self.ssl:
                context = ssl.create_default_context()
                smtp.starttls(context=context)
            if self.auth:
                smtp.login(self.__user, self.__password)
            # Send and quit
            self.logger.debug('Sending message: "%s"', subject)
            # "Note: The from_addr and to_addrs parameters are used to construct
            # the message envelope used by the transport agents. sendmail does
            # not modify the message headers in any way."
            # In other words, we need to have the same information present
            # inside the msg object, even though we're also giving it
            # explicitly to smtp.sendmail.
            smtp.sendmail(from_addr, recipients, msg.as_string())
        self.logger.info('Message sent: "%s"', subject)
        return msg
