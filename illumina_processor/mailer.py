import ssl
import os
import pwd
import collections.abc
import socket
import smtplib
from email.message import EmailMessage

# https://stackoverflow.com/a/2899055
def get_username():
        entry = pwd.getpwuid(os.getuid())
        username = entry[0] 
        return(username)

class Mailer:
    """A simple interface for sending email.
    
    With no arguments it will set up an SMTP connection to localhost on port
    25, and will send messages as user@fully.qualified.domain.name of
    localhost.  Connection details, SSL, auth, and the apparent From address
    can be customized."""
    
    def __init__(self, host="localhost", port=25,
            ssl=False, auth=False, user=None, password=None):
        """Configure connection details for sending mail over SMTP."""
        self.host = host
        self.port = port
        self.ssl = ssl
        self.auth = auth
        self.__user = user
        self.__password = password

    def mail(self, to_addrs, subject, msg_body, msg_html=None, from_addr=None):
        """Send a message.
        
        This will connect to the SMTP server (with authentication if enabled
        for the object), format and send a single message, and disconnect."""
        if isinstance(to_addrs, str):
            to_addrs = [to_addrs]
        if not from_addr:
            name = self.__user or get_username()
            if "@" in name:
                from_addr = name
            else:
                srv = self.host
                if socket.getfqdn(srv) == "localhost":
                    srv = socket.getfqdn()
                from_addr = name + "@" + srv
        # Connect
        smtp = smtplib.SMTP(self.host, port=self.port)
        if self.ssl:
            context = ssl.create_default_context()
            smtp.starttls(context=context)
        if self.auth:
            smtp.login(self.__user, self.__password)
        # Construct message
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_addrs)
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
        # Send and quit
        smtp.sendmail(from_addr, to_addrs, msg.as_string())
        smtp.quit()
