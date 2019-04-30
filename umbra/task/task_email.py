"""Send notfication email for a finished ProjectData."""

from umbra import task

class TaskEmail(task.Task):
    """Send notfication email for a finished ProjectData."""

    order = 1003
    dependencies = ["upload"]

    def run(self):
        # Gather fields to fill in for the message
        # (The name prefix is considered OK by RFC822, so we should be able to
        # leave that intact for both the sending part and the "To:" field.)
        contacts = self.proj.contacts
        contacts = ["%s <%s>" % (k, contacts[k]) for k in contacts]
        # Build subject, message text, and message body html
        subject = self.config["template_subject"].format(self=self)
        body = self.config["template_text"].format(self=self)
        html = self.config["template_html"].format(self=self)
        # Send
        kwargs = {
            "to_addrs": contacts,
            "subject": subject,
            "msg_body": body,
            "msg_html": html
            }
        self.proj.mailer(**kwargs)
        return kwargs

    @property
    def url(self):
        """URL reported by upload task."""
        upload_output = self.proj.task_output.get("upload", {})
        url = upload_output.get("url", "")
        return url
