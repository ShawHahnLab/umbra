"""Send notfication email for a finished ProjectData."""

from umbra import task
class TaskEmail(task.Task):
    """Send notfication email for a finished ProjectData."""

    order = 1003
    dependencies = ["upload"]

    def run(self):
        # TODO reorganize mailer and metadata
        # Gather fields to fill in for the message
        # (The name prefix is considered OK by RFC822, so we should be able to
        # leave that intact for both the sending part and the "To:" field.)
        contacts = self.proj._metadata["experiment_info"]["contacts"]
        contacts = ["%s <%s>" % (k, contacts[k]) for k in contacts]
        url = self.proj._metadata["task_output"].get("upload", {}).get("url", "")
        subject = "Illumina Run Processing Complete for %s" % self.proj.work_dir
        # Build message text and html
        body = "Hello,\n\n"
        body += "Illumina run processing is complete for %s\n" % self.proj.work_dir
        body += "and a zip file with results can be downloaded from this url:\n"
        body += "\n%s\n" % url
        html = "Hello,\n"
        html += "<br><br>\n\n"
        html += "Illumina run processing is complete for %s\n" % self.proj.work_dir
        html += "and a zip file with results can be downloaded from this url:\n"
        html += "<br><br>\n"
        html += "\n<a href='%s'>%s</a>\n" % (url, url)
        # Send
        kwargs = {
            "to_addrs": contacts,
            "subject": subject,
            "msg_body": body,
            "msg_html": html
            }
        self.proj.mailer(**kwargs)
        return kwargs