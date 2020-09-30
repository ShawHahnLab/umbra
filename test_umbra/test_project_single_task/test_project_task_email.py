"""
Test for single-task "email".
"""

from .test_project_task import TestProjectDataOneTask
from ..test_common import md5

class TestProjectDataEmail(TestProjectDataOneTask):
    """ Test for single-task "email".

    The mailer here is a stub that just records the email parameters given to
    it, so this doesn't test much, just that the message was constructed as
    expected.
    """

    def set_up_vars(self):
        self.task = "email"
        super().set_up_vars()
        # These checksums are *after* replacing the variable temporary directory
        # path with "TMP"; see make_paths_static helper method.
        self.expected["msg_body"] = "b34e1b1d387c3e9a3554b7f414545a00"
        self.expected["msg_html"] = "1ae87b9d9f92216a287a410f99eb30c6"
        self.expected["to_addrs"] = ["Name Lastname <name@example.com>"]

    def test_process(self):
        # The basic checks
        super().test_process()
        # After processing, there should be an email "sent" with the expected
        # attributes.  Using MD5 checksums on message text/html since it's a
        # bit bulky.
        email_obs = self.mails
        self.assertEqual(len(email_obs), 1)
        msg = email_obs[0]
        keys_exp = ["msg_body", "msg_html", "subject", "to_addrs"]
        self.assertEqual(sorted(msg.keys()), keys_exp)
        subject_exp = "Illumina Run Processing Complete for %s" % \
            self.proj.work_dir
        to_addrs_exp = self.expected["to_addrs"]
        self.assertEqual(msg["subject"], subject_exp)
        self.assertEqual(msg["to_addrs"], to_addrs_exp)
        self.assertEqual(
            md5(self.make_paths_static(msg["msg_body"])),
            self.expected["msg_body"])
        self.assertEqual(
            md5(self.make_paths_static(msg["msg_html"])),
            self.expected["msg_html"])

    def make_paths_static(self, txt):
        """Simple find-and-replace on the variable temp dir path.

        This makes the final output for text containing directories constant
        and testable even though we're using temporary directories during
        testing.
        """
        return txt.replace(str(self.paths["top"]), "TMP")


class TestProjectDataEmailOneName(TestProjectDataEmail):
    """What should happen if the email task just has one name?

    No difference.
    """

    def set_up_vars(self):
        super().set_up_vars()
        self.contacts_str = "Name <name@example.com>"
        self.expected["to_addrs"] = ["Name <name@example.com>"]
        self.expected["contacts"] = {"Name": "name@example.com"}


class TestProjectDataEmailNoName(TestProjectDataEmail):
    """What should happen if the email task just has a plain email address?

    This should use the first part of the address as the contact dict key and
    in the work_dir text, but nothing much else should change.
    """

    def set_up_vars(self):
        super().set_up_vars()
        self.contacts_str = "name@example.com"
        self.expected["contacts"] = {"name": "name@example.com"}
        self.expected["to_addrs"] = ["name <name@example.com>"]
        # (Very slightly different workdir (lowercase "name") and thus download
        # URL and thus message checksums)
        self.expected["work_dir"] = "2018-01-01-TestProject-name-XXXXX"
        self.expected["msg_body"] = "16278d694baba6f0d08f5a83a60f95cb"
        self.expected["msg_html"] = "54b0a2cb505e97a083bd63013a75e652"


class TestProjectDataEmailNoContacts(TestProjectDataEmail):
    """What should happen if the email task is run with no contact info at all?

    Nothing much different here.  The mailer should still be called as usual
    (it might have recipients it always appends) with the expected arguments.
    (As for other cases the actual Mailer behavior is tested separately for
    that class.) In this ProjectData, the work_dir slug will be shorter and the
    contacts should be an empty dict, modifying the formatted message slightly,
    but that's about it.
    """

    def set_up_vars(self):
        super().set_up_vars()
        self.contacts_str = ""
        self.expected["contacts"] = {}
        self.expected["to_addrs"] = []
        self.expected["work_dir"] = "2018-01-01-TestProject-XXXXX"
        self.expected["msg_body"] = "9bc8f2684423504363cc9d6ec7bbcdf4"
        self.expected["msg_html"] = "6d0796877d5c60c1c5661838dc207789"
