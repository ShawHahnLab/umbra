"""
Mock-up of the boxsdk package for testing.
"""

# Should consider cleaning this up as per here, especially the "Mocking
# classes" section:
# https://medium.com/@yeraydiazdiaz/what-the-mock-cheatsheet-mocking-in-python-6a71db997832

from unittest.mock import (Mock, create_autospec, DEFAULT)
import tempfile
from pathlib import Path
from requests.exceptions import ConnectionError as RequestsConnectionError
import boxsdk
import boxsdk.exception

# Objects used from boxsdk
# pylint: disable=invalid-name
exception = boxsdk.exception
OAuth2 = create_autospec(boxsdk.OAuth2)
Client = create_autospec(boxsdk.Client)
Client.return_value.user.return_value.get = Mock(
    return_value={"max_upload_size": 1024, "login": "login"})
FOLDER = Client.return_value.folder.return_value

class BoxShim:
    """Manage mock Box SDK objects and temp files.

    Just use the one instance of this instantiated within this module.
    Re-assign a new instance to reset the mock objects and temp files.
    """

    def __init__(self):
        self.dir = tempfile.TemporaryDirectory()
        # The folder object mock-up, with a very brittle assumption that on the first
        # get_items call nothing has been uploaded, on the second one file has, and on
        # the third there's nothing in the folder (so no need to clean anything up).
        # This matches how the tests interact with BoxUploader.
        FOLDER.upload.side_effect = self.upload
        FOLDER.upload.return_value.get_shared_link_download_url.side_effect = self.shared_link_url
        FOLDER.get_items = Mock(side_effect=lambda _: list(Path(self.dir.name).glob("*")))
        self.uploads = []

    def __del__(self):
        self.dir.cleanup()

    def upload(self, path, name, **__):
        """Mock upload effect (dump file in /tmp location)."""
        with open(path, "rb") as f_in:
            data = f_in.read()
        path = Path(self.dir.name, name)
        with open(path, "wb") as f_out:
            f_out.write(data)
            f_out.flush()
        self.uploads.append(path)
        return DEFAULT

    def shared_link_url(self, *_, **__):
        """Mock shared link (file URL from /tmp location).

        This just gives the most recent file "uploaded."
        """
        return "file://%s" % self.uploads[-1]

    def setup_network_blip(self, numfails=1):
        """Simulate network failure during Box folder upload.

        Set the returned function from this as the side effect for folder.upload to
        test intermittent network failure.
        """
        def blip(*_, **__):
            if not "uploads" in dir(FOLDER):
                FOLDER.uploads = 0
            try:
                if FOLDER.uploads < numfails:
                    # Use the exception seen from the requests package, just like
                    # boxsdk actually does
                    raise RequestsConnectionError()
            finally:
                FOLDER.uploads += 1
            self.upload(*_, **__)
            return DEFAULT
        FOLDER.upload.side_effect = blip

BOXSHIM = BoxShim()
