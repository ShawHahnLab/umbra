"""
Mock-up of the boxsdk package for testing.
"""

# Should consider cleaning this up as per here, especially the "Mocking
# classes" section:
# https://medium.com/@yeraydiazdiaz/what-the-mock-cheatsheet-mocking-in-python-6a71db997832

from unittest.mock import (Mock, create_autospec, DEFAULT)
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

def setup_folder_get_items(folder):
    "Re-usable side effect setup for the folder mock."""
    folder.get_items = Mock(side_effect=[[], [0], []])

def setup_network_blip(numfails=1):
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
        return DEFAULT

    return blip

# The folder object mock-up, with a very brittle assumption that on the first
# get_items call nothing has been uploaded, on the second one file has, and on
# the third there's nothing in the folder (so no need to clean anything up).
# This matches how the tests interact with BoxUploader.
FOLDER = Client.return_value.folder.return_value
setup_folder_get_items(FOLDER)
