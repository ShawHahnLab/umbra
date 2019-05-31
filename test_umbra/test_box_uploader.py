#!/usr/bin/env python
"""
Test umbra.box_uploader

The Box SDK module is replaced with a mock version and local tests are run.  If
a file with real Box API credentials are supplied via the configuration, a live
test will also be run, over the real Box API.
"""

import urllib.request
import unittest
import copy
import sys
from tempfile import NamedTemporaryFile
from pathlib import Path
from umbra.box_uploader import BoxUploader
from .test_common import CONFIG
from .shims import mock_boxsdk


class TestBoxUploader(unittest.TestCase):
    """Test the BoxUploader class that handles file uploads to box.com."""

    def setUp(self):
        msg = "Box credentials not supplied."
        self.box_config = CONFIG.get("box", {})
        try:
            path = self.box_config.get("credentials_path")
            if not path:
                raise unittest.SkipTest(msg)
            self.box = BoxUploader(path, self.box_config)
        except FileNotFoundError:
            raise unittest.SkipTest(msg)

    def tearDown(self):
        for item in self.box.list():
            item.delete()

    def test_upload(self):
        """Test uploading a file to Box."""
        self.assertEqual(len(self.box.list()), 0)
        data_exp = b"test_upload\n"
        with NamedTemporaryFile() as ftmp:
            ftmp.write(data_exp)
            ftmp.flush()
            url = self.box.upload(ftmp.name, "test_upload.txt")
        with urllib.request.urlopen(url) as furl:
            data_obs = furl.read()
        self.assertEqual(data_obs, data_exp)
        self.assertEqual(len(self.box.list()), 1)


class TestBoxUploaderMock(TestBoxUploader):
    """Test the BoxUploader class using a mock connection."""

    def setUp(self):
        self.box_config = copy.deepcopy(CONFIG.get("box", {}))
        self.setup_box_shim()
        self.box = BoxUploader(
            self.box_config.get("credentials_path"),
            self.box_config)

    # Based on:
    # https://stackoverflow.com/a/1950214/4499968
    # But, see the patching mechanism in unittest.mock.  That may be the right
    # way to go.
    def setup_box_shim(self):
        """Setup testing shim module for the Box SDK."""
        mock_boxsdk.setup_folder_get_items(mock_boxsdk.FOLDER)
        self.real_boxsdk = sys.modules["umbra"].box_uploader.boxsdk
        sys.modules["umbra"].box_uploader.boxsdk = mock_boxsdk
        box_path = self.box_config.get("credentials_path")
        if not box_path or not Path(box_path).exists():
            box_path = NamedTemporaryFile().name
            self.box_config["credentials_path"] = box_path
            fakecreds = {
                "client_id": "A",
                "client_secret": "B",
                "redirect_uri": "https://example.com"}
            fakecreds = ["%s: %s\n" % (k, fakecreds[k]) for k in fakecreds]
            with open(box_path, "w") as f_out:
                f_out.writelines(fakecreds)

    def teardown_box_shim(self):
        """Swap testing shim module for the real Box SDK."""
        # Tests seem to run fine either way but I don't like the idea that it
        # leaves the fake module masking the real one.
        sys.modules["umbra"].box_uploader.boxsdk = self.real_boxsdk

    def tearDown(self):
        self.teardown_box_shim()

    def test_upload(self):
        """Test uploading a file to Box."""
        self.assertEqual(len(self.box.list()), 0)
        data_exp = b"test_upload\n"
        with NamedTemporaryFile() as ftmp:
            ftmp.write(data_exp)
            ftmp.flush()
            self.box.upload(ftmp.name, "test_upload.txt")
        upload_mock = mock_boxsdk.FOLDER.upload
        upload_mock.assert_called()
        self.assertEqual(len(self.box.list()), 1)


class TestBoxUploaderMockDisconnect(TestBoxUploaderMock):
    """Test the case where an upload attempt is interrupted."""

    def setUp(self):
        mock_boxsdk.FOLDER.upload.side_effect = mock_boxsdk.setup_network_blip(1)
        super().setUp()

    def test_upload(self):
        self.assertEqual(len(self.box.list()), 0)
        data_exp = b"test_upload\n"
        with NamedTemporaryFile() as ftmp:
            ftmp.write(data_exp)
            ftmp.flush()
            with self.assertLogs(level="WARNING") as logging_context:
                self.box.upload(ftmp.name, "test_upload.txt")
                self.assertEqual(len(logging_context.output), 2)
        mock_boxsdk.FOLDER.upload.assert_called()
        self.assertEqual(len(self.box.list()), 1)

if __name__ == '__main__':
    unittest.main()
