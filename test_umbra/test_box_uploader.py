#!/usr/bin/env python
"""
Test umbra.box_uploader

More specifically, tests for BoxUploader objects.  If a file with real Box API
credentials are supplied via the configuration, a live test will be run, over
the Box API.  Otherwise Box tests are skipped.
"""

import urllib.request
import unittest
from tempfile import NamedTemporaryFile
from umbra.box_uploader import BoxUploader
from .test_common import CONFIG


class TestBoxUploader(unittest.TestCase):
    """Test the BoxUploader class that handles file uploads to box.com."""

    def setUp(self):
        box_config = CONFIG.get("box", {})
        msg = "Box credentials not supplied."
        try:
            path = box_config.get("credentials_path")
            if not path:
                raise unittest.SkipTest(msg)
            self.box = BoxUploader(path, box_config)
        except FileNotFoundError:
            raise unittest.SkipTest(msg)

    def tearDown(self):
        for item in self.box._list():
            item.delete()

    def test_upload(self):
        """Test uploading a file to Box."""
        data_exp = b"test_upload\n"
        with NamedTemporaryFile() as ftmp:
            ftmp.write(data_exp)
            ftmp.flush()
            url = self.box.upload(ftmp.name, "test_upload.txt")
        with urllib.request.urlopen(url) as furl:
            data_obs = furl.read()
        self.assertEqual(data_obs, data_exp)


if __name__ == '__main__':
    unittest.main()
