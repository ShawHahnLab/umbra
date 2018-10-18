#!/usr/bin/env python
"""
Tests for BoxUploader objects.

If a file with real Box API credentials are supplied via the configuration, a
live test will be run, over the Box API.  Otherwise Box tests are skipped.
"""

from test_common import *
import urllib.request
from illumina_processor.box_uploader import BoxUploader

config = test_config.get("box", {})

class TestBoxUploader(unittest.TestCase):

    def setUp(self):
        msg = "Box credentials not supplied."
        try:
            path = config.get("credentials_path")
            if not path:
                raise unittest.SkipTest(msg)
            self.box = BoxUploader(path, config)
        except FileNotFoundError:
            raise unittest.SkipTest(msg)

    def tearDown(self):
        for item in self.box._list():
            item.delete()

    def test_upload(self):
        data_exp = b"test_upload\n"
        with NamedTemporaryFile() as f:
            f.write(data_exp)
            f.flush()
            url = self.box.upload(f.name, "test_upload.txt")
        with urllib.request.urlopen(url) as f:
            data_obs = f.read()
        self.assertEqual(data_obs, data_exp)


if __name__ == '__main__':
    unittest.main()
