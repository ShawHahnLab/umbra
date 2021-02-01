"""
Test umbra.box_uploader

The Box SDK module is replaced with a mock version and local tests are run.  If
a file with real Box API credentials are supplied via the configuration, a live
test will also be run, over the real Box API.
"""

import urllib.request
import copy
import sys
import warnings
from tempfile import NamedTemporaryFile
from pathlib import Path
import threading
import queue
from umbra.box_uploader import BoxUploader
from .test_common import CONFIG, TestBase
from .shims import mock_boxsdk


class TestBoxUploaderBase(TestBase):
    """A shared setup for testing Box uploads."""

    def setUp(self):
        self.data_exp = b"test_upload\n"
        self.queue = queue.Queue()
        self.box_config = copy.deepcopy(CONFIG.get("box", {}))
        self.box = self.setup_box()

    def setup_box(self):
        """Set up BoxUploader object to test."""
        msg = "Box credentials not supplied."
        try:
            path = self.box_config.get("credentials_path")
            if not path:
                self.skipTest(msg)
            box = BoxUploader(path, self.box_config)
            return box
        except FileNotFoundError:
            self.skipTest(msg)

    def tearDown(self):
        for item in self.box.list():
            item.delete()

    def do_upload(self):
        """Upload a test file and fetch the data back."""
        with NamedTemporaryFile() as ftmp:
            ftmp.write(self.data_exp)
            ftmp.flush()
            filename = "test_upload_%s.txt" % threading.get_ident()
            url = self.box.upload(ftmp.name, filename)
        with urllib.request.urlopen(url) as furl:
            data_obs = furl.read()
        self.queue.put(data_obs)
        return data_obs

    def do_big_upload(self, mbytes):
        """Upload a bigger test file and fetch the data back."""
        # N MB of nothin'
        data_exp = bytes(1024 * 1024 * mbytes)
        with NamedTemporaryFile() as ftmp:
            ftmp.write(data_exp)
            ftmp.flush()
            filename = "test_big_upload_%s.txt" % threading.get_ident()
            url = self.box.upload(ftmp.name, filename)
        with urllib.request.urlopen(url) as furl:
            data_obs = furl.read()
        self.queue.put(data_obs)
        return data_obs, data_exp


class TestBoxUploader(TestBoxUploaderBase):
    """Test the BoxUploader class that handles file uploads to box.com."""

    def setUp(self):
        super().setUp()
        # This pattern will respect the warnings configuration in the calling
        # environment, but default to ignoring ResourceWarning as somewhere in
        # the depths of boxsdk sockets are getting left open at times.
        # https://stackoverflow.com/q/26563711/4499968
        if not sys.warnoptions:
            warnings.simplefilter("ignore", ResourceWarning)

    def test_upload(self):
        """Test uploading a file to Box."""
        self.assertEqual(len(self.box.list()), 0)
        data_obs = self.do_upload()
        self.assertEqual(data_obs, self.data_exp)
        self.assertEqual(len(self.box.list()), 1)

    def test_big_upload(self):
        """Test uploading a bigger file to Box.

        100 MB should get us into the realm of using the chunked uploader
        instead of the one-shot version.
        """
        self.assertEqual(len(self.box.list()), 0)
        data_obs, data_exp = self.do_big_upload(100)
        self.assertEqual(data_obs, data_exp)
        self.assertEqual(len(self.box.list()), 1)

    def test_parallel_upload(self):
        """Do threaded uploads with the same client work?

        (They do.  At one point I suspected this was causing a problem but it
        seems to do just fine.)
        """
        newth = lambda: threading.Thread(target=self.do_upload)
        numuploads = 4
        threads = [newth() for _ in range(numuploads)]
        # As simultaneously as we can, start up the threads, and then wait for
        # each to finish.
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        items = [self.queue.get_nowait() for _ in range(self.queue.qsize())]
        # There should be a bunch of identical file contents, as many as we did
        # uploads
        self.assertEqual(len(items), numuploads)
        self.assertEqual(set(items), set([self.data_exp]))


class TestBoxUploaderMock(TestBoxUploaderBase):
    """Test the BoxUploader class using a mock connection."""

    def setup_box(self):
        self.setup_box_shim()
        box = BoxUploader(
            self.box_config.get("credentials_path"),
            self.box_config)
        return box

    def tearDown(self):
        for item in self.box.list():
            item.unlink()
        self.teardown_box_shim()

    # Based on:
    # https://stackoverflow.com/a/1950214/4499968
    # But, see the patching mechanism in unittest.mock.  That may be the right
    # way to go.
    def setup_box_shim(self):
        """Setup testing shim module for the Box SDK."""
        sys.modules["real_boxsdk"] = sys.modules["umbra"].box_uploader.boxsdk
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

    @staticmethod
    def teardown_box_shim():
        """Swap testing shim module for the real Box SDK."""
        # Tests seem to run fine either way but I don't like the idea that it
        # leaves the fake module masking the real one.
        sys.modules["umbra"].box_uploader.boxsdk = sys.modules["real_boxsdk"]
        # Rebuild the BoxShim object to cleanup temp files and reset the mock
        # objects
        mock_boxsdk.BOXSHIM = mock_boxsdk.BoxShim()

    def test_upload(self):
        """Test uploading a file to Box."""
        self.assertEqual(len(self.box.list()), 0)
        self.do_upload()
        upload_mock = mock_boxsdk.FOLDER.upload
        upload_mock.assert_called()
        self.assertEqual(len(self.box.list()), 1)


class TestBoxUploaderMockDisconnect(TestBoxUploaderMock):
    """Test the case where an upload attempt is interrupted."""

    def setUp(self):
        mock_boxsdk.BOXSHIM.setup_network_blip(1)
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
