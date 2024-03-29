"""
A simple interface to Box.com to upload individual files to a folder.

See BoxUploader for usage details.
"""

import re
import logging
import time
import datetime
import subprocess
from pathlib import Path
import yaml
import boxsdk
from boxsdk.exception import (BoxAPIException, BoxOAuthException)
from .util import yaml_load

LOGGER = logging.getLogger(__name__)

# Box logs the entire (!) uploaded file at level INFO.
# Let's ignore INFO and below.
__BOXLOGGER = logging.getLogger("boxsdk.network.default_network")
__BOXLOGGER.setLevel(logging.WARNING)


class BoxHandler:
    """Lower-level Box client details like authentication.

    See BoxUploader.
    """

    # These dict keys must appear in the credentials file.  user_access_token
    # and user_refresh_token can be missing.
    REQUIRED_FIELDS = ["client_id", "client_secret", "redirect_uri"]

    def __init__(self, creds_store_path, config=None):
        if config is None:
            config = {}
        self.config = config
        self.creds_store_path = Path(creds_store_path)
        self.creds = self._load_creds()
        self.client = self._init_client_wrapper()
        self.__max_upload_size = self._init_max_upload_size()

    def _init_client(self):
        oauth = boxsdk.OAuth2(
            client_id=self.creds["client_id"],
            client_secret=self.creds["client_secret"],
            store_tokens=self._store_tokens,
            access_token=self.creds["user_access_token"],
            refresh_token=self.creds["user_refresh_token"])
        client = boxsdk.Client(oauth)
        return client

    def _load_creds(self):
        creds = yaml_load(self.creds_store_path)
        req = BoxHandler.REQUIRED_FIELDS
        miss = [field for field in req if field not in creds.keys()]
        if miss:
            raise ValueError("Missing configuration entries: %s" % ", ".join(miss))
        for key in ["user_access_token", "user_refresh_token"]:
            creds[key] = creds.get(key, 0)
        return creds

    def _init_max_upload_size(self):
        user_info = self.client.user(user_id='me').get()
        max_upload_size = int(user_info["max_upload_size"])
        return max_upload_size

    @property
    def max_upload_size(self):
        """Maximum allowed upload size in bytes."""
        return self.__max_upload_size

    def _store_tokens(self, access_token, refresh_token):
        """Callback to store new access/refresh tokens to disk."""
        self.creds["user_access_token"] = access_token
        self.creds["user_refresh_token"] = refresh_token
        with open(self.creds_store_path, "w") as fout:
            fout.write(yaml.dump(self.creds))
        LOGGER.info("Tokens refreshed.")

    def _init_client_wrapper(self):
        """Load Box API credentials and put in an OAuth2 object.

        If authentication fails or credentials are missing, attempt to
        re-connect with Box in a browser with a provided URL."""
        try:
            client = self._init_client()
            client.user(user_id='me').get()
        # If (and only if) the problem is authentication, try getting new
        # access and refresh tokens.
        # I've seen two different classes of exception in this case, the API
        # one originally and the OAuth one more recently.  I'll try to keep
        # handling either.
        except BoxAPIException as exception:
            # b'{"error":"invalid_grant","error_description":"Refresh token has expired"}'
            if exception.status == 400 and not self.config.get("strict_auth"):
                LOGGER.critical(
                    ("Authentication failure.  Try re-connecting with Box in"
                     " a browser with the URL shown."))
                client = self._janky_auth_trick()
            else:
                raise exception
        except BoxOAuthException as exception:
            if "Status: 400" in str(exception) and not self.config.get("strict_auth"):
                LOGGER.critical(
                    ("Authentication failure.  Try re-connecting with Box in"
                     " a browser with the URL shown."))
                client = self._janky_auth_trick()
            else:
                raise exception
        return client

    def _janky_auth_trick(self, log_path="/var/log/nginx/access.log"):
        """The most minimal sort of method of getting a Box API access token.

        Steps:
        1) set up App on box.com, pointing the redirect URL to your own web server
        2) run this function
        3) take app auth URL from standard output
        4) visit app auth URL in browser, triggering Box to contact your server
        5) take OAuth2 object as return value and/or auth text from standard output

        log_path should be the path to a readable web server log file in the Common
        Log Format.

        In more detail:

        1) First create an App at https://app.box.com/developers/console.  For
        Authentication Method use "Standard OAuth 2.0 (User Authentication)"
        Copy and paste the Client ID and Client Secret text into the
        credeitnals file.  Set Redirect URI to a HTTPS URL you control.  The
        destination doesn't much matter since it can just trigger a 404 error
        on your side that shows up in the log.  You must have control over an
        HTTPS-enabled web server to do this; I originally planned to use netcat
        on a high-numbered port and just read the text, but Box requires HTTPS.
        (All this, even though using the API doesn't actually require a web
        server running.)

        2) Now run this function and note the URL printed.

        3) Nothing much will happen at first.  Copy and paste the URL printed to
        standard output into a web browser.

        4) Say "yes" in the browser to give the app access to your account, and
        then watch for more lines on the terminal.  Box should connect, make an
        HTTP request with all the relevant info, and trigger a 404 error that ends
        up in the logs.  (There's a pretty short timeout -- sixty seconds? -- on
        the whole process, so as soon as Box sends the data to your web server this
        function needs to catch it.)

        5) Use the returned OAuth2 object and/or enter those extra lines into
        config.py as user_access_token and user_refresh_token.
        """
        oauth = boxsdk.OAuth2(client_id=self.creds["client_id"],
                              client_secret=self.creds["client_secret"])
        auth_url = oauth.get_authorization_url(self.creds["redirect_uri"])[0]
        LOGGER.critical("Auth URL: %s", auth_url)
        code = scrape_log_for_code(log_path)
        access_token, refresh_token = oauth.authenticate(code)
        self._store_tokens(access_token, refresh_token)
        return self._init_client()


class BoxUploader(BoxHandler):
    """A simple Box API interface to upload files to one directory.

    Credentials are provided in a dedicated YAML-formatted text file that must
    at least contain client_id, client_secret, and redirect_uri entries.
    user_access_token and user_refresh_token are the time-limited credentials
    provided by Box.  They will be generated by a one-time web-based login if
    needed, but note that requires a manual step with a browser and read access
    to web server logs.  Once valid the credentials will be refreshed
    automatically and can last indefinitely as long as there's communication
    with Box at least every sixty days.  For more on the first-time credentials
    setup see _janky_auth_trick().

    Other options are provided by a configuration dictionary, currently just
    specifying the folder ID to upload to.  (This ID is the last segment of the
    URL shown in the web interface for any given folder; 0 for the root
    folder.)

    There is just one public function, upload(), taking a file path and an
    optional custom name.  An existing file with the same name in Box will
    cause an exception."""

    def __init__(self, creds_store_path, config):
        super().__init__(creds_store_path, config)
        user_info = self.client.user(user_id='me').get()
        folder_id = self.config.get("folder_id", 0)
        folder_name = self.folder.get()['name']
        LOGGER.info('User: %s', user_info['login'])
        LOGGER.info('Max upload size in bytes: %d', self.max_upload_size)
        LOGGER.info("Upload folder: %d (%s)", folder_id, folder_name)
        LOGGER.debug("BoxUploader initialized.")

    @property
    def folder(self):
        """The Box folder object for the configured upload folder."""
        folder_id = self.config.get("folder_id", 0)
        return self.client.folder(str(folder_id))

    def upload(self, path, name=None):
        """Upload file from a given path, optionally with custom name."""
        path = Path(path)
        fsize = path.stat().st_size
        if fsize > self.max_upload_size:
            msg = "File size (%d) exceeds max upload size (%d)"
            msg += msg % (fsize, self.max_upload_size)
            raise ValueError(msg)
        if not name:
            name = path.name
        # "The Chunked Upload API is only for uploading large files and will
        # not accept files smaller than 20MB in size."
        # https://medium.com/box-developer-blog/introducing-the-chunked-upload-api-f82c820ccfcb
        if fsize < 50 * 1024 * 1024:
            # Possibly add a call to folder.canUpload() to make sure it would work,
            # first.
            box_file = self.__ft_upload(path, name)
            url = box_file.get_shared_link_download_url(access="open")
        else:
            box_file = self.__chunked_upload(path, name)
            url = box_file.get_shared_link_download_url(access="open")
        LOGGER.info("File uploaded: %s", str(path))
        return url

    def __ft_upload(self, path, name, tries=8):
        """Fault-tolerant basic upload for smaller files.

        Try a few times, intercepting and logging any sort of IOError
        encountered during each try.  This fault-tolerant approach might be
        obsolete thanks to the chunked upload for larger files, but leaving
        as-is for now.
        """
        # Vaguely based on:
        # https://medium.com/@echohack/patterns-with-python-poll-an-api-832173a03e93
        upload_error = None
        box_file = None
        time_delta = 1
        time_mult = 2
        for trynum in range(tries):
            try:
                box_file = self.folder.upload(str(path), name)
            except IOError as err:
                # (requests.exception.RequestException and all the rest are
                # subclasses of IOError)
                upload_error = err
                LOGGER.warning("Upload attempt %d failed", trynum+1)
                time.sleep(time_delta)
                time_delta *= time_mult
            else:
                if trynum > 0:
                    LOGGER.warning("Upload attempt %d succeeded", trynum+1)
                break
        else:
            LOGGER.error(
                "Upload attempts exhausted (%s)",
                upload_error.__class__.__name__)
            raise upload_error
        return box_file

    def __chunked_upload(self, path, name):
        """Chunked upload for larger files."""
        # https://github.com/box/box-python-sdk/blob/master/boxsdk/object/folder.py#L151
        total_size = Path(path).stat().st_size
        content_stream = open(path, "rb")
        upload_session = self.folder.create_upload_session(total_size, name)
        uploader = upload_session.get_chunked_uploader_for_stream(content_stream, total_size)
        box_file = uploader.start()
        return box_file

    def list(self, chunk=100):
        """List of file and folder objects in the uploader folder.

        With the older Box SDK this gave an actual Python list back, but later
        versions switched to a LimitOffsetBasedObjectCollection object which
        doesn't implement basic stuff like len().  I should probably look into
        what that class is but for now casting it to a list seems to work.
        """
        offset = 0
        items = list(self.folder.get_items(chunk))
        allitems = items
        while len(items) == chunk:
            offset += chunk
            items = list(self.folder.get_items(chunk, offset))
            allitems.extend(items)
        return allitems


def _parse_log_line(line):
    """Parse a single line of log file text assuming Common Log Format."""
    # Nginx:
    #log_format combined '$remote_addr - $remote_user [$time_local] '
    #                    '"$request" $status $body_bytes_sent '
    #                    '"$http_referer" "$http_user_agent"';
    # See also:
    # https://nginx.org/en/docs/http/ngx_http_log_module.html
    # https://en.wikipedia.org/wiki/Common_Log_Format
    try:
        line = line.decode("ASCII")
    except AttributeError:
        pass
    #       src IP   X  user   time    request  status   bytes     referer   agent
    fmt = r'([0-9.]+) - (.+) \[(.+)\] "([^"]+)" ([0-9]+) ([0-9]+) "([^"]+)" "([^"]+)"'
    fmt_date = "%d/%b/%Y:%H:%M:%S %z"
    match = re.match(fmt, line)
    logentry = {}
    logentry["IP"] = match.group(1)
    logentry["User"] = match.group(2)
    logentry["Time"] = datetime.datetime.strptime(match.group(3), fmt_date)
    logentry["Request"] = match.group(4)
    logentry["Status"] = match.group(5)
    logentry["Bytes"] = int(match.group(6))
    logentry["Referer"] = match.group(7)
    logentry["User Agent"] = match.group(8)
    return logentry

def scrape_log_for_code(log_path):
    """Watch log file until a Box auth code appears, then return it."""
    box_url_suffix = 'box.com'
    # "tail -n 0 -f" will give all lines written to the file after the tail
    # process starts.
    # Based on: https://stackoverflow.com/a/12523371/6073858
    pipes = subprocess.Popen(['tail', '-n', '0', '-f', log_path],
                             stdout=subprocess.PIPE)
    while True:
        line = pipes.stdout.readline()
        logentry = _parse_log_line(line)
        if box_url_suffix in logentry["Referer"]:
            match = re.search('code=([^ ]*) ', logentry["Request"])
            code = match.group(1)
            return code
