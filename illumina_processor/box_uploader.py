import boxsdk
import yaml

class BoxUploader:

    def __init__(self, creds_store_path):
        self.creds_store_path = Path(creds_store_path)
        self._init_creds()
        self.client = boxsdk.Client(self.oauth)

    def _init_creds(self):
        """Load Box API credentials and put in an OAuth2 object."""
        with open(self.creds_store_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.oauth = boxsdk.OAuth2(client_id = self.config.client_id,
                       client_secret = self.config.client_secret,
                       store_tokens = self._store_tokens,
                       access_token = self.config.user_access_token,
                       refresh_token = self.config.user_refresh_token)

    def _store_tokens(self, access_token, refresh_token):
        """Callback to store new access/refresh tokens to disk."""
        self.config["access_token"] = access_token
        self.config["refresh_token"] = refresh_token
        with open(self.creds_store_path, "w") as f:
            f.write(yaml.dump(self.config))
