import requests

from app.auth.oauth_auth import OAuthLoginManager
from env import QuerybookSettings, get_env_config
from lib.logger import get_logger
from lib.utils.decorators import in_mem_memoized
from .utils import AuthenticationError

LOG = get_logger(__file__)

OIDC_CALLBACK_PATH = "/storybook/auth/oidc/callback"

"""
AUTH_BACKEND: 'app.auth.heimdall_auth'
OIDC_CLIENT_ID: 'storybook'
OIDC_CLIENT_SECRET: 'e63Q3sMd8hhqhVthKcxoC5derCdphUMl'
OIDC_BASE_URL: https://easily-champion-frog.dataos.io/oidc
HEIMDALL_BASE_URL: https://easily-champion-frog.dataos.io/heimdall
PUBLIC_URL: http://127.0.0.1:3000
"""


class HeimdallLoginManager(OAuthLoginManager):
    def init_app(self, flask_app):
        super().init_app(flask_app)

        self.flask_app.add_url_rule(
            OIDC_CALLBACK_PATH, "oidc_callback", self.oauth_callback
        )

    def get_oidc_urls(self):
        oidc_base_url = get_env_config("OIDC_BASE_URL")
        LOG.debug(f"oidc_base_url: {oidc_base_url}")

        authorization_url = f"{oidc_base_url}/auth"
        token_url = f"{oidc_base_url}/token"
        profile_url = f"{oidc_base_url}/userinfo"

        return authorization_url, token_url, profile_url

    def get_oidc_secrets(self):
        client_id = get_env_config("OIDC_CLIENT_ID")
        client_secret = get_env_config("OIDC_CLIENT_SECRET")

        LOG.debug(f"client_id: {client_id}")

        return client_id, client_secret

    @property
    @in_mem_memoized()
    def oauth_config(self):
        authorization_url, token_url, profile_url = self.get_oidc_urls()
        client_id, client_secret = self.get_oidc_secrets()
        callback_url = "{}{}".format(QuerybookSettings.PUBLIC_URL, OIDC_CALLBACK_PATH)
        LOG.debug(f"callback_url: {callback_url}")

        return {
            "callback_url": callback_url,
            "client_id": client_id,
            "client_secret": client_secret,
            "authorization_url": authorization_url,
            "token_url": token_url,
            "profile_url": profile_url,
            "scope": ["openid", "profile", "email", "groups", "federated:id"],
        }

    def _get_user_profile(self, access_token):
        heimdall_base_url = get_env_config("HEIMDALL_BASE_URL")

        # Authorize
        heimdall_auth_url = f"{heimdall_base_url}/api/v1/authorize"
        resp = requests.post(heimdall_auth_url, json={"token": access_token})
        LOG.debug(f"resp: {resp.status_code}")
        if resp and resp.status_code == 200:
            reply = resp.json()
            if reply["allow"] and reply["result"] is not None:
                user_id = reply["result"]["id"]

                # Profile
                heimdall_profile_url = f"{heimdall_base_url}/api/v1/users/{user_id}"
                LOG.debug(f"url: {heimdall_profile_url}")

                headers = {"Authorization": "Bearer {}".format(access_token)}
                resp = requests.get(heimdall_profile_url, headers=headers)
                LOG.debug(f"resp: {resp.status_code}")
                if resp.status_code == 200:
                    return self._parse_user_profile(resp)
                else:
                    raise AuthenticationError(
                        "Failed to fetch user profile, status ({0}), body ({1})".format(
                            resp.status if resp else "None",
                            resp.json() if resp else "None",
                        )
                    )
        else:
            raise AuthenticationError(
                "Failed to authorize with Heimdall, status ({0}), body ({1})".format(
                    resp.status if resp else "None", resp.json() if resp else "None"
                )
            )

    def _parse_user_profile(self, resp):
        user = resp.json()
        LOG.info(f"resolved user: {user}")
        return user["name"], user["email"]


login_manager = HeimdallLoginManager()

ignore_paths = [OIDC_CALLBACK_PATH]


def init_app(app):
    login_manager.init_app(app)


def login(request):
    return login_manager.login(request)
