import requests

from flask import request, session as flask_session, redirect
import flask_login

from app.db import with_session, DBSession
from logic.user import get_user_by_name, create_user, update_user_properties
from app.auth.utils import (
    AuthUser,
    abort_unauthorized,
    AuthenticationError
)

from app.auth.oauth_auth import OAuthLoginManager
from env import QuerybookSettings, get_env_config
from lib.logger import get_logger
from lib.utils.decorators import in_mem_memoized

LOG = get_logger(__file__)

OIDC_CALLBACK_PATH = "/storybook/auth/oidc/callback"

CONST_OIDC_CLIENT_ID = "OIDC_CLIENT_ID"
CONST_OIDC_CLIENT_SECRET = "OIDC_CLIENT_SECRET"
CONST_OIDC_BASE_URL = "OIDC_BASE_URL"
CONST_HEIMDALL_BASE_URL = "HEIMDALL_BASE_URL"

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
            OIDC_CALLBACK_PATH, "oidc_callback", self.oidc_callback
        )

    def oidc_callback(self):
        LOG.debug("*** Handling Oidc callback...")

        if request.args.get("error"):
            return f"<h1>Error: {request.args.get('error')}</h1>"

        code = request.args.get("code")

        try:
            access_token = self._fetch_access_token(code)
            username, email = self._get_user_profile(access_token)
            apikey = self._get_heimdall_apikey(username, access_token)
            with DBSession() as session:
                flask_login.login_user(
                    AuthUser(self.login_user(username, email, apikey, session=session))
                )
        except AuthenticationError:
            abort_unauthorized()

        next_url = "/"
        if "next" in flask_session:
            next_url = flask_session["next"]
            del flask_session["next"]

        return redirect(next_url)

    @with_session
    def login_user(self, username, email, apikey, session=None):
        user = get_user_by_name(username, session=session)
        if not user:
            user = create_user(
                username=username, fullname=username, email=email, session=session
            )
        if apikey:
            update_user_properties(user.id, heimdall=apikey)

        return user

    def get_oidc_urls(self):
        oidc_base_url = get_env_config(CONST_OIDC_BASE_URL)
        LOG.debug(f"HEIMDALL oidc_base_url: {oidc_base_url}")

        authorization_url = f"{oidc_base_url}/auth"
        token_url = f"{oidc_base_url}/token"
        profile_url = f"{oidc_base_url}/userinfo"

        return authorization_url, token_url, profile_url

    def get_oidc_secrets(self):
        client_id = get_env_config(CONST_OIDC_CLIENT_ID)
        client_secret = get_env_config(CONST_OIDC_CLIENT_SECRET)

        LOG.debug(f"OIDC client_id: {client_id}")

        return client_id, client_secret

    @property
    @in_mem_memoized()
    def oauth_config(self):
        authorization_url, token_url, profile_url = self.get_oidc_urls()
        client_id, client_secret = self.get_oidc_secrets()
        callback_url = "{}{}".format(QuerybookSettings.PUBLIC_URL, OIDC_CALLBACK_PATH)
        LOG.debug(f"HEIMDALL callback_url: {callback_url}")

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
        heimdall_base_url = get_env_config(CONST_HEIMDALL_BASE_URL)

        # Authorize
        heimdall_auth_url = f"{heimdall_base_url}/api/v1/authorize"
        LOG.debug(f"HEIMDALL auth_url: {heimdall_auth_url}")

        resp = requests.post(heimdall_auth_url, json={"token": access_token})
        LOG.debug(f"resp: {resp.status_code}")
        if resp and resp.status_code == 200:
            reply = resp.json()
            if reply["allow"] and reply["result"] is not None:
                user_id = reply["result"]["id"]

                # Profile
                heimdall_profile_url = f"{heimdall_base_url}/api/v1/users/{user_id}"
                LOG.debug(f"HEIMDALL profile_url: {heimdall_profile_url}")

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

    def _get_heimdall_apikey(self, user_id, access_token):
        heimdall_base_url = get_env_config(CONST_HEIMDALL_BASE_URL)
        heimdall_apikey_url = f"{heimdall_base_url}/api/v1/tokens"
        LOG.debug(f"HEIMDALL apikey_url: {heimdall_apikey_url}")

        headers = {"Authorization": "Bearer {}".format(access_token)}
        resp = requests.post(
            heimdall_apikey_url,
            headers=headers,
            json={
                "user_id": user_id,
                "type": "apikey",
                "use_existing": True,
                "duration": "8760h",  # 1 Year
            },
        )

        if resp.status_code == 200 or resp.status_code == 201:
            token = resp.json()
            return token["data"]["apikey"]
        else:
            raise AuthenticationError(
                "Failed to get_or_create apikey from Heimdall, status ({0}), body ({1})".format(
                    resp.status if resp else "None", resp.json() if resp else "None"
                )
            )


login_manager = HeimdallLoginManager()

ignore_paths = [OIDC_CALLBACK_PATH]


def init_app(app):
    login_manager.init_app(app)


def login(request):
    return login_manager.login(request)
