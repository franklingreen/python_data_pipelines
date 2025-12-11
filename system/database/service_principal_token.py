
import msal
import struct
import logging
import traceback
from enum import Enum
from typing import Union
from system.config.sys_config_reader import microsoft_graph_config_my_app


class MicrosoftGraph(Enum):
    """Enum representing Microsoft Graph configurations."""
    MY_APP = (
        microsoft_graph_config_my_app().get("name"),
        microsoft_graph_config_my_app().get("tenant_id"),
        microsoft_graph_config_my_app().get("client_id"),
        microsoft_graph_config_my_app().get("client_secret"),
        microsoft_graph_config_my_app().get("app_secret"),
        microsoft_graph_config_my_app().get("authority"),
        microsoft_graph_config_my_app().get("scope_db"),
        microsoft_graph_config_my_app().get("scope_graph")
    )


    def __init__(self, name, tenant_id, client_id, client_secret, app_secret, authority, scope_db,
                 log: bool = True) -> None:
        """
        Initializes a MicrosoftGraph instance.

        Parameters:
        - tenant_id (str): Tenant ID.
        - client_id (str): Client ID.
        - client_secret (str): Client Secret.
        - app_secret (str): App Secret.
        - authority (str): Authority.
        - scope_db (str): Scope for database.
        - log (bool, optional): Enable logging. Default is False.
        """
        self.result = None
        self.token = None
        self.user_id = name
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.app_secret = app_secret
        self.authority = authority
        self.scope_db = scope_db
        self.app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            authority=self.authority,
            client_credential=self.client_secret
        )

        # Optional logging
        if log:
            logging.basicConfig(level=logging.INFO)
            logging.getLogger("msal").setLevel(logging.INFO)  # Optionally disable MSAL DEBUG logs

    @property
    def token(self) -> bytes:
        """
        Property to get the token.

        Returns:
        - bytes: Token in byte format.
        """
        return self._token_struct()

    @token.setter
    def token(self, value) -> None:
        """
        Setter for the token property.

        Parameters:
        - value (bytes): Token in byte format.
        """
        self._token = value

    def _get_token(self) -> None:
        """
        Private method to acquire and set the token.
        """
        try:
            self.result = self.app.acquire_token_silent(scopes=[self.scope_db], account=None)
            if not self.result:
                self.result = self.app.acquire_token_for_client(scopes=[self.scope_db])

            if "access_token" in self.result:
                self.token = self.result["access_token"]
            else:
                raise Exception(f"Token acquisition failed: {self.result.get('error')}")
        except Exception as e:
            raise Exception(f"Token acquisition failed: {traceback.format_exc()}")

    def _token_byte(self) -> bytes:
        """
        Private method to get token in byte format.

        Returns:
        - bytes: Token in byte format.
        """
        return bytes(self.result["access_token"], "UTF-8")

    def _exp_token(self) -> bytes:
        """
        Private method to expand the token.

        Returns:
        - bytes: Expanded token in byte format.
        """
        return b"".join(bytes([i, 0]) for i in self._token_byte())

    def _token_struct(self) -> bytes:
        """
        Private method to structure the token.

        Returns:
        - bytes: Structured token in byte format.
        """
        return struct.pack("=i", len(self._exp_token())) + self._exp_token()

    def get_user_id(self):
        return self.user_id

    def get_token(self, as_bytes: bool = True) -> Union[bytes, str]:
        """
        Public method to get the token.

        Returns:
        - bytes or string: Token in byte or string format.
        """
        self._get_token()
        if as_bytes:
            return self._token_struct()
        else:
            return self.result["access_token"]


if __name__ == '__main__':
    pass
    # test = MicrosoftGraph.MY_APP
    # test.get_token()
    # print(test.result)
