"""ICE Connect API data source plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

import aiohttp

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class ICEConnectPlugin(DataSourcePlugin):
    """Plugin for ICE Connect API (Intercontinental Exchange)."""

    plugin_id = "ice_connect"
    plugin_name = "ICE Connect"
    plugin_description = "Connect to ICE (Intercontinental Exchange) data feeds"
    plugin_icon = "trending_up"

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._session = None
        self._access_token = None

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="api_url",
                label="API URL",
                field_type=FieldType.TEXT,
                required=True,
                default="https://api.theice.com",
                placeholder="https://api.theice.com",
                help_text="ICE Connect API base URL",
            ),
            CredentialField(
                name="client_id",
                label="Client ID",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="Your ICE client ID",
            ),
            CredentialField(
                name="client_secret",
                label="Client Secret",
                field_type=FieldType.PASSWORD,
                required=True,
                placeholder="Your ICE client secret",
            ),
            CredentialField(
                name="username",
                label="Username",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="ICE Connect username",
            ),
            CredentialField(
                name="password",
                label="Password",
                field_type=FieldType.PASSWORD,
                required=True,
            ),
            CredentialField(
                name="data_type",
                label="Data Type",
                field_type=FieldType.SELECT,
                required=True,
                default="eod",
                options=[
                    {"value": "eod", "label": "End of Day Prices"},
                    {"value": "intraday", "label": "Intraday Data"},
                    {"value": "reference", "label": "Reference Data"},
                    {"value": "curves", "label": "Forward Curves"},
                    {"value": "settlements", "label": "Settlements"},
                ],
            ),
            CredentialField(
                name="products",
                label="Products / Contracts",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="BRN, WTI, NG",
                help_text="Comma-separated list of ICE product codes",
            ),
            CredentialField(
                name="exchange",
                label="Exchange",
                field_type=FieldType.SELECT,
                required=False,
                default="IFEU",
                options=[
                    {"value": "IFEU", "label": "ICE Futures Europe"},
                    {"value": "IFUS", "label": "ICE Futures U.S."},
                    {"value": "IFCA", "label": "ICE Futures Canada"},
                    {"value": "IFEN", "label": "ICE Endex"},
                    {"value": "IFSG", "label": "ICE Futures Singapore"},
                ],
            ),
        ]

    async def _authenticate(self) -> bool:
        """Authenticate with ICE Connect API."""
        try:
            api_url = self.credentials.get("api_url", "").rstrip("/")
            client_id = self.credentials.get("client_id", "")
            client_secret = self.credentials.get("client_secret", "")
            username = self.credentials.get("username", "")
            password = self.credentials.get("password", "")

            auth_url = f"{api_url}/oauth/token"

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    auth_url,
                    data={
                        "grant_type": "password",
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "username": username,
                        "password": password,
                    },
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        self._access_token = data.get("access_token")
                        return True
                    else:
                        return False

        except Exception as e:
            print(f"ICE authentication error: {e}")
            return False

    async def connect(self) -> bool:
        """Connect to ICE Connect API."""
        self._session = aiohttp.ClientSession()

        if await self._authenticate():
            self._connected = True
            return True
        else:
            await self._session.close()
            self._session = None
            return False

    async def disconnect(self):
        """Disconnect from ICE Connect API."""
        if self._session:
            await self._session.close()
            self._session = None
        self._access_token = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        """Test ICE Connect connection."""
        client_id = self.credentials.get("client_id", "")
        client_secret = self.credentials.get("client_secret", "")

        if not client_id:
            return False, "Client ID is required"
        if not client_secret:
            return False, "Client Secret is required"

        if await self._authenticate():
            return True, "Successfully authenticated with ICE Connect"
        else:
            return False, "Authentication failed - check credentials"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        """Fetch data from ICE Connect API."""
        if not self._session or not self._access_token:
            return

        api_url = self.credentials.get("api_url", "").rstrip("/")
        data_type = self.credentials.get("data_type", "eod")
        products = [p.strip() for p in self.credentials.get("products", "").split(",")]
        exchange = self.credentials.get("exchange", "IFEU")

        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
        }

        # Build endpoint based on data type
        endpoints = {
            "eod": "/v1/marketdata/eod",
            "intraday": "/v1/marketdata/intraday",
            "reference": "/v1/reference/products",
            "curves": "/v1/marketdata/curves",
            "settlements": "/v1/marketdata/settlements",
        }

        endpoint = endpoints.get(data_type, "/v1/marketdata/eod")

        try:
            for product in products:
                url = f"{api_url}{endpoint}"
                params = {
                    "product": product,
                    "exchange": exchange,
                }

                async with self._session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Handle various response formats
                        records = data if isinstance(data, list) else data.get("data", [data])

                        for i, record in enumerate(records):
                            yield DataRecord(
                                source_id=self.source_id,
                                source_type=self.plugin_id,
                                timestamp=datetime.utcnow().isoformat(),
                                data=record,
                                metadata={
                                    "product": product,
                                    "exchange": exchange,
                                    "data_type": data_type,
                                    "index": i,
                                },
                            )
                    elif response.status == 401:
                        # Token expired, try to re-authenticate
                        if await self._authenticate():
                            headers["Authorization"] = f"Bearer {self._access_token}"

        except Exception as e:
            print(f"ICE Connect fetch error: {e}")
