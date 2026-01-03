"""FactSet data source plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

import aiohttp

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class FactSetPlugin(DataSourcePlugin):
    """Plugin for FactSet Data API."""

    plugin_id = "factset"
    plugin_name = "FactSet"
    plugin_description = "Connect to FactSet data services"
    plugin_icon = "insights"

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._session = None

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="username",
                label="FactSet Username",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="Your FactSet username",
            ),
            CredentialField(
                name="api_key",
                label="API Key",
                field_type=FieldType.PASSWORD,
                required=True,
                placeholder="Your FactSet API key",
            ),
            CredentialField(
                name="base_url",
                label="API Base URL",
                field_type=FieldType.TEXT,
                required=False,
                default="https://api.factset.com",
                placeholder="https://api.factset.com",
            ),
            CredentialField(
                name="data_type",
                label="Data Type",
                field_type=FieldType.SELECT,
                required=True,
                default="prices",
                options=[
                    {"value": "prices", "label": "Prices & Returns"},
                    {"value": "fundamentals", "label": "Fundamentals"},
                    {"value": "estimates", "label": "Estimates"},
                    {"value": "ownership", "label": "Ownership"},
                    {"value": "transcripts", "label": "Transcripts"},
                ],
            ),
            CredentialField(
                name="ids",
                label="Security IDs",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="AAPL-US, MSFT-US",
                help_text="Comma-separated FactSet security IDs",
            ),
            CredentialField(
                name="fields",
                label="Data Fields",
                field_type=FieldType.TEXT,
                required=False,
                default="price,priceOpen,priceHigh,priceLow,volume",
                placeholder="price,volume,marketCap",
                help_text="Comma-separated field names",
            ),
            CredentialField(
                name="start_date",
                label="Start Date",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="2024-01-01",
            ),
            CredentialField(
                name="end_date",
                label="End Date",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="2024-12-31",
            ),
        ]

    async def connect(self) -> bool:
        """Connect to FactSet API."""
        self._session = aiohttp.ClientSession()
        self._connected = True
        return True

    async def disconnect(self):
        """Disconnect from FactSet."""
        if self._session:
            await self._session.close()
            self._session = None
        self._connected = False

    def _get_auth(self) -> aiohttp.BasicAuth:
        """Get authentication for requests."""
        username = self.credentials.get("username", "")
        api_key = self.credentials.get("api_key", "")
        return aiohttp.BasicAuth(f"{username}-api", api_key)

    async def test_connection(self) -> tuple[bool, str]:
        """Test FactSet connection."""
        username = self.credentials.get("username", "")
        api_key = self.credentials.get("api_key", "")
        base_url = self.credentials.get("base_url", "https://api.factset.com")
        ids = self.credentials.get("ids", "")

        if not username:
            return False, "Username is required"
        if not api_key:
            return False, "API Key is required"
        if not ids:
            return False, "At least one security ID is required"

        try:
            async with aiohttp.ClientSession() as session:
                # Test with prices endpoint
                url = f"{base_url}/content/factset-prices/v1/prices"
                auth = aiohttp.BasicAuth(f"{username}-api", api_key)

                first_id = ids.split(",")[0].strip()
                payload = {"ids": [first_id], "fields": ["price"]}

                async with session.post(url, json=payload, auth=auth) as response:
                    if response.status == 200:
                        return True, f"Connected to FactSet"
                    elif response.status == 401:
                        return False, "Authentication failed"
                    else:
                        text = await response.text()
                        return False, f"Error {response.status}: {text[:100]}"

        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        """Fetch data from FactSet."""
        if not self._session:
            return

        base_url = self.credentials.get("base_url", "https://api.factset.com")
        data_type = self.credentials.get("data_type", "prices")
        ids = [i.strip() for i in self.credentials.get("ids", "").split(",")]
        fields = [f.strip() for f in self.credentials.get("fields", "").split(",")]
        start_date = self.credentials.get("start_date", "")
        end_date = self.credentials.get("end_date", "")

        auth = self._get_auth()

        # Map data types to endpoints
        endpoints = {
            "prices": "/content/factset-prices/v1/prices",
            "fundamentals": "/content/factset-fundamentals/v1/fundamentals",
            "estimates": "/content/factset-estimates/v1/consensus",
            "ownership": "/content/factset-ownership/v1/holders",
            "transcripts": "/content/factset-callstreet/v1/transcripts",
        }

        endpoint = endpoints.get(data_type, "/content/factset-prices/v1/prices")
        url = f"{base_url}{endpoint}"

        try:
            payload = {"ids": ids, "fields": fields}
            if start_date:
                payload["startDate"] = start_date
            if end_date:
                payload["endDate"] = end_date

            async with self._session.post(url, json=payload, auth=auth) as response:
                if response.status == 200:
                    data = await response.json()

                    # Handle FactSet response format
                    records = data.get("data", [])
                    for i, record in enumerate(records):
                        yield DataRecord(
                            source_id=self.source_id,
                            source_type=self.plugin_id,
                            timestamp=datetime.utcnow().isoformat(),
                            data=record,
                            metadata={"data_type": data_type, "index": i},
                        )

        except Exception as e:
            print(f"FactSet fetch error: {e}")
