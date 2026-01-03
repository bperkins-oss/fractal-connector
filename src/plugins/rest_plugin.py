"""REST API data source plugin."""
import asyncio
from datetime import datetime
from typing import Any, AsyncIterator

import aiohttp

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class RESTPlugin(DataSourcePlugin):
    """Plugin for fetching data from REST APIs."""

    plugin_id = "rest_api"
    plugin_name = "REST API"
    plugin_description = "Connect to any REST API endpoint"
    plugin_icon = "api"

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._session = None

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="base_url",
                label="API Base URL",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="https://api.example.com",
                help_text="Base URL of the API",
            ),
            CredentialField(
                name="endpoint",
                label="Endpoint Path",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="/v1/data",
                help_text="API endpoint to fetch data from",
            ),
            CredentialField(
                name="auth_type",
                label="Authentication Type",
                field_type=FieldType.SELECT,
                required=True,
                default="none",
                options=[
                    {"value": "none", "label": "None"},
                    {"value": "api_key", "label": "API Key"},
                    {"value": "bearer", "label": "Bearer Token"},
                    {"value": "basic", "label": "Basic Auth"},
                ],
            ),
            CredentialField(
                name="api_key",
                label="API Key / Token",
                field_type=FieldType.PASSWORD,
                required=False,
                placeholder="Your API key or token",
                help_text="API key or bearer token for authentication",
            ),
            CredentialField(
                name="api_key_header",
                label="API Key Header Name",
                field_type=FieldType.TEXT,
                required=False,
                default="X-API-Key",
                placeholder="X-API-Key",
                help_text="Header name for API key (if using API Key auth)",
            ),
            CredentialField(
                name="username",
                label="Username",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="Username for Basic Auth",
            ),
            CredentialField(
                name="password",
                label="Password",
                field_type=FieldType.PASSWORD,
                required=False,
                placeholder="Password for Basic Auth",
            ),
        ]

    def _build_headers(self) -> dict[str, str]:
        """Build request headers based on auth type."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        auth_type = self.credentials.get("auth_type", "none")
        api_key = self.credentials.get("api_key", "")

        if auth_type == "api_key" and api_key:
            header_name = self.credentials.get("api_key_header", "X-API-Key")
            headers[header_name] = api_key
        elif auth_type == "bearer" and api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        return headers

    def _build_auth(self) -> aiohttp.BasicAuth | None:
        """Build basic auth if configured."""
        auth_type = self.credentials.get("auth_type", "none")

        if auth_type == "basic":
            username = self.credentials.get("username", "")
            password = self.credentials.get("password", "")
            if username:
                return aiohttp.BasicAuth(username, password)

        return None

    async def connect(self) -> bool:
        """Create HTTP session."""
        self._session = aiohttp.ClientSession()
        self._connected = True
        return True

    async def disconnect(self):
        """Close HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        """Test the API connection."""
        base_url = self.credentials.get("base_url", "").rstrip("/")
        endpoint = self.credentials.get("endpoint", "")

        if not base_url:
            return False, "Base URL is required"
        if not endpoint:
            return False, "Endpoint is required"

        url = f"{base_url}{endpoint}"

        try:
            async with aiohttp.ClientSession() as session:
                headers = self._build_headers()
                auth = self._build_auth()

                async with session.get(url, headers=headers, auth=auth, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if isinstance(data, list):
                            return True, f"Success! Received {len(data)} records"
                        else:
                            return True, "Success! Connected to API"
                    elif response.status == 401:
                        return False, "Authentication failed (401)"
                    elif response.status == 403:
                        return False, "Access forbidden (403)"
                    elif response.status == 404:
                        return False, "Endpoint not found (404)"
                    else:
                        return False, f"HTTP {response.status}: {response.reason}"

        except asyncio.TimeoutError:
            return False, "Connection timeout"
        except aiohttp.ClientError as e:
            return False, f"Connection error: {str(e)}"
        except Exception as e:
            return False, f"Error: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        """Fetch data from the API."""
        base_url = self.credentials.get("base_url", "").rstrip("/")
        endpoint = self.credentials.get("endpoint", "")
        url = f"{base_url}{endpoint}"

        try:
            headers = self._build_headers()
            auth = self._build_auth()

            async with self._session.get(url, headers=headers, auth=auth) as response:
                if response.status == 200:
                    data = await response.json()

                    # Handle both array and object responses
                    if isinstance(data, list):
                        records = data
                    elif isinstance(data, dict):
                        # Try common patterns for nested data
                        for key in ['data', 'results', 'items', 'records']:
                            if key in data and isinstance(data[key], list):
                                records = data[key]
                                break
                        else:
                            records = [data]
                    else:
                        records = []

                    for i, record in enumerate(records):
                        yield DataRecord(
                            source_id=self.source_id,
                            source_type=self.plugin_id,
                            timestamp=datetime.utcnow().isoformat(),
                            data=record if isinstance(record, dict) else {"value": record},
                            metadata={
                                "endpoint": endpoint,
                                "index": i,
                                "total": len(records),
                            }
                        )

        except Exception as e:
            print(f"Error fetching from API: {e}")
