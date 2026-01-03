"""Quandl / Nasdaq Data Link data source plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

import aiohttp

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class QuandlPlugin(DataSourcePlugin):
    """Plugin for Quandl / Nasdaq Data Link."""

    plugin_id = "quandl"
    plugin_name = "Nasdaq Data Link (Quandl)"
    plugin_description = "Access financial and economic data from Nasdaq Data Link"
    plugin_icon = "bar_chart"

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._session = None

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="api_key",
                label="API Key",
                field_type=FieldType.PASSWORD,
                required=True,
                placeholder="Your Nasdaq Data Link API key",
                help_text="Get from data.nasdaq.com",
            ),
            CredentialField(
                name="database",
                label="Database Code",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="WIKI, FRED, EOD",
                help_text="Quandl database code",
            ),
            CredentialField(
                name="dataset",
                label="Dataset Code",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="AAPL, GDP, FB",
                help_text="Dataset code within the database",
            ),
            CredentialField(
                name="start_date",
                label="Start Date",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="2020-01-01",
            ),
            CredentialField(
                name="end_date",
                label="End Date",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="2024-12-31",
            ),
            CredentialField(
                name="collapse",
                label="Frequency",
                field_type=FieldType.SELECT,
                required=False,
                default="none",
                options=[
                    {"value": "none", "label": "None (as-is)"},
                    {"value": "daily", "label": "Daily"},
                    {"value": "weekly", "label": "Weekly"},
                    {"value": "monthly", "label": "Monthly"},
                    {"value": "quarterly", "label": "Quarterly"},
                    {"value": "annual", "label": "Annual"},
                ],
            ),
            CredentialField(
                name="transform",
                label="Transform",
                field_type=FieldType.SELECT,
                required=False,
                default="none",
                options=[
                    {"value": "none", "label": "None"},
                    {"value": "diff", "label": "Change"},
                    {"value": "rdiff", "label": "% Change"},
                    {"value": "normalize", "label": "Normalize"},
                    {"value": "cumul", "label": "Cumulative"},
                ],
            ),
        ]

    async def connect(self) -> bool:
        """Connect to Nasdaq Data Link."""
        self._session = aiohttp.ClientSession()
        self._connected = True
        return True

    async def disconnect(self):
        """Disconnect."""
        if self._session:
            await self._session.close()
            self._session = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        """Test connection."""
        api_key = self.credentials.get("api_key", "")
        database = self.credentials.get("database", "")
        dataset = self.credentials.get("dataset", "")

        if not api_key:
            return False, "API Key is required"
        if not database:
            return False, "Database code is required"
        if not dataset:
            return False, "Dataset code is required"

        try:
            url = f"https://data.nasdaq.com/api/v3/datasets/{database}/{dataset}.json"
            params = {"api_key": api_key, "rows": 1}

            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        name = data.get("dataset", {}).get("name", "Unknown")
                        return True, f"Connected! Dataset: {name}"
                    elif response.status == 404:
                        return False, f"Dataset {database}/{dataset} not found"
                    elif response.status == 401:
                        return False, "Invalid API key"
                    else:
                        return False, f"Error: {response.status}"

        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        """Fetch data from Nasdaq Data Link."""
        if not self._session:
            return

        api_key = self.credentials.get("api_key", "")
        database = self.credentials.get("database", "")
        dataset = self.credentials.get("dataset", "")
        start_date = self.credentials.get("start_date", "")
        end_date = self.credentials.get("end_date", "")
        collapse = self.credentials.get("collapse", "none")
        transform = self.credentials.get("transform", "none")

        url = f"https://data.nasdaq.com/api/v3/datasets/{database}/{dataset}.json"
        params = {"api_key": api_key}

        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if collapse != "none":
            params["collapse"] = collapse
        if transform != "none":
            params["transform"] = transform

        try:
            async with self._session.get(url, params=params) as response:
                if response.status == 200:
                    result = await response.json()
                    dataset_info = result.get("dataset", {})
                    column_names = dataset_info.get("column_names", [])
                    data = dataset_info.get("data", [])

                    for i, row in enumerate(data):
                        record = dict(zip(column_names, row))
                        yield DataRecord(
                            source_id=self.source_id,
                            source_type=self.plugin_id,
                            timestamp=datetime.utcnow().isoformat(),
                            data=record,
                            metadata={
                                "database": database,
                                "dataset": dataset,
                                "index": i,
                            },
                        )

        except Exception as e:
            print(f"Quandl fetch error: {e}")
