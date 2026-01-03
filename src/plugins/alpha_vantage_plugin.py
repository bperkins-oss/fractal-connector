"""Alpha Vantage market data plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

import aiohttp

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class AlphaVantagePlugin(DataSourcePlugin):
    """Plugin for Alpha Vantage market data."""

    plugin_id = "alpha_vantage"
    plugin_name = "Alpha Vantage"
    plugin_description = "Free stock, forex, and crypto data from Alpha Vantage"
    plugin_icon = "query_stats"

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="api_key",
                label="API Key",
                field_type=FieldType.PASSWORD,
                required=True,
                help_text="Get free key from alphavantage.co",
            ),
            CredentialField(
                name="function",
                label="Data Function",
                field_type=FieldType.SELECT,
                required=True,
                default="TIME_SERIES_DAILY",
                options=[
                    {"value": "TIME_SERIES_INTRADAY", "label": "Intraday"},
                    {"value": "TIME_SERIES_DAILY", "label": "Daily"},
                    {"value": "TIME_SERIES_WEEKLY", "label": "Weekly"},
                    {"value": "TIME_SERIES_MONTHLY", "label": "Monthly"},
                    {"value": "GLOBAL_QUOTE", "label": "Quote"},
                    {"value": "FX_DAILY", "label": "Forex Daily"},
                    {"value": "CRYPTO_DAILY", "label": "Crypto Daily"},
                ],
            ),
            CredentialField(
                name="symbol",
                label="Symbol",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="AAPL, IBM, MSFT",
                help_text="Stock symbol or comma-separated list",
            ),
            CredentialField(
                name="interval",
                label="Interval (Intraday)",
                field_type=FieldType.SELECT,
                required=False,
                default="5min",
                options=[
                    {"value": "1min", "label": "1 Minute"},
                    {"value": "5min", "label": "5 Minutes"},
                    {"value": "15min", "label": "15 Minutes"},
                    {"value": "30min", "label": "30 Minutes"},
                    {"value": "60min", "label": "60 Minutes"},
                ],
            ),
            CredentialField(
                name="outputsize",
                label="Output Size",
                field_type=FieldType.SELECT,
                required=False,
                default="compact",
                options=[
                    {"value": "compact", "label": "Compact (100 points)"},
                    {"value": "full", "label": "Full (all data)"},
                ],
            ),
        ]

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._session = None

    async def connect(self) -> bool:
        self._session = aiohttp.ClientSession()
        self._connected = True
        return True

    async def disconnect(self):
        if self._session:
            await self._session.close()
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        api_key = self.credentials.get("api_key", "")
        if not api_key:
            return False, "API key is required"

        try:
            async with aiohttp.ClientSession() as session:
                url = "https://www.alphavantage.co/query"
                params = {"function": "TIME_SERIES_INTRADAY", "symbol": "IBM", "interval": "5min", "apikey": api_key}
                async with session.get(url, params=params) as response:
                    data = await response.json()
                    if "Error Message" in data:
                        return False, data["Error Message"]
                    if "Note" in data:
                        return False, "API rate limit reached"
                    return True, "Connected to Alpha Vantage"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._session:
            return

        api_key = self.credentials.get("api_key", "")
        function = self.credentials.get("function", "TIME_SERIES_DAILY")
        symbols = [s.strip() for s in self.credentials.get("symbol", "").split(",")]
        interval = self.credentials.get("interval", "5min")
        outputsize = self.credentials.get("outputsize", "compact")

        base_url = "https://www.alphavantage.co/query"

        for symbol in symbols:
            params = {"function": function, "symbol": symbol, "apikey": api_key, "outputsize": outputsize}
            if function == "TIME_SERIES_INTRADAY":
                params["interval"] = interval

            try:
                async with self._session.get(base_url, params=params) as response:
                    data = await response.json()

                    # Find the time series key
                    ts_key = None
                    for key in data:
                        if "Time Series" in key or "Technical" in key:
                            ts_key = key
                            break

                    if ts_key and isinstance(data[ts_key], dict):
                        for timestamp, values in data[ts_key].items():
                            record = {"symbol": symbol, "timestamp": timestamp}
                            for k, v in values.items():
                                clean_key = k.split(". ")[-1] if ". " in k else k
                                record[clean_key] = float(v) if v.replace(".", "").replace("-", "").isdigit() else v

                            yield DataRecord(
                                source_id=self.source_id,
                                source_type=self.plugin_id,
                                timestamp=datetime.utcnow().isoformat(),
                                data=record,
                                metadata={"function": function},
                            )
                    elif "Global Quote" in data:
                        quote = data["Global Quote"]
                        record = {"symbol": symbol}
                        for k, v in quote.items():
                            clean_key = k.split(". ")[-1] if ". " in k else k
                            record[clean_key] = v
                        yield DataRecord(
                            source_id=self.source_id,
                            source_type=self.plugin_id,
                            timestamp=datetime.utcnow().isoformat(),
                            data=record,
                            metadata={"function": function},
                        )

            except Exception as e:
                print(f"Alpha Vantage fetch error for {symbol}: {e}")
