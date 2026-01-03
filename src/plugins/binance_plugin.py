"""Binance cryptocurrency exchange plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

import aiohttp

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class BinancePlugin(DataSourcePlugin):
    """Plugin for Binance exchange data."""

    plugin_id = "binance"
    plugin_name = "Binance"
    plugin_description = "Connect to Binance for crypto market data"
    plugin_icon = "currency_bitcoin"

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
                required=False,
                help_text="Optional - needed for account data, not for public market data",
            ),
            CredentialField(
                name="api_secret",
                label="API Secret",
                field_type=FieldType.PASSWORD,
                required=False,
            ),
            CredentialField(
                name="testnet",
                label="Use Testnet",
                field_type=FieldType.CHECKBOX,
                required=False,
                default=False,
            ),
            CredentialField(
                name="data_type",
                label="Data Type",
                field_type=FieldType.SELECT,
                required=True,
                default="ticker",
                options=[
                    {"value": "ticker", "label": "24h Ticker Price"},
                    {"value": "klines", "label": "Candlestick/Klines"},
                    {"value": "depth", "label": "Order Book Depth"},
                    {"value": "trades", "label": "Recent Trades"},
                    {"value": "account", "label": "Account Balances"},
                ],
            ),
            CredentialField(
                name="symbols",
                label="Trading Pairs",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="BTCUSDT, ETHUSDT",
                help_text="Comma-separated trading pairs",
            ),
            CredentialField(
                name="interval",
                label="Kline Interval",
                field_type=FieldType.SELECT,
                required=False,
                default="1h",
                options=[
                    {"value": "1m", "label": "1 Minute"},
                    {"value": "5m", "label": "5 Minutes"},
                    {"value": "15m", "label": "15 Minutes"},
                    {"value": "1h", "label": "1 Hour"},
                    {"value": "4h", "label": "4 Hours"},
                    {"value": "1d", "label": "1 Day"},
                ],
            ),
            CredentialField(
                name="limit",
                label="Limit",
                field_type=FieldType.NUMBER,
                required=False,
                default="100",
                placeholder="100",
            ),
        ]

    def _get_base_url(self) -> str:
        testnet = self.credentials.get("testnet", False)
        if testnet:
            return "https://testnet.binance.vision/api/v3"
        return "https://api.binance.com/api/v3"

    async def connect(self) -> bool:
        self._session = aiohttp.ClientSession()
        self._connected = True
        return True

    async def disconnect(self):
        if self._session:
            await self._session.close()
            self._session = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        symbols = self.credentials.get("symbols", "")
        if not symbols:
            return False, "At least one trading pair is required"

        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self._get_base_url()}/ping"
                async with session.get(url) as response:
                    if response.status == 200:
                        return True, "Connected to Binance API"
                    return False, f"API error: {response.status}"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._session:
            return

        base_url = self._get_base_url()
        data_type = self.credentials.get("data_type", "ticker")
        symbols = [s.strip().upper() for s in self.credentials.get("symbols", "").split(",")]
        interval = self.credentials.get("interval", "1h")
        limit = int(self.credentials.get("limit", 100) or 100)

        try:
            for symbol in symbols:
                if data_type == "ticker":
                    url = f"{base_url}/ticker/24hr"
                    params = {"symbol": symbol}
                elif data_type == "klines":
                    url = f"{base_url}/klines"
                    params = {"symbol": symbol, "interval": interval, "limit": limit}
                elif data_type == "depth":
                    url = f"{base_url}/depth"
                    params = {"symbol": symbol, "limit": min(limit, 1000)}
                elif data_type == "trades":
                    url = f"{base_url}/trades"
                    params = {"symbol": symbol, "limit": min(limit, 1000)}
                else:
                    continue

                async with self._session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        if data_type == "klines":
                            for i, kline in enumerate(data):
                                yield DataRecord(
                                    source_id=self.source_id,
                                    source_type=self.plugin_id,
                                    timestamp=datetime.utcnow().isoformat(),
                                    data={
                                        "symbol": symbol,
                                        "open_time": kline[0],
                                        "open": float(kline[1]),
                                        "high": float(kline[2]),
                                        "low": float(kline[3]),
                                        "close": float(kline[4]),
                                        "volume": float(kline[5]),
                                        "close_time": kline[6],
                                    },
                                    metadata={"data_type": data_type, "interval": interval},
                                )
                        elif isinstance(data, list):
                            for i, item in enumerate(data):
                                yield DataRecord(
                                    source_id=self.source_id,
                                    source_type=self.plugin_id,
                                    timestamp=datetime.utcnow().isoformat(),
                                    data={"symbol": symbol, **item} if isinstance(item, dict) else {"symbol": symbol, "value": item},
                                    metadata={"data_type": data_type},
                                )
                        else:
                            yield DataRecord(
                                source_id=self.source_id,
                                source_type=self.plugin_id,
                                timestamp=datetime.utcnow().isoformat(),
                                data={"symbol": symbol, **data},
                                metadata={"data_type": data_type},
                            )

        except Exception as e:
            print(f"Binance fetch error: {e}")
