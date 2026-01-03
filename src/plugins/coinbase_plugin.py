"""Coinbase cryptocurrency exchange plugin."""
from datetime import datetime
from typing import Any, AsyncIterator
import hmac
import hashlib
import time

import aiohttp

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class CoinbasePlugin(DataSourcePlugin):
    """Plugin for Coinbase exchange data."""

    plugin_id = "coinbase"
    plugin_name = "Coinbase"
    plugin_description = "Connect to Coinbase for crypto market data"
    plugin_icon = "paid"

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
                help_text="Optional for public data",
            ),
            CredentialField(
                name="api_secret",
                label="API Secret",
                field_type=FieldType.PASSWORD,
                required=False,
            ),
            CredentialField(
                name="data_type",
                label="Data Type",
                field_type=FieldType.SELECT,
                required=True,
                default="ticker",
                options=[
                    {"value": "ticker", "label": "Ticker/Price"},
                    {"value": "candles", "label": "Candlesticks"},
                    {"value": "trades", "label": "Recent Trades"},
                    {"value": "orderbook", "label": "Order Book"},
                    {"value": "products", "label": "Available Products"},
                ],
            ),
            CredentialField(
                name="products",
                label="Product IDs",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="BTC-USD, ETH-USD",
                help_text="Comma-separated product IDs",
            ),
            CredentialField(
                name="granularity",
                label="Candle Granularity",
                field_type=FieldType.SELECT,
                required=False,
                default="3600",
                options=[
                    {"value": "60", "label": "1 Minute"},
                    {"value": "300", "label": "5 Minutes"},
                    {"value": "900", "label": "15 Minutes"},
                    {"value": "3600", "label": "1 Hour"},
                    {"value": "21600", "label": "6 Hours"},
                    {"value": "86400", "label": "1 Day"},
                ],
            ),
        ]

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
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.exchange.coinbase.com/products") as response:
                    if response.status == 200:
                        products = await response.json()
                        return True, f"Connected! {len(products)} products available"
                    return False, f"API error: {response.status}"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._session:
            return

        base_url = "https://api.exchange.coinbase.com"
        data_type = self.credentials.get("data_type", "ticker")
        products = [p.strip().upper() for p in self.credentials.get("products", "").split(",")]
        granularity = self.credentials.get("granularity", "3600")

        try:
            for product in products:
                if data_type == "ticker":
                    url = f"{base_url}/products/{product}/ticker"
                elif data_type == "candles":
                    url = f"{base_url}/products/{product}/candles"
                    params = {"granularity": granularity}
                elif data_type == "trades":
                    url = f"{base_url}/products/{product}/trades"
                elif data_type == "orderbook":
                    url = f"{base_url}/products/{product}/book"
                    params = {"level": 2}
                else:
                    continue

                params = params if 'params' in dir() else {}
                async with self._session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        if data_type == "candles" and isinstance(data, list):
                            for candle in data:
                                yield DataRecord(
                                    source_id=self.source_id,
                                    source_type=self.plugin_id,
                                    timestamp=datetime.utcnow().isoformat(),
                                    data={
                                        "product": product,
                                        "time": candle[0],
                                        "low": candle[1],
                                        "high": candle[2],
                                        "open": candle[3],
                                        "close": candle[4],
                                        "volume": candle[5],
                                    },
                                    metadata={"data_type": data_type},
                                )
                        elif isinstance(data, list):
                            for item in data:
                                yield DataRecord(
                                    source_id=self.source_id,
                                    source_type=self.plugin_id,
                                    timestamp=datetime.utcnow().isoformat(),
                                    data={"product": product, **item} if isinstance(item, dict) else {"product": product, "value": item},
                                    metadata={"data_type": data_type},
                                )
                        else:
                            yield DataRecord(
                                source_id=self.source_id,
                                source_type=self.plugin_id,
                                timestamp=datetime.utcnow().isoformat(),
                                data={"product": product, **data},
                                metadata={"data_type": data_type},
                            )

        except Exception as e:
            print(f"Coinbase fetch error: {e}")
