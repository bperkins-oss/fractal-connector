"""Yahoo Finance data plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class YahooFinancePlugin(DataSourcePlugin):
    """Plugin for Yahoo Finance data via yfinance."""

    plugin_id = "yahoo_finance"
    plugin_name = "Yahoo Finance"
    plugin_description = "Free stock data from Yahoo Finance"
    plugin_icon = "trending_up"

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="symbols",
                label="Symbols",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="AAPL, MSFT, GOOGL",
                help_text="Comma-separated stock symbols",
            ),
            CredentialField(
                name="data_type",
                label="Data Type",
                field_type=FieldType.SELECT,
                required=True,
                default="history",
                options=[
                    {"value": "history", "label": "Historical Prices"},
                    {"value": "info", "label": "Company Info"},
                    {"value": "financials", "label": "Financial Statements"},
                    {"value": "holders", "label": "Institutional Holders"},
                    {"value": "recommendations", "label": "Analyst Recommendations"},
                ],
            ),
            CredentialField(
                name="period",
                label="Period",
                field_type=FieldType.SELECT,
                required=False,
                default="1mo",
                options=[
                    {"value": "1d", "label": "1 Day"},
                    {"value": "5d", "label": "5 Days"},
                    {"value": "1mo", "label": "1 Month"},
                    {"value": "3mo", "label": "3 Months"},
                    {"value": "6mo", "label": "6 Months"},
                    {"value": "1y", "label": "1 Year"},
                    {"value": "2y", "label": "2 Years"},
                    {"value": "5y", "label": "5 Years"},
                    {"value": "max", "label": "Max"},
                ],
            ),
            CredentialField(
                name="interval",
                label="Interval",
                field_type=FieldType.SELECT,
                required=False,
                default="1d",
                options=[
                    {"value": "1m", "label": "1 Minute"},
                    {"value": "5m", "label": "5 Minutes"},
                    {"value": "15m", "label": "15 Minutes"},
                    {"value": "1h", "label": "1 Hour"},
                    {"value": "1d", "label": "1 Day"},
                    {"value": "1wk", "label": "1 Week"},
                    {"value": "1mo", "label": "1 Month"},
                ],
            ),
        ]

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._yf = None

    async def connect(self) -> bool:
        try:
            import yfinance as yf
            self._yf = yf
            self._connected = True
            return True
        except ImportError:
            print("yfinance not installed. Run: pip install yfinance")
            return False

    async def disconnect(self):
        self._yf = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        symbols = self.credentials.get("symbols", "")
        if not symbols:
            return False, "At least one symbol is required"

        try:
            import yfinance as yf
            first_symbol = symbols.split(",")[0].strip()
            ticker = yf.Ticker(first_symbol)
            info = ticker.info
            if info:
                name = info.get("shortName", first_symbol)
                return True, f"Connected! Found: {name}"
            return False, "Could not fetch data"
        except ImportError:
            return False, "yfinance not installed. Run: pip install yfinance"
        except Exception as e:
            return False, f"Error: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._yf:
            return

        symbols = [s.strip() for s in self.credentials.get("symbols", "").split(",")]
        data_type = self.credentials.get("data_type", "history")
        period = self.credentials.get("period", "1mo")
        interval = self.credentials.get("interval", "1d")

        for symbol in symbols:
            try:
                ticker = self._yf.Ticker(symbol)

                if data_type == "history":
                    hist = ticker.history(period=period, interval=interval)
                    for idx, row in hist.iterrows():
                        yield DataRecord(
                            source_id=self.source_id,
                            source_type=self.plugin_id,
                            timestamp=datetime.utcnow().isoformat(),
                            data={
                                "symbol": symbol,
                                "date": str(idx),
                                "open": row.get("Open"),
                                "high": row.get("High"),
                                "low": row.get("Low"),
                                "close": row.get("Close"),
                                "volume": row.get("Volume"),
                            },
                            metadata={"data_type": data_type},
                        )

                elif data_type == "info":
                    info = ticker.info
                    yield DataRecord(
                        source_id=self.source_id,
                        source_type=self.plugin_id,
                        timestamp=datetime.utcnow().isoformat(),
                        data={"symbol": symbol, **info},
                        metadata={"data_type": data_type},
                    )

                elif data_type == "financials":
                    for period_name, df in [("annual", ticker.financials), ("quarterly", ticker.quarterly_financials)]:
                        if df is not None and not df.empty:
                            for col in df.columns:
                                record = {"symbol": symbol, "period": period_name, "date": str(col)}
                                for idx, value in df[col].items():
                                    record[str(idx)] = value
                                yield DataRecord(
                                    source_id=self.source_id,
                                    source_type=self.plugin_id,
                                    timestamp=datetime.utcnow().isoformat(),
                                    data=record,
                                    metadata={"data_type": data_type},
                                )

                elif data_type == "holders":
                    holders = ticker.institutional_holders
                    if holders is not None:
                        for _, row in holders.iterrows():
                            yield DataRecord(
                                source_id=self.source_id,
                                source_type=self.plugin_id,
                                timestamp=datetime.utcnow().isoformat(),
                                data={"symbol": symbol, **row.to_dict()},
                                metadata={"data_type": data_type},
                            )

                elif data_type == "recommendations":
                    recs = ticker.recommendations
                    if recs is not None:
                        for idx, row in recs.iterrows():
                            yield DataRecord(
                                source_id=self.source_id,
                                source_type=self.plugin_id,
                                timestamp=datetime.utcnow().isoformat(),
                                data={"symbol": symbol, "date": str(idx), **row.to_dict()},
                                metadata={"data_type": data_type},
                            )

            except Exception as e:
                print(f"Yahoo Finance error for {symbol}: {e}")
