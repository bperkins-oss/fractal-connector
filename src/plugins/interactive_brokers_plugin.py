"""Interactive Brokers TWS/Gateway data source plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class InteractiveBrokersPlugin(DataSourcePlugin):
    """Plugin for Interactive Brokers TWS/Gateway API."""

    plugin_id = "interactive_brokers"
    plugin_name = "Interactive Brokers"
    plugin_description = "Connect to Interactive Brokers TWS or IB Gateway"
    plugin_icon = "account_balance"

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._ib = None

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="host",
                label="TWS/Gateway Host",
                field_type=FieldType.TEXT,
                required=True,
                default="127.0.0.1",
                placeholder="127.0.0.1",
                help_text="Host running TWS or IB Gateway",
            ),
            CredentialField(
                name="port",
                label="Port",
                field_type=FieldType.NUMBER,
                required=True,
                default="7497",
                placeholder="7497",
                help_text="7497 for TWS paper, 7496 for TWS live, 4002 for Gateway paper, 4001 for Gateway live",
            ),
            CredentialField(
                name="client_id",
                label="Client ID",
                field_type=FieldType.NUMBER,
                required=True,
                default="1",
                placeholder="1",
                help_text="Unique client ID (1-32)",
            ),
            CredentialField(
                name="data_type",
                label="Data Type",
                field_type=FieldType.SELECT,
                required=True,
                default="positions",
                options=[
                    {"value": "positions", "label": "Portfolio Positions"},
                    {"value": "account", "label": "Account Summary"},
                    {"value": "orders", "label": "Open Orders"},
                    {"value": "executions", "label": "Executions/Trades"},
                    {"value": "market_data", "label": "Market Data"},
                    {"value": "historical", "label": "Historical Data"},
                ],
            ),
            CredentialField(
                name="symbols",
                label="Symbols",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="AAPL, MSFT, EUR.USD",
                help_text="For market/historical data - comma-separated symbols",
            ),
            CredentialField(
                name="exchange",
                label="Exchange",
                field_type=FieldType.TEXT,
                required=False,
                default="SMART",
                placeholder="SMART, NYSE, NASDAQ",
            ),
            CredentialField(
                name="sec_type",
                label="Security Type",
                field_type=FieldType.SELECT,
                required=False,
                default="STK",
                options=[
                    {"value": "STK", "label": "Stock"},
                    {"value": "OPT", "label": "Option"},
                    {"value": "FUT", "label": "Future"},
                    {"value": "CASH", "label": "Forex"},
                    {"value": "CFD", "label": "CFD"},
                    {"value": "IND", "label": "Index"},
                ],
            ),
            CredentialField(
                name="currency",
                label="Currency",
                field_type=FieldType.TEXT,
                required=False,
                default="USD",
                placeholder="USD",
            ),
        ]

    async def connect(self) -> bool:
        """Connect to Interactive Brokers TWS/Gateway."""
        try:
            from ib_insync import IB

            host = self.credentials.get("host", "127.0.0.1")
            port = int(self.credentials.get("port", 7497))
            client_id = int(self.credentials.get("client_id", 1))

            self._ib = IB()
            self._ib.connect(host, port, clientId=client_id)

            self._connected = True
            return True

        except ImportError:
            print("Warning: ib_insync library not installed. Run: pip install ib_insync")
            return False
        except Exception as e:
            print(f"IB connection error: {e}")
            return False

    async def disconnect(self):
        """Disconnect from Interactive Brokers."""
        if self._ib and self._ib.isConnected():
            self._ib.disconnect()
        self._ib = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        """Test Interactive Brokers connection."""
        host = self.credentials.get("host", "127.0.0.1")
        port = self.credentials.get("port", "7497")

        try:
            from ib_insync import IB

            ib = IB()
            ib.connect(host, int(port), clientId=999, timeout=10)

            if ib.isConnected():
                accounts = ib.managedAccounts()
                ib.disconnect()
                return True, f"Connected! Accounts: {', '.join(accounts)}"
            else:
                return False, "Failed to connect"

        except ImportError:
            return False, "ib_insync library not installed. Run: pip install ib_insync"
        except Exception as e:
            if "actively refused" in str(e):
                return False, f"Cannot connect to TWS/Gateway at {host}:{port}. Ensure it's running."
            return False, f"Connection failed: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        """Fetch data from Interactive Brokers."""
        if not self._ib or not self._ib.isConnected():
            return

        from ib_insync import Stock, Forex, Future, Option, Contract

        data_type = self.credentials.get("data_type", "positions")
        symbols = [s.strip() for s in self.credentials.get("symbols", "").split(",") if s.strip()]
        exchange = self.credentials.get("exchange", "SMART")
        sec_type = self.credentials.get("sec_type", "STK")
        currency = self.credentials.get("currency", "USD")

        try:
            if data_type == "positions":
                positions = self._ib.positions()
                for pos in positions:
                    yield DataRecord(
                        source_id=self.source_id,
                        source_type=self.plugin_id,
                        timestamp=datetime.utcnow().isoformat(),
                        data={
                            "account": pos.account,
                            "symbol": pos.contract.symbol,
                            "sec_type": pos.contract.secType,
                            "exchange": pos.contract.exchange,
                            "currency": pos.contract.currency,
                            "position": pos.position,
                            "avg_cost": pos.avgCost,
                        },
                        metadata={"data_type": "position"},
                    )

            elif data_type == "account":
                account_values = self._ib.accountSummary()
                for av in account_values:
                    yield DataRecord(
                        source_id=self.source_id,
                        source_type=self.plugin_id,
                        timestamp=datetime.utcnow().isoformat(),
                        data={
                            "account": av.account,
                            "tag": av.tag,
                            "value": av.value,
                            "currency": av.currency,
                        },
                        metadata={"data_type": "account"},
                    )

            elif data_type == "orders":
                orders = self._ib.openOrders()
                for order in orders:
                    yield DataRecord(
                        source_id=self.source_id,
                        source_type=self.plugin_id,
                        timestamp=datetime.utcnow().isoformat(),
                        data={
                            "order_id": order.orderId,
                            "action": order.action,
                            "quantity": order.totalQuantity,
                            "order_type": order.orderType,
                            "limit_price": order.lmtPrice,
                            "status": order.status,
                        },
                        metadata={"data_type": "order"},
                    )

            elif data_type == "market_data" and symbols:
                for symbol in symbols:
                    if sec_type == "STK":
                        contract = Stock(symbol, exchange, currency)
                    elif sec_type == "CASH":
                        contract = Forex(symbol)
                    else:
                        contract = Contract(symbol=symbol, secType=sec_type, exchange=exchange, currency=currency)

                    self._ib.qualifyContracts(contract)
                    ticker = self._ib.reqMktData(contract)
                    self._ib.sleep(2)  # Wait for data

                    yield DataRecord(
                        source_id=self.source_id,
                        source_type=self.plugin_id,
                        timestamp=datetime.utcnow().isoformat(),
                        data={
                            "symbol": symbol,
                            "bid": ticker.bid,
                            "ask": ticker.ask,
                            "last": ticker.last,
                            "volume": ticker.volume,
                            "high": ticker.high,
                            "low": ticker.low,
                        },
                        metadata={"data_type": "market_data"},
                    )

                    self._ib.cancelMktData(contract)

        except Exception as e:
            print(f"IB fetch error: {e}")
