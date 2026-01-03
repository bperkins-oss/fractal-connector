"""Refinitiv (Reuters) Eikon/Workspace data source plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class RefinitivPlugin(DataSourcePlugin):
    """Plugin for Refinitiv Eikon/Workspace API."""

    plugin_id = "refinitiv"
    plugin_name = "Refinitiv Eikon"
    plugin_description = "Connect to Refinitiv Eikon/Workspace for market data"
    plugin_icon = "analytics"

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._ek = None

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="app_key",
                label="Application Key",
                field_type=FieldType.PASSWORD,
                required=True,
                placeholder="Your Refinitiv App Key",
                help_text="Get this from Refinitiv App Key Generator",
            ),
            CredentialField(
                name="rics",
                label="RICs (Reuters Instrument Codes)",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="AAPL.O, MSFT.O, EUR=",
                help_text="Comma-separated list of RICs",
            ),
            CredentialField(
                name="fields",
                label="Fields",
                field_type=FieldType.TEXT,
                required=True,
                default="TR.PriceClose,TR.Open,TR.High,TR.Low,TR.Volume",
                placeholder="TR.PriceClose, TR.Volume",
                help_text="Comma-separated Refinitiv field names",
            ),
            CredentialField(
                name="data_type",
                label="Data Type",
                field_type=FieldType.SELECT,
                required=True,
                default="snapshot",
                options=[
                    {"value": "snapshot", "label": "Snapshot (Current)"},
                    {"value": "timeseries", "label": "Time Series (Historical)"},
                    {"value": "fundamentals", "label": "Fundamentals"},
                    {"value": "news", "label": "News"},
                ],
            ),
            CredentialField(
                name="start_date",
                label="Start Date",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="2024-01-01",
                help_text="For time series data (YYYY-MM-DD)",
            ),
            CredentialField(
                name="end_date",
                label="End Date",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="2024-12-31",
                help_text="For time series data (YYYY-MM-DD)",
            ),
        ]

    async def connect(self) -> bool:
        """Connect to Refinitiv Eikon API."""
        try:
            import eikon as ek

            app_key = self.credentials.get("app_key", "")
            ek.set_app_key(app_key)

            # Test connection with a simple request
            ek.get_data("IBM.N", "TR.CompanyName")

            self._ek = ek
            self._connected = True
            return True

        except ImportError:
            print("Warning: eikon library not installed. Run: pip install eikon")
            return False
        except Exception as e:
            print(f"Refinitiv connection error: {e}")
            return False

    async def disconnect(self):
        """Disconnect from Refinitiv."""
        self._ek = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        """Test Refinitiv connection."""
        app_key = self.credentials.get("app_key", "")
        rics = self.credentials.get("rics", "")

        if not app_key:
            return False, "Application Key is required"
        if not rics:
            return False, "At least one RIC is required"

        try:
            import eikon as ek

            ek.set_app_key(app_key)

            # Test with first RIC
            first_ric = rics.split(",")[0].strip()
            df, err = ek.get_data(first_ric, "TR.CompanyName")

            if err:
                return False, f"Error: {err}"

            return True, f"Connected! Retrieved data for {first_ric}"

        except ImportError:
            return False, "eikon library not installed. Run: pip install eikon"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        """Fetch data from Refinitiv."""
        if not self._ek:
            return

        rics = [r.strip() for r in self.credentials.get("rics", "").split(",")]
        fields = [f.strip() for f in self.credentials.get("fields", "").split(",")]
        data_type = self.credentials.get("data_type", "snapshot")

        try:
            if data_type == "snapshot":
                df, err = self._ek.get_data(rics, fields)
                if df is not None:
                    records = df.to_dict(orient='records')
                    for i, record in enumerate(records):
                        yield DataRecord(
                            source_id=self.source_id,
                            source_type=self.plugin_id,
                            timestamp=datetime.utcnow().isoformat(),
                            data=record,
                            metadata={"data_type": data_type, "index": i},
                        )

            elif data_type == "timeseries":
                start_date = self.credentials.get("start_date", "")
                end_date = self.credentials.get("end_date", "")

                for ric in rics:
                    df = self._ek.get_timeseries(
                        ric,
                        fields=["CLOSE", "OPEN", "HIGH", "LOW", "VOLUME"],
                        start_date=start_date,
                        end_date=end_date,
                    )
                    if df is not None:
                        df['RIC'] = ric
                        records = df.reset_index().to_dict(orient='records')
                        for i, record in enumerate(records):
                            yield DataRecord(
                                source_id=self.source_id,
                                source_type=self.plugin_id,
                                timestamp=datetime.utcnow().isoformat(),
                                data=record,
                                metadata={"ric": ric, "data_type": data_type},
                            )

        except Exception as e:
            print(f"Refinitiv fetch error: {e}")
