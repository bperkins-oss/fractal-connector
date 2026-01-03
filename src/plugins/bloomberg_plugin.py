"""Bloomberg Terminal data source plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class BloombergPlugin(DataSourcePlugin):
    """Plugin for connecting to Bloomberg Terminal via BLPAPI."""

    plugin_id = "bloomberg"
    plugin_name = "Bloomberg Terminal"
    plugin_description = "Connect to Bloomberg Terminal for market data"
    plugin_icon = "show_chart"

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._session = None
        self._service = None

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="host",
                label="Bloomberg Server Host",
                field_type=FieldType.TEXT,
                required=True,
                default="localhost",
                placeholder="localhost",
                help_text="Bloomberg API server host",
            ),
            CredentialField(
                name="port",
                label="Port",
                field_type=FieldType.NUMBER,
                required=True,
                default="8194",
                placeholder="8194",
                help_text="Bloomberg API port (default: 8194)",
            ),
            CredentialField(
                name="service",
                label="Service",
                field_type=FieldType.SELECT,
                required=True,
                default="//blp/refdata",
                options=[
                    {"value": "//blp/refdata", "label": "Reference Data"},
                    {"value": "//blp/mktdata", "label": "Market Data (Real-time)"},
                    {"value": "//blp/mktbar", "label": "Market Bars"},
                    {"value": "//blp/apiauth", "label": "API Authentication"},
                ],
            ),
            CredentialField(
                name="securities",
                label="Securities",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="AAPL US Equity, MSFT US Equity",
                help_text="Comma-separated list of securities",
            ),
            CredentialField(
                name="fields",
                label="Fields",
                field_type=FieldType.TEXT,
                required=True,
                default="PX_LAST,PX_OPEN,PX_HIGH,PX_LOW,VOLUME",
                placeholder="PX_LAST, PX_OPEN, PX_HIGH",
                help_text="Comma-separated list of Bloomberg fields",
            ),
            CredentialField(
                name="app_name",
                label="Application Name",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="FractalConnector",
                help_text="Optional application identifier",
            ),
        ]

    async def connect(self) -> bool:
        """Connect to Bloomberg API."""
        try:
            # Import blpapi - will fail if not installed
            import blpapi

            host = self.credentials.get("host", "localhost")
            port = int(self.credentials.get("port", 8194))

            # Create session options
            session_options = blpapi.SessionOptions()
            session_options.setServerHost(host)
            session_options.setServerPort(port)

            app_name = self.credentials.get("app_name")
            if app_name:
                session_options.setAuthenticationOptions(
                    f"AuthenticationType=APPLICATION;ApplicationName={app_name}"
                )

            # Create and start session
            self._session = blpapi.Session(session_options)

            if not self._session.start():
                return False

            # Open service
            service_name = self.credentials.get("service", "//blp/refdata")
            if not self._session.openService(service_name):
                self._session.stop()
                return False

            self._service = self._session.getService(service_name)
            self._connected = True
            return True

        except ImportError:
            # blpapi not installed - log warning
            print("Warning: blpapi not installed. Install with: pip install blpapi")
            return False
        except Exception as e:
            print(f"Bloomberg connection error: {e}")
            return False

    async def disconnect(self):
        """Disconnect from Bloomberg API."""
        if self._session:
            try:
                self._session.stop()
            except:
                pass
            self._session = None
            self._service = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        """Test Bloomberg connection."""
        host = self.credentials.get("host", "localhost")
        port = self.credentials.get("port", "8194")
        securities = self.credentials.get("securities", "")
        fields = self.credentials.get("fields", "")

        if not securities:
            return False, "At least one security is required"
        if not fields:
            return False, "At least one field is required"

        try:
            import blpapi

            session_options = blpapi.SessionOptions()
            session_options.setServerHost(host)
            session_options.setServerPort(int(port))

            session = blpapi.Session(session_options)

            if not session.start():
                return False, f"Cannot connect to Bloomberg at {host}:{port}"

            service_name = self.credentials.get("service", "//blp/refdata")
            if not session.openService(service_name):
                session.stop()
                return False, f"Cannot open service: {service_name}"

            session.stop()
            return True, f"Connected to Bloomberg at {host}:{port}"

        except ImportError:
            return False, "blpapi library not installed. Run: pip install blpapi"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        """Fetch data from Bloomberg."""
        if not self._session or not self._service:
            return

        try:
            import blpapi

            securities = [s.strip() for s in self.credentials.get("securities", "").split(",")]
            fields = [f.strip() for f in self.credentials.get("fields", "").split(",")]

            # Create reference data request
            request = self._service.createRequest("ReferenceDataRequest")

            for security in securities:
                request.append("securities", security)

            for field in fields:
                request.append("fields", field)

            # Send request
            self._session.sendRequest(request)

            # Process responses
            while True:
                event = self._session.nextEvent(500)

                for msg in event:
                    if msg.hasElement("securityData"):
                        security_data = msg.getElement("securityData")

                        for i in range(security_data.numValues()):
                            security = security_data.getValueAsElement(i)
                            security_name = security.getElementAsString("security")

                            field_data = security.getElement("fieldData")
                            record_data = {"security": security_name}

                            for field in fields:
                                if field_data.hasElement(field):
                                    value = field_data.getElement(field)
                                    record_data[field] = self._convert_value(value)

                            yield DataRecord(
                                source_id=self.source_id,
                                source_type=self.plugin_id,
                                timestamp=datetime.utcnow().isoformat(),
                                data=record_data,
                                metadata={"service": self.credentials.get("service")},
                            )

                if event.eventType() == blpapi.Event.RESPONSE:
                    break

        except Exception as e:
            print(f"Bloomberg fetch error: {e}")

    def _convert_value(self, element) -> Any:
        """Convert Bloomberg element to Python value."""
        try:
            import blpapi

            if element.datatype() == blpapi.DataType.FLOAT64:
                return element.getValueAsFloat()
            elif element.datatype() == blpapi.DataType.INT32:
                return element.getValueAsInteger()
            elif element.datatype() == blpapi.DataType.STRING:
                return element.getValueAsString()
            elif element.datatype() == blpapi.DataType.DATE:
                return str(element.getValueAsDatetime())
            else:
                return str(element)
        except:
            return None
