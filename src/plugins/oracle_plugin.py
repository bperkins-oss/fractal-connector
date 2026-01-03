"""Oracle Database plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class OraclePlugin(DataSourcePlugin):
    """Plugin for Oracle Database."""

    plugin_id = "oracle"
    plugin_name = "Oracle Database"
    plugin_description = "Connect to Oracle databases"
    plugin_icon = "database"

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="connection_type",
                label="Connection Type",
                field_type=FieldType.SELECT,
                required=True,
                default="basic",
                options=[
                    {"value": "basic", "label": "Basic (Host/Port/Service)"},
                    {"value": "dsn", "label": "DSN/TNS Name"},
                    {"value": "connection_string", "label": "Connection String"},
                ],
            ),
            CredentialField(
                name="host",
                label="Host",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="localhost",
            ),
            CredentialField(
                name="port",
                label="Port",
                field_type=FieldType.NUMBER,
                required=False,
                default="1521",
            ),
            CredentialField(
                name="service_name",
                label="Service Name",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="ORCL",
            ),
            CredentialField(
                name="dsn",
                label="DSN / TNS Name",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="mydb_tns",
                help_text="For DSN connection type",
            ),
            CredentialField(
                name="connection_string",
                label="Connection String",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="user/pass@host:port/service",
            ),
            CredentialField(
                name="username",
                label="Username",
                field_type=FieldType.TEXT,
                required=True,
            ),
            CredentialField(
                name="password",
                label="Password",
                field_type=FieldType.PASSWORD,
                required=True,
            ),
            CredentialField(
                name="query",
                label="SQL Query",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="SELECT * FROM my_table",
            ),
        ]

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._conn = None

    async def connect(self) -> bool:
        try:
            import oracledb

            conn_type = self.credentials.get("connection_type", "basic")
            username = self.credentials.get("username", "")
            password = self.credentials.get("password", "")

            if conn_type == "basic":
                host = self.credentials.get("host", "localhost")
                port = int(self.credentials.get("port", 1521))
                service = self.credentials.get("service_name", "")
                dsn = oracledb.makedsn(host, port, service_name=service)
                self._conn = oracledb.connect(user=username, password=password, dsn=dsn)
            elif conn_type == "dsn":
                dsn = self.credentials.get("dsn", "")
                self._conn = oracledb.connect(user=username, password=password, dsn=dsn)
            else:
                conn_str = self.credentials.get("connection_string", "")
                self._conn = oracledb.connect(conn_str)

            self._connected = True
            return True
        except ImportError:
            print("oracledb not installed. Run: pip install oracledb")
            return False
        except Exception as e:
            print(f"Oracle connection error: {e}")
            return False

    async def disconnect(self):
        if self._conn:
            self._conn.close()
        self._conn = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        username = self.credentials.get("username", "")
        if not username:
            return False, "Username is required"

        try:
            if await self.connect():
                cursor = self._conn.cursor()
                cursor.execute("SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1")
                version = cursor.fetchone()[0]
                cursor.close()
                await self.disconnect()
                return True, f"Connected: {version[:50]}"
            return False, "Connection failed"
        except ImportError:
            return False, "oracledb not installed. Run: pip install oracledb"
        except Exception as e:
            return False, f"Error: {str(e)[:100]}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._conn:
            return

        query = self.credentials.get("query", "")
        if not query:
            return

        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]

            for i, row in enumerate(cursor):
                record = {}
                for j, col in enumerate(columns):
                    value = row[j]
                    if isinstance(value, datetime):
                        value = value.isoformat()
                    record[col] = value

                yield DataRecord(
                    source_id=self.source_id,
                    source_type=self.plugin_id,
                    timestamp=datetime.utcnow().isoformat(),
                    data=record,
                    metadata={"row_index": i},
                )

            cursor.close()
        except Exception as e:
            print(f"Oracle query error: {e}")
