"""Microsoft SQL Server dedicated plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class MSSQLPlugin(DataSourcePlugin):
    """Plugin for Microsoft SQL Server."""

    plugin_id = "mssql"
    plugin_name = "Microsoft SQL Server"
    plugin_description = "Connect to Microsoft SQL Server databases"
    plugin_icon = "dns"

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._engine: Engine | None = None

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="host",
                label="Server Host",
                field_type=FieldType.TEXT,
                required=True,
                default="localhost",
                placeholder="localhost or server\\instance",
                help_text="SQL Server host (use server\\instance for named instances)",
            ),
            CredentialField(
                name="port",
                label="Port",
                field_type=FieldType.NUMBER,
                required=False,
                default="1433",
                placeholder="1433",
                help_text="Leave empty for default port",
            ),
            CredentialField(
                name="database",
                label="Database",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="master",
            ),
            CredentialField(
                name="auth_type",
                label="Authentication",
                field_type=FieldType.SELECT,
                required=True,
                default="sql",
                options=[
                    {"value": "sql", "label": "SQL Server Authentication"},
                    {"value": "windows", "label": "Windows Authentication"},
                ],
            ),
            CredentialField(
                name="username",
                label="Username",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="sa",
                help_text="Required for SQL Server Authentication",
            ),
            CredentialField(
                name="password",
                label="Password",
                field_type=FieldType.PASSWORD,
                required=False,
            ),
            CredentialField(
                name="driver",
                label="ODBC Driver",
                field_type=FieldType.SELECT,
                required=True,
                default="ODBC Driver 17 for SQL Server",
                options=[
                    {"value": "ODBC Driver 17 for SQL Server", "label": "ODBC Driver 17"},
                    {"value": "ODBC Driver 18 for SQL Server", "label": "ODBC Driver 18"},
                    {"value": "SQL Server Native Client 11.0", "label": "Native Client 11"},
                    {"value": "SQL Server", "label": "SQL Server (legacy)"},
                ],
            ),
            CredentialField(
                name="query",
                label="SQL Query",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="SELECT * FROM MyTable",
            ),
            CredentialField(
                name="trust_cert",
                label="Trust Server Certificate",
                field_type=FieldType.CHECKBOX,
                required=False,
                default=True,
                help_text="Trust the server certificate (required for some configurations)",
            ),
        ]

    def _build_connection_string(self) -> str:
        """Build SQL Server connection string."""
        host = self.credentials.get("host", "localhost")
        port = self.credentials.get("port", "1433")
        database = self.credentials.get("database", "")
        auth_type = self.credentials.get("auth_type", "sql")
        username = self.credentials.get("username", "")
        password = self.credentials.get("password", "")
        driver = self.credentials.get("driver", "ODBC Driver 17 for SQL Server")
        trust_cert = self.credentials.get("trust_cert", True)

        # Build connection string
        params = [f"DRIVER={{{driver}}}"]
        params.append(f"SERVER={host},{port}")
        params.append(f"DATABASE={database}")

        if auth_type == "windows":
            params.append("Trusted_Connection=yes")
        else:
            params.append(f"UID={username}")
            params.append(f"PWD={password}")

        if trust_cert:
            params.append("TrustServerCertificate=yes")

        conn_str = ";".join(params)
        return f"mssql+pyodbc:///?odbc_connect={conn_str}"

    async def connect(self) -> bool:
        """Connect to SQL Server."""
        try:
            conn_str = self._build_connection_string()
            self._engine = create_engine(conn_str)

            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            self._connected = True
            return True
        except Exception as e:
            print(f"SQL Server connection error: {e}")
            return False

    async def disconnect(self):
        """Disconnect from SQL Server."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        """Test SQL Server connection."""
        host = self.credentials.get("host", "")
        database = self.credentials.get("database", "")
        query = self.credentials.get("query", "")

        if not host:
            return False, "Server host is required"
        if not database:
            return False, "Database name is required"
        if not query:
            return False, "SQL query is required"

        try:
            conn_str = self._build_connection_string()
            engine = create_engine(conn_str)

            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                result = conn.execute(text(f"{query.rstrip(';')} OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY"))
                columns = list(result.keys())

            engine.dispose()
            return True, f"Connected! Query returns {len(columns)} columns"

        except Exception as e:
            error = str(e)
            if "Login failed" in error:
                return False, "Login failed - check username/password"
            elif "Cannot open database" in error:
                return False, f"Cannot open database '{database}'"
            elif "driver" in error.lower():
                return False, "ODBC driver not found. Install SQL Server ODBC drivers."
            else:
                return False, f"Error: {error[:100]}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        """Execute query and yield results."""
        query = self.credentials.get("query", "")

        if not self._engine or not query:
            return

        try:
            with self._engine.connect() as conn:
                result = conn.execute(text(query))
                columns = list(result.keys())

                for i, row in enumerate(result):
                    record = {}
                    for j, col in enumerate(columns):
                        value = row[j]
                        if isinstance(value, datetime):
                            value = value.isoformat()
                        elif hasattr(value, '__dict__'):
                            value = str(value)
                        record[col] = value

                    yield DataRecord(
                        source_id=self.source_id,
                        source_type=self.plugin_id,
                        timestamp=datetime.utcnow().isoformat(),
                        data=record,
                        metadata={"row_index": i},
                    )

        except Exception as e:
            print(f"SQL Server query error: {e}")
