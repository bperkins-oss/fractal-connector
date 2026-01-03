"""PostgreSQL dedicated plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class PostgreSQLPlugin(DataSourcePlugin):
    """Plugin for PostgreSQL databases."""

    plugin_id = "postgresql"
    plugin_name = "PostgreSQL"
    plugin_description = "Connect to PostgreSQL databases"
    plugin_icon = "storage"

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._engine: Engine | None = None

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="host",
                label="Host",
                field_type=FieldType.TEXT,
                required=True,
                default="localhost",
                placeholder="localhost",
            ),
            CredentialField(
                name="port",
                label="Port",
                field_type=FieldType.NUMBER,
                required=False,
                default="5432",
                placeholder="5432",
            ),
            CredentialField(
                name="database",
                label="Database",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="postgres",
            ),
            CredentialField(
                name="username",
                label="Username",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="postgres",
            ),
            CredentialField(
                name="password",
                label="Password",
                field_type=FieldType.PASSWORD,
                required=False,
            ),
            CredentialField(
                name="schema",
                label="Schema",
                field_type=FieldType.TEXT,
                required=False,
                default="public",
                placeholder="public",
                help_text="Default schema to use",
            ),
            CredentialField(
                name="ssl_mode",
                label="SSL Mode",
                field_type=FieldType.SELECT,
                required=False,
                default="prefer",
                options=[
                    {"value": "disable", "label": "Disable"},
                    {"value": "allow", "label": "Allow"},
                    {"value": "prefer", "label": "Prefer"},
                    {"value": "require", "label": "Require"},
                    {"value": "verify-ca", "label": "Verify CA"},
                    {"value": "verify-full", "label": "Verify Full"},
                ],
            ),
            CredentialField(
                name="query",
                label="SQL Query",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="SELECT * FROM my_table",
            ),
        ]

    def _build_connection_string(self) -> str:
        """Build PostgreSQL connection string."""
        host = self.credentials.get("host", "localhost")
        port = self.credentials.get("port", "5432")
        database = self.credentials.get("database", "")
        username = self.credentials.get("username", "")
        password = self.credentials.get("password", "")
        ssl_mode = self.credentials.get("ssl_mode", "prefer")

        # URL encode password if it contains special characters
        from urllib.parse import quote_plus
        if password:
            password = quote_plus(password)

        conn_str = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        conn_str += f"?sslmode={ssl_mode}"

        return conn_str

    async def connect(self) -> bool:
        """Connect to PostgreSQL."""
        try:
            conn_str = self._build_connection_string()
            self._engine = create_engine(conn_str)

            # Set search path if schema specified
            schema = self.credentials.get("schema", "public")
            with self._engine.connect() as conn:
                conn.execute(text(f"SET search_path TO {schema}"))
                conn.execute(text("SELECT 1"))

            self._connected = True
            return True
        except Exception as e:
            print(f"PostgreSQL connection error: {e}")
            return False

    async def disconnect(self):
        """Disconnect from PostgreSQL."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        """Test PostgreSQL connection."""
        host = self.credentials.get("host", "")
        database = self.credentials.get("database", "")
        query = self.credentials.get("query", "")

        if not database:
            return False, "Database name is required"
        if not query:
            return False, "SQL query is required"

        try:
            conn_str = self._build_connection_string()
            engine = create_engine(conn_str)

            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                result = conn.execute(text(f"{query.rstrip(';')} LIMIT 1"))
                columns = list(result.keys())

            engine.dispose()
            return True, f"Connected to {host}! Query returns {len(columns)} columns"

        except Exception as e:
            error = str(e)
            if "password authentication failed" in error:
                return False, "Authentication failed - check username/password"
            elif "does not exist" in error:
                return False, "Database or table does not exist"
            elif "could not connect" in error:
                return False, f"Cannot connect to {host}"
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
            print(f"PostgreSQL query error: {e}")
