"""Database data source plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class DatabasePlugin(DataSourcePlugin):
    """Plugin for connecting to SQL databases."""

    plugin_id = "database"
    plugin_name = "SQL Database"
    plugin_description = "Connect to PostgreSQL, MySQL, SQLite, or SQL Server"
    plugin_icon = "storage"

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._engine: Engine | None = None

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="db_type",
                label="Database Type",
                field_type=FieldType.SELECT,
                required=True,
                default="postgresql",
                options=[
                    {"value": "postgresql", "label": "PostgreSQL"},
                    {"value": "mysql", "label": "MySQL"},
                    {"value": "sqlite", "label": "SQLite"},
                    {"value": "mssql", "label": "SQL Server"},
                ],
            ),
            CredentialField(
                name="host",
                label="Host",
                field_type=FieldType.TEXT,
                required=False,
                default="localhost",
                placeholder="localhost or IP address",
                help_text="Not required for SQLite",
            ),
            CredentialField(
                name="port",
                label="Port",
                field_type=FieldType.NUMBER,
                required=False,
                placeholder="5432",
                help_text="Leave empty for default port",
            ),
            CredentialField(
                name="database",
                label="Database Name / File Path",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="mydb or /path/to/database.db",
            ),
            CredentialField(
                name="username",
                label="Username",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="Database username",
            ),
            CredentialField(
                name="password",
                label="Password",
                field_type=FieldType.PASSWORD,
                required=False,
                placeholder="Database password",
            ),
            CredentialField(
                name="query",
                label="SQL Query",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="SELECT * FROM my_table",
                help_text="SQL query to fetch data",
            ),
        ]

    def _build_connection_string(self) -> str:
        """Build SQLAlchemy connection string."""
        db_type = self.credentials.get("db_type", "postgresql")
        host = self.credentials.get("host", "localhost")
        port = self.credentials.get("port", "")
        database = self.credentials.get("database", "")
        username = self.credentials.get("username", "")
        password = self.credentials.get("password", "")

        if db_type == "sqlite":
            return f"sqlite:///{database}"

        # Build connection string for other databases
        drivers = {
            "postgresql": "postgresql",
            "mysql": "mysql+pymysql",
            "mssql": "mssql+pyodbc",
        }

        driver = drivers.get(db_type, db_type)

        # Default ports
        default_ports = {
            "postgresql": "5432",
            "mysql": "3306",
            "mssql": "1433",
        }

        if not port:
            port = default_ports.get(db_type, "")

        # Build URL
        auth = ""
        if username:
            auth = username
            if password:
                auth += f":{password}"
            auth += "@"

        host_port = host
        if port:
            host_port += f":{port}"

        conn_str = f"{driver}://{auth}{host_port}/{database}"

        # Add ODBC driver for SQL Server
        if db_type == "mssql":
            conn_str += "?driver=ODBC+Driver+17+for+SQL+Server"

        return conn_str

    async def connect(self) -> bool:
        """Create database connection."""
        try:
            conn_str = self._build_connection_string()
            self._engine = create_engine(conn_str)

            # Test connection
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            self._connected = True
            return True

        except Exception as e:
            print(f"Database connection error: {e}")
            return False

    async def disconnect(self):
        """Close database connection."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        """Test database connection."""
        db_type = self.credentials.get("db_type", "")
        database = self.credentials.get("database", "")
        query = self.credentials.get("query", "")

        if not database:
            return False, "Database name/path is required"
        if not query:
            return False, "SQL query is required"

        try:
            conn_str = self._build_connection_string()
            engine = create_engine(conn_str)

            with engine.connect() as conn:
                # Test with a simple query first
                conn.execute(text("SELECT 1"))

                # Then test the actual query with LIMIT
                test_query = query.strip().rstrip(';')
                if 'LIMIT' not in test_query.upper():
                    test_query += " LIMIT 1"

                result = conn.execute(text(test_query))
                columns = result.keys()

                engine.dispose()
                return True, f"Connected! Query returns {len(list(columns))} columns"

        except Exception as e:
            error_msg = str(e)
            if "could not connect" in error_msg.lower():
                return False, "Could not connect to database server"
            elif "authentication failed" in error_msg.lower():
                return False, "Authentication failed - check username/password"
            elif "does not exist" in error_msg.lower():
                return False, "Database or table does not exist"
            else:
                return False, f"Error: {error_msg[:100]}"

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
                    record = dict(zip(columns, row))

                    # Convert non-serializable types
                    for key, value in record.items():
                        if isinstance(value, (datetime,)):
                            record[key] = value.isoformat()
                        elif hasattr(value, '__dict__'):
                            record[key] = str(value)

                    yield DataRecord(
                        source_id=self.source_id,
                        source_type=self.plugin_id,
                        timestamp=datetime.utcnow().isoformat(),
                        data=record,
                        metadata={
                            "query": query[:100],
                            "row_index": i,
                        }
                    )

        except Exception as e:
            print(f"Database query error: {e}")
