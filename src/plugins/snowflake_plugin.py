"""Snowflake data source plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class SnowflakePlugin(DataSourcePlugin):
    """Plugin for Snowflake Data Warehouse."""

    plugin_id = "snowflake"
    plugin_name = "Snowflake"
    plugin_description = "Connect to Snowflake data warehouse"
    plugin_icon = "ac_unit"

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._conn = None

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="account",
                label="Account Identifier",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="xy12345.us-east-1",
                help_text="Snowflake account identifier",
            ),
            CredentialField(
                name="username",
                label="Username",
                field_type=FieldType.TEXT,
                required=True,
            ),
            CredentialField(
                name="auth_type",
                label="Authentication",
                field_type=FieldType.SELECT,
                required=True,
                default="password",
                options=[
                    {"value": "password", "label": "Password"},
                    {"value": "externalbrowser", "label": "External Browser (SSO)"},
                    {"value": "keypair", "label": "Key Pair"},
                ],
            ),
            CredentialField(
                name="password",
                label="Password",
                field_type=FieldType.PASSWORD,
                required=False,
                help_text="Required for password authentication",
            ),
            CredentialField(
                name="private_key_path",
                label="Private Key Path",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="/path/to/rsa_key.p8",
                help_text="For key pair authentication",
            ),
            CredentialField(
                name="warehouse",
                label="Warehouse",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="COMPUTE_WH",
            ),
            CredentialField(
                name="database",
                label="Database",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="MY_DATABASE",
            ),
            CredentialField(
                name="schema",
                label="Schema",
                field_type=FieldType.TEXT,
                required=False,
                default="PUBLIC",
                placeholder="PUBLIC",
            ),
            CredentialField(
                name="role",
                label="Role",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="ACCOUNTADMIN",
            ),
            CredentialField(
                name="query",
                label="SQL Query",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="SELECT * FROM my_table",
            ),
        ]

    async def connect(self) -> bool:
        """Connect to Snowflake."""
        try:
            import snowflake.connector

            account = self.credentials.get("account", "")
            username = self.credentials.get("username", "")
            auth_type = self.credentials.get("auth_type", "password")
            password = self.credentials.get("password", "")
            warehouse = self.credentials.get("warehouse", "")
            database = self.credentials.get("database", "")
            schema = self.credentials.get("schema", "PUBLIC")
            role = self.credentials.get("role", "")

            conn_params = {
                "account": account,
                "user": username,
                "warehouse": warehouse,
                "database": database,
                "schema": schema,
            }

            if role:
                conn_params["role"] = role

            if auth_type == "password":
                conn_params["password"] = password
            elif auth_type == "externalbrowser":
                conn_params["authenticator"] = "externalbrowser"
            elif auth_type == "keypair":
                private_key_path = self.credentials.get("private_key_path", "")
                with open(private_key_path, "rb") as key_file:
                    from cryptography.hazmat.backends import default_backend
                    from cryptography.hazmat.primitives import serialization
                    p_key = serialization.load_pem_private_key(
                        key_file.read(),
                        password=None,
                        backend=default_backend()
                    )
                    pkb = p_key.private_bytes(
                        encoding=serialization.Encoding.DER,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption()
                    )
                    conn_params["private_key"] = pkb

            self._conn = snowflake.connector.connect(**conn_params)
            self._connected = True
            return True

        except ImportError:
            print("Warning: snowflake-connector-python not installed")
            return False
        except Exception as e:
            print(f"Snowflake connection error: {e}")
            return False

    async def disconnect(self):
        """Disconnect from Snowflake."""
        if self._conn:
            self._conn.close()
            self._conn = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        """Test Snowflake connection."""
        account = self.credentials.get("account", "")
        username = self.credentials.get("username", "")
        warehouse = self.credentials.get("warehouse", "")
        database = self.credentials.get("database", "")

        if not account:
            return False, "Account identifier is required"
        if not username:
            return False, "Username is required"
        if not warehouse:
            return False, "Warehouse is required"
        if not database:
            return False, "Database is required"

        try:
            import snowflake.connector

            if await self.connect():
                cursor = self._conn.cursor()
                cursor.execute("SELECT CURRENT_VERSION()")
                version = cursor.fetchone()[0]
                cursor.close()
                await self.disconnect()
                return True, f"Connected to Snowflake {version}"
            else:
                return False, "Connection failed"

        except ImportError:
            return False, "snowflake-connector-python not installed"
        except Exception as e:
            return False, f"Error: {str(e)[:100]}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        """Execute query and yield results."""
        if not self._conn:
            return

        query = self.credentials.get("query", "")
        if not query:
            return

        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]

            for i, row in enumerate(cursor):
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

            cursor.close()

        except Exception as e:
            print(f"Snowflake query error: {e}")
