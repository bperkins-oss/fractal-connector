"""MongoDB data source plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class MongoDBPlugin(DataSourcePlugin):
    """Plugin for MongoDB databases."""

    plugin_id = "mongodb"
    plugin_name = "MongoDB"
    plugin_description = "Connect to MongoDB databases"
    plugin_icon = "forest"

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._client = None
        self._db = None

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="connection_type",
                label="Connection Type",
                field_type=FieldType.SELECT,
                required=True,
                default="host",
                options=[
                    {"value": "host", "label": "Host/Port"},
                    {"value": "uri", "label": "Connection URI"},
                ],
            ),
            CredentialField(
                name="uri",
                label="Connection URI",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="mongodb://user:pass@host:27017/db",
                help_text="Full MongoDB connection string (for URI mode)",
            ),
            CredentialField(
                name="host",
                label="Host",
                field_type=FieldType.TEXT,
                required=False,
                default="localhost",
                placeholder="localhost",
            ),
            CredentialField(
                name="port",
                label="Port",
                field_type=FieldType.NUMBER,
                required=False,
                default="27017",
                placeholder="27017",
            ),
            CredentialField(
                name="username",
                label="Username",
                field_type=FieldType.TEXT,
                required=False,
            ),
            CredentialField(
                name="password",
                label="Password",
                field_type=FieldType.PASSWORD,
                required=False,
            ),
            CredentialField(
                name="database",
                label="Database",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="mydb",
            ),
            CredentialField(
                name="collection",
                label="Collection",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="users",
            ),
            CredentialField(
                name="query",
                label="Query Filter (JSON)",
                field_type=FieldType.TEXT,
                required=False,
                default="{}",
                placeholder='{"status": "active"}',
                help_text="MongoDB query filter as JSON",
            ),
            CredentialField(
                name="projection",
                label="Projection (JSON)",
                field_type=FieldType.TEXT,
                required=False,
                placeholder='{"name": 1, "email": 1}',
                help_text="Fields to include/exclude",
            ),
            CredentialField(
                name="limit",
                label="Limit",
                field_type=FieldType.NUMBER,
                required=False,
                default="1000",
                placeholder="1000",
                help_text="Maximum documents to fetch",
            ),
        ]

    def _build_uri(self) -> str:
        """Build MongoDB connection URI."""
        connection_type = self.credentials.get("connection_type", "host")

        if connection_type == "uri":
            return self.credentials.get("uri", "")

        host = self.credentials.get("host", "localhost")
        port = self.credentials.get("port", "27017")
        username = self.credentials.get("username", "")
        password = self.credentials.get("password", "")
        database = self.credentials.get("database", "")

        if username and password:
            from urllib.parse import quote_plus
            return f"mongodb://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/{database}"
        else:
            return f"mongodb://{host}:{port}/{database}"

    async def connect(self) -> bool:
        """Connect to MongoDB."""
        try:
            from pymongo import MongoClient

            uri = self._build_uri()
            database = self.credentials.get("database", "")

            self._client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            self._db = self._client[database]

            # Test connection
            self._client.server_info()

            self._connected = True
            return True

        except ImportError:
            print("Warning: pymongo not installed. Run: pip install pymongo")
            return False
        except Exception as e:
            print(f"MongoDB connection error: {e}")
            return False

    async def disconnect(self):
        """Disconnect from MongoDB."""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        """Test MongoDB connection."""
        database = self.credentials.get("database", "")
        collection = self.credentials.get("collection", "")

        if not database:
            return False, "Database name is required"
        if not collection:
            return False, "Collection name is required"

        try:
            from pymongo import MongoClient

            uri = self._build_uri()
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)

            # Test connection
            client.server_info()

            # Check collection exists
            db = client[database]
            count = db[collection].estimated_document_count()

            client.close()
            return True, f"Connected! Collection has ~{count} documents"

        except ImportError:
            return False, "pymongo not installed. Run: pip install pymongo"
        except Exception as e:
            error = str(e)
            if "Authentication failed" in error:
                return False, "Authentication failed"
            elif "timed out" in error:
                return False, "Connection timed out"
            else:
                return False, f"Error: {error[:100]}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        """Fetch data from MongoDB."""
        if not self._db:
            return

        import json

        collection = self.credentials.get("collection", "")
        query_str = self.credentials.get("query", "{}")
        projection_str = self.credentials.get("projection", "")
        limit = int(self.credentials.get("limit", 1000) or 1000)

        try:
            query = json.loads(query_str) if query_str else {}
            projection = json.loads(projection_str) if projection_str else None
        except json.JSONDecodeError:
            query = {}
            projection = None

        try:
            coll = self._db[collection]
            cursor = coll.find(query, projection).limit(limit)

            for i, doc in enumerate(cursor):
                # Convert ObjectId and other BSON types
                record = {}
                for key, value in doc.items():
                    if key == "_id":
                        record["_id"] = str(value)
                    elif isinstance(value, datetime):
                        record[key] = value.isoformat()
                    elif hasattr(value, '__dict__'):
                        record[key] = str(value)
                    else:
                        record[key] = value

                yield DataRecord(
                    source_id=self.source_id,
                    source_type=self.plugin_id,
                    timestamp=datetime.utcnow().isoformat(),
                    data=record,
                    metadata={
                        "collection": collection,
                        "index": i,
                    },
                )

        except Exception as e:
            print(f"MongoDB fetch error: {e}")
