"""Elasticsearch plugin."""
from datetime import datetime
from typing import Any, AsyncIterator
import json

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class ElasticsearchPlugin(DataSourcePlugin):
    """Plugin for Elasticsearch."""

    plugin_id = "elasticsearch"
    plugin_name = "Elasticsearch"
    plugin_description = "Search and retrieve data from Elasticsearch"
    plugin_icon = "search"

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="hosts",
                label="Hosts",
                field_type=FieldType.TEXT,
                required=True,
                default="http://localhost:9200",
                placeholder="http://localhost:9200",
                help_text="Comma-separated list of hosts",
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
                name="api_key",
                label="API Key",
                field_type=FieldType.PASSWORD,
                required=False,
                help_text="Alternative to username/password",
            ),
            CredentialField(
                name="index",
                label="Index Pattern",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="my-index-*",
            ),
            CredentialField(
                name="query",
                label="Query (JSON)",
                field_type=FieldType.TEXT,
                required=False,
                default='{"match_all": {}}',
                placeholder='{"match": {"field": "value"}}',
            ),
            CredentialField(
                name="size",
                label="Max Results",
                field_type=FieldType.NUMBER,
                required=False,
                default="100",
            ),
            CredentialField(
                name="sort",
                label="Sort Field",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="@timestamp:desc",
            ),
            CredentialField(
                name="verify_certs",
                label="Verify SSL Certificates",
                field_type=FieldType.CHECKBOX,
                required=False,
                default=True,
            ),
        ]

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._es = None

    async def connect(self) -> bool:
        try:
            from elasticsearch import Elasticsearch

            hosts = [h.strip() for h in self.credentials.get("hosts", "").split(",")]
            username = self.credentials.get("username", "")
            password = self.credentials.get("password", "")
            api_key = self.credentials.get("api_key", "")
            verify_certs = self.credentials.get("verify_certs", True)

            kwargs = {"hosts": hosts, "verify_certs": verify_certs}

            if api_key:
                kwargs["api_key"] = api_key
            elif username and password:
                kwargs["basic_auth"] = (username, password)

            self._es = Elasticsearch(**kwargs)
            self._connected = True
            return True
        except ImportError:
            print("elasticsearch not installed. Run: pip install elasticsearch")
            return False
        except Exception as e:
            print(f"Elasticsearch connection error: {e}")
            return False

    async def disconnect(self):
        if self._es:
            self._es.close()
        self._es = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        hosts = self.credentials.get("hosts", "")
        index = self.credentials.get("index", "")

        if not hosts:
            return False, "Hosts are required"
        if not index:
            return False, "Index pattern is required"

        try:
            if await self.connect():
                info = self._es.info()
                version = info.get("version", {}).get("number", "unknown")
                await self.disconnect()
                return True, f"Connected to Elasticsearch {version}"
            return False, "Connection failed"
        except ImportError:
            return False, "elasticsearch not installed"
        except Exception as e:
            return False, f"Error: {str(e)[:100]}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._es:
            return

        index = self.credentials.get("index", "")
        query_str = self.credentials.get("query", '{"match_all": {}}')
        size = int(self.credentials.get("size", 100) or 100)
        sort_str = self.credentials.get("sort", "")

        try:
            query = json.loads(query_str)
        except json.JSONDecodeError:
            query = {"match_all": {}}

        body = {"query": query, "size": size}

        if sort_str:
            field, order = sort_str.split(":") if ":" in sort_str else (sort_str, "asc")
            body["sort"] = [{field: {"order": order}}]

        try:
            response = self._es.search(index=index, body=body)
            hits = response.get("hits", {}).get("hits", [])

            for i, hit in enumerate(hits):
                yield DataRecord(
                    source_id=self.source_id,
                    source_type=self.plugin_id,
                    timestamp=datetime.utcnow().isoformat(),
                    data={
                        "_id": hit.get("_id"),
                        "_index": hit.get("_index"),
                        "_score": hit.get("_score"),
                        **hit.get("_source", {}),
                    },
                    metadata={"index": i, "total": response.get("hits", {}).get("total", {}).get("value", 0)},
                )

        except Exception as e:
            print(f"Elasticsearch query error: {e}")
