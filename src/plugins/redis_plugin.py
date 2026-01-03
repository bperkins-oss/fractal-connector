"""Redis plugin."""
from datetime import datetime
from typing import Any, AsyncIterator
import json

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class RedisPlugin(DataSourcePlugin):
    """Plugin for Redis."""

    plugin_id = "redis"
    plugin_name = "Redis"
    plugin_description = "Read data from Redis"
    plugin_icon = "memory"

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="host",
                label="Host",
                field_type=FieldType.TEXT,
                required=True,
                default="localhost",
            ),
            CredentialField(
                name="port",
                label="Port",
                field_type=FieldType.NUMBER,
                required=False,
                default="6379",
            ),
            CredentialField(
                name="password",
                label="Password",
                field_type=FieldType.PASSWORD,
                required=False,
            ),
            CredentialField(
                name="db",
                label="Database Number",
                field_type=FieldType.NUMBER,
                required=False,
                default="0",
            ),
            CredentialField(
                name="ssl",
                label="Use SSL",
                field_type=FieldType.CHECKBOX,
                required=False,
                default=False,
            ),
            CredentialField(
                name="data_type",
                label="Data Type",
                field_type=FieldType.SELECT,
                required=True,
                default="keys",
                options=[
                    {"value": "keys", "label": "Keys (by pattern)"},
                    {"value": "stream", "label": "Stream"},
                    {"value": "list", "label": "List"},
                    {"value": "hash", "label": "Hash"},
                    {"value": "set", "label": "Set"},
                ],
            ),
            CredentialField(
                name="pattern",
                label="Key Pattern",
                field_type=FieldType.TEXT,
                required=False,
                default="*",
                placeholder="user:*",
                help_text="Pattern for key matching",
            ),
            CredentialField(
                name="key",
                label="Specific Key",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="mystream",
                help_text="For stream/list/hash/set data types",
            ),
            CredentialField(
                name="limit",
                label="Max Keys/Items",
                field_type=FieldType.NUMBER,
                required=False,
                default="100",
            ),
        ]

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._redis = None

    async def connect(self) -> bool:
        try:
            import redis

            self._redis = redis.Redis(
                host=self.credentials.get("host", "localhost"),
                port=int(self.credentials.get("port", 6379)),
                password=self.credentials.get("password") or None,
                db=int(self.credentials.get("db", 0)),
                ssl=self.credentials.get("ssl", False),
                decode_responses=True,
            )
            self._redis.ping()
            self._connected = True
            return True
        except ImportError:
            print("redis not installed. Run: pip install redis")
            return False
        except Exception as e:
            print(f"Redis connection error: {e}")
            return False

    async def disconnect(self):
        if self._redis:
            self._redis.close()
        self._redis = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        try:
            if await self.connect():
                info = self._redis.info("server")
                version = info.get("redis_version", "unknown")
                await self.disconnect()
                return True, f"Connected to Redis {version}"
            return False, "Connection failed"
        except ImportError:
            return False, "redis not installed"
        except Exception as e:
            return False, f"Error: {str(e)[:100]}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._redis:
            return

        data_type = self.credentials.get("data_type", "keys")
        pattern = self.credentials.get("pattern", "*")
        key = self.credentials.get("key", "")
        limit = int(self.credentials.get("limit", 100) or 100)

        try:
            if data_type == "keys":
                keys = list(self._redis.scan_iter(match=pattern, count=limit))[:limit]
                for i, k in enumerate(keys):
                    key_type = self._redis.type(k)
                    value = None
                    if key_type == "string":
                        value = self._redis.get(k)
                    elif key_type == "hash":
                        value = self._redis.hgetall(k)
                    elif key_type == "list":
                        value = self._redis.lrange(k, 0, -1)
                    elif key_type == "set":
                        value = list(self._redis.smembers(k))

                    yield DataRecord(
                        source_id=self.source_id,
                        source_type=self.plugin_id,
                        timestamp=datetime.utcnow().isoformat(),
                        data={"key": k, "type": key_type, "value": value},
                        metadata={"index": i},
                    )

            elif data_type == "stream" and key:
                entries = self._redis.xrange(key, count=limit)
                for i, (entry_id, fields) in enumerate(entries):
                    yield DataRecord(
                        source_id=self.source_id,
                        source_type=self.plugin_id,
                        timestamp=datetime.utcnow().isoformat(),
                        data={"id": entry_id, **fields},
                        metadata={"key": key, "index": i},
                    )

            elif data_type == "list" and key:
                items = self._redis.lrange(key, 0, limit - 1)
                for i, item in enumerate(items):
                    try:
                        data = json.loads(item)
                    except:
                        data = {"value": item}
                    yield DataRecord(
                        source_id=self.source_id,
                        source_type=self.plugin_id,
                        timestamp=datetime.utcnow().isoformat(),
                        data=data,
                        metadata={"key": key, "index": i},
                    )

            elif data_type == "hash" and key:
                fields = self._redis.hgetall(key)
                yield DataRecord(
                    source_id=self.source_id,
                    source_type=self.plugin_id,
                    timestamp=datetime.utcnow().isoformat(),
                    data=fields,
                    metadata={"key": key},
                )

            elif data_type == "set" and key:
                members = list(self._redis.smembers(key))[:limit]
                for i, member in enumerate(members):
                    yield DataRecord(
                        source_id=self.source_id,
                        source_type=self.plugin_id,
                        timestamp=datetime.utcnow().isoformat(),
                        data={"value": member},
                        metadata={"key": key, "index": i},
                    )

        except Exception as e:
            print(f"Redis fetch error: {e}")
