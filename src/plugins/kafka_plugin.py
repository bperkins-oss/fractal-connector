"""Apache Kafka plugin."""
from datetime import datetime
from typing import Any, AsyncIterator
import json

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class KafkaPlugin(DataSourcePlugin):
    """Plugin for Apache Kafka."""

    plugin_id = "kafka"
    plugin_name = "Apache Kafka"
    plugin_description = "Consume messages from Kafka topics"
    plugin_icon = "stream"

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="bootstrap_servers",
                label="Bootstrap Servers",
                field_type=FieldType.TEXT,
                required=True,
                default="localhost:9092",
                placeholder="localhost:9092",
                help_text="Comma-separated list of broker addresses",
            ),
            CredentialField(
                name="topic",
                label="Topic",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="my-topic",
            ),
            CredentialField(
                name="group_id",
                label="Consumer Group ID",
                field_type=FieldType.TEXT,
                required=True,
                default="fractal-connector",
            ),
            CredentialField(
                name="auto_offset",
                label="Auto Offset Reset",
                field_type=FieldType.SELECT,
                required=False,
                default="latest",
                options=[
                    {"value": "latest", "label": "Latest"},
                    {"value": "earliest", "label": "Earliest"},
                ],
            ),
            CredentialField(
                name="security_protocol",
                label="Security Protocol",
                field_type=FieldType.SELECT,
                required=False,
                default="PLAINTEXT",
                options=[
                    {"value": "PLAINTEXT", "label": "Plaintext"},
                    {"value": "SSL", "label": "SSL"},
                    {"value": "SASL_PLAINTEXT", "label": "SASL Plaintext"},
                    {"value": "SASL_SSL", "label": "SASL SSL"},
                ],
            ),
            CredentialField(
                name="sasl_mechanism",
                label="SASL Mechanism",
                field_type=FieldType.SELECT,
                required=False,
                default="PLAIN",
                options=[
                    {"value": "PLAIN", "label": "PLAIN"},
                    {"value": "SCRAM-SHA-256", "label": "SCRAM-SHA-256"},
                    {"value": "SCRAM-SHA-512", "label": "SCRAM-SHA-512"},
                ],
            ),
            CredentialField(
                name="username",
                label="SASL Username",
                field_type=FieldType.TEXT,
                required=False,
            ),
            CredentialField(
                name="password",
                label="SASL Password",
                field_type=FieldType.PASSWORD,
                required=False,
            ),
            CredentialField(
                name="max_messages",
                label="Max Messages per Fetch",
                field_type=FieldType.NUMBER,
                required=False,
                default="100",
            ),
        ]

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._consumer = None

    async def connect(self) -> bool:
        try:
            from kafka import KafkaConsumer

            servers = self.credentials.get("bootstrap_servers", "localhost:9092")
            topic = self.credentials.get("topic", "")
            group_id = self.credentials.get("group_id", "fractal-connector")
            auto_offset = self.credentials.get("auto_offset", "latest")
            security = self.credentials.get("security_protocol", "PLAINTEXT")
            sasl_mech = self.credentials.get("sasl_mechanism", "PLAIN")
            username = self.credentials.get("username", "")
            password = self.credentials.get("password", "")

            kwargs = {
                "bootstrap_servers": servers.split(","),
                "group_id": group_id,
                "auto_offset_reset": auto_offset,
                "security_protocol": security,
                "value_deserializer": lambda m: json.loads(m.decode("utf-8")) if m else None,
                "consumer_timeout_ms": 5000,
            }

            if security in ["SASL_PLAINTEXT", "SASL_SSL"]:
                kwargs["sasl_mechanism"] = sasl_mech
                kwargs["sasl_plain_username"] = username
                kwargs["sasl_plain_password"] = password

            self._consumer = KafkaConsumer(topic, **kwargs)
            self._connected = True
            return True
        except ImportError:
            print("kafka-python not installed. Run: pip install kafka-python")
            return False
        except Exception as e:
            print(f"Kafka connection error: {e}")
            return False

    async def disconnect(self):
        if self._consumer:
            self._consumer.close()
        self._consumer = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        servers = self.credentials.get("bootstrap_servers", "")
        topic = self.credentials.get("topic", "")

        if not servers:
            return False, "Bootstrap servers are required"
        if not topic:
            return False, "Topic is required"

        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                bootstrap_servers=servers.split(","),
                consumer_timeout_ms=5000,
            )
            topics = consumer.topics()
            consumer.close()

            if topic in topics:
                return True, f"Connected! Topic '{topic}' found"
            return True, f"Connected! Topic '{topic}' not found yet (will be created)"
        except ImportError:
            return False, "kafka-python not installed"
        except Exception as e:
            return False, f"Error: {str(e)[:100]}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._consumer:
            return

        max_messages = int(self.credentials.get("max_messages", 100) or 100)
        topic = self.credentials.get("topic", "")

        try:
            count = 0
            for message in self._consumer:
                if count >= max_messages:
                    break

                data = message.value if isinstance(message.value, dict) else {"value": message.value}

                yield DataRecord(
                    source_id=self.source_id,
                    source_type=self.plugin_id,
                    timestamp=datetime.utcnow().isoformat(),
                    data=data,
                    metadata={
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "key": message.key.decode("utf-8") if message.key else None,
                    },
                )
                count += 1

        except Exception as e:
            print(f"Kafka consume error: {e}")
