"""Azure Blob Storage plugin."""
from datetime import datetime
from typing import Any, AsyncIterator
import io

import pandas as pd

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class AzureBlobPlugin(DataSourcePlugin):
    """Plugin for Azure Blob Storage."""

    plugin_id = "azure_blob"
    plugin_name = "Azure Blob Storage"
    plugin_description = "Read data files from Azure Blob Storage"
    plugin_icon = "cloud_queue"

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="connection_string",
                label="Connection String",
                field_type=FieldType.PASSWORD,
                required=True,
                help_text="Azure Storage connection string",
            ),
            CredentialField(
                name="container",
                label="Container Name",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="my-container",
            ),
            CredentialField(
                name="prefix",
                label="Blob Prefix (Folder)",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="data/",
            ),
            CredentialField(
                name="file_pattern",
                label="File Pattern",
                field_type=FieldType.TEXT,
                required=False,
                default="*.csv",
            ),
            CredentialField(
                name="delimiter",
                label="CSV Delimiter",
                field_type=FieldType.SELECT,
                required=False,
                default=",",
                options=[
                    {"value": ",", "label": "Comma"},
                    {"value": ";", "label": "Semicolon"},
                    {"value": "\t", "label": "Tab"},
                ],
            ),
        ]

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._blob_service = None
        self._container_client = None

    async def connect(self) -> bool:
        try:
            from azure.storage.blob import BlobServiceClient

            conn_str = self.credentials.get("connection_string", "")
            container = self.credentials.get("container", "")

            self._blob_service = BlobServiceClient.from_connection_string(conn_str)
            self._container_client = self._blob_service.get_container_client(container)

            self._connected = True
            return True
        except ImportError:
            print("azure-storage-blob not installed")
            return False
        except Exception as e:
            print(f"Azure Blob connection error: {e}")
            return False

    async def disconnect(self):
        self._blob_service = None
        self._container_client = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        conn_str = self.credentials.get("connection_string", "")
        container = self.credentials.get("container", "")

        if not conn_str:
            return False, "Connection string is required"
        if not container:
            return False, "Container name is required"

        try:
            from azure.storage.blob import BlobServiceClient

            service = BlobServiceClient.from_connection_string(conn_str)
            container_client = service.get_container_client(container)

            if container_client.exists():
                return True, f"Connected to container: {container}"
            return False, f"Container {container} not found"
        except ImportError:
            return False, "azure-storage-blob not installed"
        except Exception as e:
            return False, f"Error: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._container_client:
            return

        import fnmatch

        prefix = self.credentials.get("prefix", "")
        pattern = self.credentials.get("file_pattern", "*.csv")
        delimiter = self.credentials.get("delimiter", ",")

        try:
            blobs = self._container_client.list_blobs(name_starts_with=prefix)

            for blob in blobs:
                filename = blob.name.split("/")[-1]
                if not fnmatch.fnmatch(filename, pattern):
                    continue

                try:
                    blob_client = self._container_client.get_blob_client(blob.name)
                    content = blob_client.download_blob().readall()

                    if filename.endswith(('.xlsx', '.xls')):
                        df = pd.read_excel(io.BytesIO(content))
                    else:
                        df = pd.read_csv(io.BytesIO(content), delimiter=delimiter)

                    for i, row in df.iterrows():
                        yield DataRecord(
                            source_id=self.source_id,
                            source_type=self.plugin_id,
                            timestamp=datetime.utcnow().isoformat(),
                            data=row.to_dict(),
                            metadata={"container": self.credentials.get("container"), "blob": blob.name, "row": i},
                        )
                except Exception as e:
                    print(f"Error reading {blob.name}: {e}")

        except Exception as e:
            print(f"Azure Blob fetch error: {e}")
