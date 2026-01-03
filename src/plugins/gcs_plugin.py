"""Google Cloud Storage plugin."""
from datetime import datetime
from typing import Any, AsyncIterator
import io

import pandas as pd

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class GCSPlugin(DataSourcePlugin):
    """Plugin for Google Cloud Storage."""

    plugin_id = "gcs"
    plugin_name = "Google Cloud Storage"
    plugin_description = "Read data files from Google Cloud Storage"
    plugin_icon = "cloud_circle"

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="credentials_json",
                label="Service Account JSON",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="/path/to/service-account.json",
                help_text="Path to service account credentials JSON file",
            ),
            CredentialField(
                name="bucket",
                label="Bucket Name",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="my-bucket",
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
        self._client = None
        self._bucket = None

    async def connect(self) -> bool:
        try:
            from google.cloud import storage
            from google.oauth2 import service_account

            creds_path = self.credentials.get("credentials_json", "")
            bucket_name = self.credentials.get("bucket", "")

            credentials = service_account.Credentials.from_service_account_file(creds_path)
            self._client = storage.Client(credentials=credentials)
            self._bucket = self._client.bucket(bucket_name)

            self._connected = True
            return True
        except ImportError:
            print("google-cloud-storage not installed")
            return False
        except Exception as e:
            print(f"GCS connection error: {e}")
            return False

    async def disconnect(self):
        self._client = None
        self._bucket = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        bucket_name = self.credentials.get("bucket", "")
        creds_path = self.credentials.get("credentials_json", "")

        if not bucket_name:
            return False, "Bucket name is required"
        if not creds_path:
            return False, "Credentials JSON path is required"

        try:
            from google.cloud import storage
            from google.oauth2 import service_account

            credentials = service_account.Credentials.from_service_account_file(creds_path)
            client = storage.Client(credentials=credentials)
            bucket = client.bucket(bucket_name)

            if bucket.exists():
                return True, f"Connected to bucket: {bucket_name}"
            return False, f"Bucket {bucket_name} not found"
        except ImportError:
            return False, "google-cloud-storage not installed"
        except Exception as e:
            return False, f"Error: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._bucket:
            return

        import fnmatch

        prefix = self.credentials.get("prefix", "")
        pattern = self.credentials.get("file_pattern", "*.csv")
        delimiter = self.credentials.get("delimiter", ",")

        try:
            blobs = self._client.list_blobs(self._bucket, prefix=prefix)

            for blob in blobs:
                filename = blob.name.split("/")[-1]
                if not fnmatch.fnmatch(filename, pattern):
                    continue

                try:
                    content = blob.download_as_bytes()

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
                            metadata={"bucket": self._bucket.name, "blob": blob.name, "row": i},
                        )
                except Exception as e:
                    print(f"Error reading {blob.name}: {e}")

        except Exception as e:
            print(f"GCS fetch error: {e}")
