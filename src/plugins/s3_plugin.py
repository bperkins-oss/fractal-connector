"""AWS S3 storage plugin."""
from datetime import datetime
from typing import Any, AsyncIterator
import io

import pandas as pd

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class S3Plugin(DataSourcePlugin):
    """Plugin for AWS S3 storage."""

    plugin_id = "aws_s3"
    plugin_name = "AWS S3"
    plugin_description = "Read data files from Amazon S3 buckets"
    plugin_icon = "cloud"

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="aws_access_key",
                label="AWS Access Key ID",
                field_type=FieldType.TEXT,
                required=True,
            ),
            CredentialField(
                name="aws_secret_key",
                label="AWS Secret Access Key",
                field_type=FieldType.PASSWORD,
                required=True,
            ),
            CredentialField(
                name="region",
                label="Region",
                field_type=FieldType.TEXT,
                required=False,
                default="us-east-1",
                placeholder="us-east-1",
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
                label="Key Prefix (Folder)",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="data/",
                help_text="S3 key prefix to filter objects",
            ),
            CredentialField(
                name="file_pattern",
                label="File Pattern",
                field_type=FieldType.TEXT,
                required=False,
                default="*.csv",
                placeholder="*.csv",
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
        self._s3 = None

    async def connect(self) -> bool:
        try:
            import boto3
            self._s3 = boto3.client(
                's3',
                aws_access_key_id=self.credentials.get("aws_access_key"),
                aws_secret_access_key=self.credentials.get("aws_secret_key"),
                region_name=self.credentials.get("region", "us-east-1"),
            )
            self._connected = True
            return True
        except ImportError:
            print("boto3 not installed. Run: pip install boto3")
            return False
        except Exception as e:
            print(f"S3 connection error: {e}")
            return False

    async def disconnect(self):
        self._s3 = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        bucket = self.credentials.get("bucket", "")
        if not bucket:
            return False, "Bucket name is required"

        try:
            import boto3
            s3 = boto3.client(
                's3',
                aws_access_key_id=self.credentials.get("aws_access_key"),
                aws_secret_access_key=self.credentials.get("aws_secret_key"),
                region_name=self.credentials.get("region", "us-east-1"),
            )
            s3.head_bucket(Bucket=bucket)
            return True, f"Connected to bucket: {bucket}"
        except ImportError:
            return False, "boto3 not installed. Run: pip install boto3"
        except Exception as e:
            return False, f"Error: {str(e)}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._s3:
            return

        import fnmatch

        bucket = self.credentials.get("bucket", "")
        prefix = self.credentials.get("prefix", "")
        pattern = self.credentials.get("file_pattern", "*.csv")
        delimiter = self.credentials.get("delimiter", ",")

        try:
            response = self._s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            objects = response.get("Contents", [])

            for obj in objects:
                key = obj["Key"]
                filename = key.split("/")[-1]

                if not fnmatch.fnmatch(filename, pattern):
                    continue

                try:
                    response = self._s3.get_object(Bucket=bucket, Key=key)
                    content = response["Body"].read()

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
                            metadata={"bucket": bucket, "key": key, "row": i},
                        )
                except Exception as e:
                    print(f"Error reading {key}: {e}")

        except Exception as e:
            print(f"S3 fetch error: {e}")
