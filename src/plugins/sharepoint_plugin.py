"""SharePoint / OneDrive plugin."""
from datetime import datetime
from typing import Any, AsyncIterator
import io

import pandas as pd
import aiohttp

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class SharePointPlugin(DataSourcePlugin):
    """Plugin for SharePoint / OneDrive."""

    plugin_id = "sharepoint"
    plugin_name = "SharePoint / OneDrive"
    plugin_description = "Read files from SharePoint or OneDrive"
    plugin_icon = "folder_shared"

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="tenant_id",
                label="Tenant ID",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="your-tenant-id",
            ),
            CredentialField(
                name="client_id",
                label="Client ID (App ID)",
                field_type=FieldType.TEXT,
                required=True,
            ),
            CredentialField(
                name="client_secret",
                label="Client Secret",
                field_type=FieldType.PASSWORD,
                required=True,
            ),
            CredentialField(
                name="site_url",
                label="SharePoint Site URL",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="https://company.sharepoint.com/sites/mysite",
                help_text="Leave empty for OneDrive",
            ),
            CredentialField(
                name="drive_id",
                label="Drive ID",
                field_type=FieldType.TEXT,
                required=False,
                help_text="Optional - will use default drive if empty",
            ),
            CredentialField(
                name="folder_path",
                label="Folder Path",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="/Documents/Data",
            ),
            CredentialField(
                name="file_pattern",
                label="File Pattern",
                field_type=FieldType.TEXT,
                required=False,
                default="*.csv",
            ),
        ]

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._session = None
        self._access_token = None

    async def _get_token(self) -> str | None:
        tenant_id = self.credentials.get("tenant_id", "")
        client_id = self.credentials.get("client_id", "")
        client_secret = self.credentials.get("client_secret", "")

        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get("access_token")
        except Exception as e:
            print(f"Token error: {e}")
        return None

    async def connect(self) -> bool:
        self._access_token = await self._get_token()
        if self._access_token:
            self._session = aiohttp.ClientSession()
            self._connected = True
            return True
        return False

    async def disconnect(self):
        if self._session:
            await self._session.close()
        self._session = None
        self._access_token = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        tenant_id = self.credentials.get("tenant_id", "")
        client_id = self.credentials.get("client_id", "")

        if not tenant_id:
            return False, "Tenant ID is required"
        if not client_id:
            return False, "Client ID is required"

        token = await self._get_token()
        if token:
            return True, "Successfully authenticated with Microsoft Graph"
        return False, "Authentication failed"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._session or not self._access_token:
            return

        import fnmatch

        site_url = self.credentials.get("site_url", "")
        drive_id = self.credentials.get("drive_id", "")
        folder_path = self.credentials.get("folder_path", "").strip("/")
        pattern = self.credentials.get("file_pattern", "*.csv")

        headers = {"Authorization": f"Bearer {self._access_token}"}

        # Build the API URL
        if site_url:
            # SharePoint
            base = f"https://graph.microsoft.com/v1.0/sites/{site_url.replace('https://', '').replace('/', ':')}"
            if drive_id:
                drive_url = f"{base}/drives/{drive_id}"
            else:
                drive_url = f"{base}/drive"
        else:
            # OneDrive
            drive_url = "https://graph.microsoft.com/v1.0/me/drive"

        # List files
        if folder_path:
            list_url = f"{drive_url}/root:/{folder_path}:/children"
        else:
            list_url = f"{drive_url}/root/children"

        try:
            async with self._session.get(list_url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    files = data.get("value", [])

                    for file_info in files:
                        name = file_info.get("name", "")
                        if not fnmatch.fnmatch(name, pattern):
                            continue

                        download_url = file_info.get("@microsoft.graph.downloadUrl")
                        if not download_url:
                            continue

                        async with self._session.get(download_url) as file_response:
                            if file_response.status == 200:
                                content = await file_response.read()

                                try:
                                    if name.endswith(('.xlsx', '.xls')):
                                        df = pd.read_excel(io.BytesIO(content))
                                    else:
                                        df = pd.read_csv(io.BytesIO(content))

                                    for i, row in df.iterrows():
                                        yield DataRecord(
                                            source_id=self.source_id,
                                            source_type=self.plugin_id,
                                            timestamp=datetime.utcnow().isoformat(),
                                            data=row.to_dict(),
                                            metadata={"file": name, "row": i},
                                        )
                                except Exception as e:
                                    print(f"Error reading {name}: {e}")

        except Exception as e:
            print(f"SharePoint fetch error: {e}")
