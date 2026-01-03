"""Google Sheets plugin."""
from datetime import datetime
from typing import Any, AsyncIterator

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class GoogleSheetsPlugin(DataSourcePlugin):
    """Plugin for Google Sheets."""

    plugin_id = "google_sheets"
    plugin_name = "Google Sheets"
    plugin_description = "Read data from Google Sheets"
    plugin_icon = "table_chart"

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="credentials_json",
                label="Service Account JSON",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="/path/to/service-account.json",
                help_text="Path to Google service account credentials",
            ),
            CredentialField(
                name="spreadsheet_id",
                label="Spreadsheet ID",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
                help_text="ID from the spreadsheet URL",
            ),
            CredentialField(
                name="sheet_name",
                label="Sheet Name",
                field_type=FieldType.TEXT,
                required=False,
                default="Sheet1",
                placeholder="Sheet1",
            ),
            CredentialField(
                name="range",
                label="Cell Range",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="A1:Z1000",
                help_text="Leave empty for entire sheet",
            ),
            CredentialField(
                name="header_row",
                label="Has Header Row",
                field_type=FieldType.CHECKBOX,
                required=False,
                default=True,
            ),
        ]

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._service = None

    async def connect(self) -> bool:
        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build

            creds_path = self.credentials.get("credentials_json", "")
            credentials = service_account.Credentials.from_service_account_file(
                creds_path,
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
            )
            self._service = build("sheets", "v4", credentials=credentials)
            self._connected = True
            return True
        except ImportError:
            print("google-api-python-client not installed")
            return False
        except Exception as e:
            print(f"Google Sheets connection error: {e}")
            return False

    async def disconnect(self):
        self._service = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        creds_path = self.credentials.get("credentials_json", "")
        spreadsheet_id = self.credentials.get("spreadsheet_id", "")

        if not creds_path:
            return False, "Credentials JSON path is required"
        if not spreadsheet_id:
            return False, "Spreadsheet ID is required"

        try:
            if await self.connect():
                spreadsheet = self._service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
                title = spreadsheet.get("properties", {}).get("title", "Unknown")
                await self.disconnect()
                return True, f"Connected to: {title}"
            return False, "Connection failed"
        except ImportError:
            return False, "google-api-python-client not installed"
        except Exception as e:
            return False, f"Error: {str(e)[:100]}"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        if not self._service:
            return

        spreadsheet_id = self.credentials.get("spreadsheet_id", "")
        sheet_name = self.credentials.get("sheet_name", "Sheet1")
        cell_range = self.credentials.get("range", "")
        has_header = self.credentials.get("header_row", True)

        range_notation = f"{sheet_name}!{cell_range}" if cell_range else sheet_name

        try:
            result = self._service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_notation,
            ).execute()

            values = result.get("values", [])
            if not values:
                return

            if has_header:
                headers = values[0]
                data_rows = values[1:]
            else:
                headers = [f"col_{i}" for i in range(len(values[0]))]
                data_rows = values

            for i, row in enumerate(data_rows):
                record = {}
                for j, header in enumerate(headers):
                    record[header] = row[j] if j < len(row) else None

                yield DataRecord(
                    source_id=self.source_id,
                    source_type=self.plugin_id,
                    timestamp=datetime.utcnow().isoformat(),
                    data=record,
                    metadata={"sheet": sheet_name, "row": i + 2 if has_header else i + 1},
                )

        except Exception as e:
            print(f"Google Sheets fetch error: {e}")
