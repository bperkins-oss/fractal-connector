"""CSV/Excel file data source plugin."""
import asyncio
import os
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator

import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class CSVFileHandler(FileSystemEventHandler):
    """Watches for file changes."""

    def __init__(self, callback):
        self.callback = callback

    def on_modified(self, event):
        if not event.is_directory:
            self.callback(event.src_path)


class CSVPlugin(DataSourcePlugin):
    """Plugin for reading CSV and Excel files."""

    plugin_id = "csv"
    plugin_name = "CSV / Excel Files"
    plugin_description = "Import data from CSV or Excel files"
    plugin_icon = "description"

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._observer = None
        self._last_data = None

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="file_path",
                label="File or Folder Path",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="C:\\Data\\mydata.csv or /home/user/data",
                help_text="Path to a CSV/Excel file or folder to watch",
            ),
            CredentialField(
                name="file_pattern",
                label="File Pattern",
                field_type=FieldType.TEXT,
                required=False,
                default="*.csv",
                placeholder="*.csv",
                help_text="Glob pattern for files (e.g., *.csv, *.xlsx)",
            ),
            CredentialField(
                name="delimiter",
                label="CSV Delimiter",
                field_type=FieldType.SELECT,
                required=False,
                default=",",
                options=[
                    {"value": ",", "label": "Comma (,)"},
                    {"value": ";", "label": "Semicolon (;)"},
                    {"value": "\t", "label": "Tab"},
                    {"value": "|", "label": "Pipe (|)"},
                ],
            ),
            CredentialField(
                name="watch_changes",
                label="Watch for changes",
                field_type=FieldType.CHECKBOX,
                required=False,
                default=True,
                help_text="Automatically sync when files change",
            ),
        ]

    async def connect(self) -> bool:
        """Start watching the file/folder."""
        file_path = self.credentials.get("file_path", "")
        watch_changes = self.credentials.get("watch_changes", True)

        if not os.path.exists(file_path):
            return False

        if watch_changes:
            watch_path = file_path if os.path.isdir(file_path) else os.path.dirname(file_path)
            self._observer = Observer()
            handler = CSVFileHandler(self._on_file_change)
            self._observer.schedule(handler, watch_path, recursive=False)
            self._observer.start()

        self._connected = True
        return True

    async def disconnect(self):
        """Stop watching."""
        if self._observer:
            self._observer.stop()
            self._observer.join()
            self._observer = None
        self._connected = False

    def _on_file_change(self, path: str):
        """Called when a watched file changes."""
        # This would trigger a sync
        pass

    async def test_connection(self) -> tuple[bool, str]:
        """Test if the file/folder exists and is readable."""
        file_path = self.credentials.get("file_path", "")

        if not file_path:
            return False, "File path is required"

        if not os.path.exists(file_path):
            return False, f"Path does not exist: {file_path}"

        if os.path.isfile(file_path):
            try:
                # Try to read the file
                if file_path.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(file_path, nrows=1)
                else:
                    delimiter = self.credentials.get("delimiter", ",")
                    df = pd.read_csv(file_path, delimiter=delimiter, nrows=1)
                return True, f"Successfully read file with {len(df.columns)} columns"
            except Exception as e:
                return False, f"Failed to read file: {str(e)}"
        else:
            # It's a directory
            pattern = self.credentials.get("file_pattern", "*.csv")
            files = list(Path(file_path).glob(pattern))
            if files:
                return True, f"Found {len(files)} matching files"
            else:
                return False, f"No files matching pattern '{pattern}'"

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        """Read and yield data from files."""
        file_path = self.credentials.get("file_path", "")
        delimiter = self.credentials.get("delimiter", ",")
        pattern = self.credentials.get("file_pattern", "*.csv")

        files_to_read = []

        if os.path.isfile(file_path):
            files_to_read = [Path(file_path)]
        else:
            files_to_read = list(Path(file_path).glob(pattern))

        for file in files_to_read:
            try:
                if str(file).endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(file)
                else:
                    df = pd.read_csv(file, delimiter=delimiter)

                # Convert to records
                records = df.to_dict(orient='records')

                for i, record in enumerate(records):
                    yield DataRecord(
                        source_id=self.source_id,
                        source_type=self.plugin_id,
                        timestamp=datetime.utcnow().isoformat(),
                        data=record,
                        metadata={
                            "file": str(file),
                            "row_index": i,
                            "total_rows": len(records),
                        }
                    )

            except Exception as e:
                # Log error but continue with other files
                print(f"Error reading {file}: {e}")
