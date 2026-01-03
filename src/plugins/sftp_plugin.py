"""FTP/SFTP data source plugin."""
import io
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator

import pandas as pd

from .base import DataSourcePlugin, CredentialField, FieldType, DataRecord


class SFTPPlugin(DataSourcePlugin):
    """Plugin for FTP/SFTP file sources."""

    plugin_id = "sftp"
    plugin_name = "FTP / SFTP"
    plugin_description = "Connect to FTP or SFTP servers for file data"
    plugin_icon = "cloud_download"

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        super().__init__(source_id, credentials)
        self._transport = None
        self._sftp = None
        self._ftp = None

    @classmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        return [
            CredentialField(
                name="protocol",
                label="Protocol",
                field_type=FieldType.SELECT,
                required=True,
                default="sftp",
                options=[
                    {"value": "sftp", "label": "SFTP (SSH)"},
                    {"value": "ftp", "label": "FTP"},
                    {"value": "ftps", "label": "FTPS (FTP over TLS)"},
                ],
            ),
            CredentialField(
                name="host",
                label="Host",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="ftp.example.com",
            ),
            CredentialField(
                name="port",
                label="Port",
                field_type=FieldType.NUMBER,
                required=False,
                placeholder="22 (SFTP) or 21 (FTP)",
                help_text="Leave empty for default port",
            ),
            CredentialField(
                name="username",
                label="Username",
                field_type=FieldType.TEXT,
                required=True,
            ),
            CredentialField(
                name="auth_type",
                label="Authentication",
                field_type=FieldType.SELECT,
                required=True,
                default="password",
                options=[
                    {"value": "password", "label": "Password"},
                    {"value": "key", "label": "SSH Key (SFTP only)"},
                ],
            ),
            CredentialField(
                name="password",
                label="Password",
                field_type=FieldType.PASSWORD,
                required=False,
            ),
            CredentialField(
                name="private_key_path",
                label="Private Key Path",
                field_type=FieldType.TEXT,
                required=False,
                placeholder="/path/to/id_rsa",
                help_text="For SSH key authentication",
            ),
            CredentialField(
                name="remote_path",
                label="Remote Path",
                field_type=FieldType.TEXT,
                required=True,
                placeholder="/data/files/",
                help_text="Directory or file path on remote server",
            ),
            CredentialField(
                name="file_pattern",
                label="File Pattern",
                field_type=FieldType.TEXT,
                required=False,
                default="*.csv",
                placeholder="*.csv",
                help_text="Glob pattern to match files",
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
        ]

    async def connect(self) -> bool:
        """Connect to FTP/SFTP server."""
        protocol = self.credentials.get("protocol", "sftp")
        host = self.credentials.get("host", "")
        port = self.credentials.get("port", "")
        username = self.credentials.get("username", "")
        auth_type = self.credentials.get("auth_type", "password")
        password = self.credentials.get("password", "")
        private_key_path = self.credentials.get("private_key_path", "")

        try:
            if protocol == "sftp":
                import paramiko

                default_port = 22
                port = int(port) if port else default_port

                self._transport = paramiko.Transport((host, port))

                if auth_type == "key" and private_key_path:
                    private_key = paramiko.RSAKey.from_private_key_file(private_key_path)
                    self._transport.connect(username=username, pkey=private_key)
                else:
                    self._transport.connect(username=username, password=password)

                self._sftp = paramiko.SFTPClient.from_transport(self._transport)

            else:  # FTP or FTPS
                from ftplib import FTP, FTP_TLS

                default_port = 21
                port = int(port) if port else default_port

                if protocol == "ftps":
                    self._ftp = FTP_TLS()
                else:
                    self._ftp = FTP()

                self._ftp.connect(host, port)
                self._ftp.login(username, password)

                if protocol == "ftps":
                    self._ftp.prot_p()  # Enable data channel encryption

            self._connected = True
            return True

        except ImportError as e:
            print(f"Missing library: {e}")
            return False
        except Exception as e:
            print(f"FTP/SFTP connection error: {e}")
            return False

    async def disconnect(self):
        """Disconnect from server."""
        if self._sftp:
            self._sftp.close()
            self._sftp = None
        if self._transport:
            self._transport.close()
            self._transport = None
        if self._ftp:
            try:
                self._ftp.quit()
            except:
                pass
            self._ftp = None
        self._connected = False

    async def test_connection(self) -> tuple[bool, str]:
        """Test connection."""
        host = self.credentials.get("host", "")
        username = self.credentials.get("username", "")
        remote_path = self.credentials.get("remote_path", "")

        if not host:
            return False, "Host is required"
        if not username:
            return False, "Username is required"
        if not remote_path:
            return False, "Remote path is required"

        try:
            if await self.connect():
                # Try to list directory
                protocol = self.credentials.get("protocol", "sftp")

                if protocol == "sftp":
                    files = self._sftp.listdir(remote_path)
                else:
                    self._ftp.cwd(remote_path)
                    files = self._ftp.nlst()

                await self.disconnect()
                return True, f"Connected! Found {len(files)} items in {remote_path}"
            else:
                return False, "Connection failed"

        except Exception as e:
            await self.disconnect()
            return False, f"Error: {str(e)[:100]}"

    def _match_pattern(self, filename: str, pattern: str) -> bool:
        """Check if filename matches glob pattern."""
        import fnmatch
        return fnmatch.fnmatch(filename, pattern)

    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        """Fetch files from FTP/SFTP."""
        if not self._sftp and not self._ftp:
            return

        remote_path = self.credentials.get("remote_path", "")
        file_pattern = self.credentials.get("file_pattern", "*.csv")
        delimiter = self.credentials.get("delimiter", ",")
        protocol = self.credentials.get("protocol", "sftp")

        try:
            # List files
            if protocol == "sftp":
                files = self._sftp.listdir(remote_path)
            else:
                self._ftp.cwd(remote_path)
                files = self._ftp.nlst()

            # Filter by pattern
            matching_files = [f for f in files if self._match_pattern(f, file_pattern)]

            for filename in matching_files:
                full_path = f"{remote_path.rstrip('/')}/{filename}"

                try:
                    # Download file content
                    if protocol == "sftp":
                        with io.BytesIO() as buffer:
                            self._sftp.getfo(full_path, buffer)
                            buffer.seek(0)
                            content = buffer.read()
                    else:
                        buffer = io.BytesIO()
                        self._ftp.retrbinary(f"RETR {filename}", buffer.write)
                        buffer.seek(0)
                        content = buffer.read()

                    # Parse CSV/Excel
                    if filename.endswith(('.xlsx', '.xls')):
                        df = pd.read_excel(io.BytesIO(content))
                    else:
                        df = pd.read_csv(io.BytesIO(content), delimiter=delimiter)

                    records = df.to_dict(orient='records')

                    for i, record in enumerate(records):
                        yield DataRecord(
                            source_id=self.source_id,
                            source_type=self.plugin_id,
                            timestamp=datetime.utcnow().isoformat(),
                            data=record,
                            metadata={
                                "file": filename,
                                "row_index": i,
                                "total_rows": len(records),
                            },
                        )

                except Exception as e:
                    print(f"Error processing {filename}: {e}")

        except Exception as e:
            print(f"FTP/SFTP fetch error: {e}")
