"""Configuration manager for Fractal Connector."""
import json
import os
from pathlib import Path
from typing import Any, Optional
from cryptography.fernet import Fernet
from pydantic import BaseModel


class FractalConfig(BaseModel):
    """Main configuration model."""
    fractal_url: str = "wss://fractal.example.com/ws"
    api_key: str = ""
    auto_connect: bool = True
    sync_interval: int = 30  # seconds


class DataSourceConfig(BaseModel):
    """Configuration for a data source."""
    id: str
    plugin_type: str
    name: str
    credentials: dict[str, Any] = {}
    enabled: bool = True
    sync_interval: int = 60  # seconds


class ConfigManager:
    """Manages configuration and secure credential storage."""

    def __init__(self, config_dir: Optional[Path] = None):
        if config_dir is None:
            # Default to user's app data directory
            if os.name == 'nt':  # Windows
                config_dir = Path(os.environ.get('APPDATA', '')) / 'FractalConnector'
            else:  # Linux/Mac
                config_dir = Path.home() / '.fractal-connector'

        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(parents=True, exist_ok=True)

        self.config_file = self.config_dir / 'config.json'
        self.key_file = self.config_dir / '.key'

        self._fernet = self._get_or_create_key()
        self._config: Optional[FractalConfig] = None
        self._data_sources: list[DataSourceConfig] = []

        self._load_config()

    def _get_or_create_key(self) -> Fernet:
        """Get or create encryption key for credentials."""
        if self.key_file.exists():
            key = self.key_file.read_bytes()
        else:
            key = Fernet.generate_key()
            self.key_file.write_bytes(key)
            # Restrict permissions on key file
            if os.name != 'nt':
                os.chmod(self.key_file, 0o600)
        return Fernet(key)

    def _encrypt(self, value: str) -> str:
        """Encrypt a string value."""
        return self._fernet.encrypt(value.encode()).decode()

    def _decrypt(self, value: str) -> str:
        """Decrypt a string value."""
        try:
            return self._fernet.decrypt(value.encode()).decode()
        except Exception:
            return value  # Return as-is if decryption fails

    def _load_config(self):
        """Load configuration from file."""
        if self.config_file.exists():
            try:
                data = json.loads(self.config_file.read_text())
                self._config = FractalConfig(**data.get('fractal', {}))

                # Load data sources with decrypted credentials
                self._data_sources = []
                for ds_data in data.get('data_sources', []):
                    # Decrypt credential values
                    if 'credentials' in ds_data:
                        for key, value in ds_data['credentials'].items():
                            if isinstance(value, str) and value.startswith('enc:'):
                                ds_data['credentials'][key] = self._decrypt(value[4:])
                    self._data_sources.append(DataSourceConfig(**ds_data))
            except Exception as e:
                print(f"Error loading config: {e}")
                self._config = FractalConfig()
                self._data_sources = []
        else:
            self._config = FractalConfig()
            self._data_sources = []

    def _save_config(self):
        """Save configuration to file."""
        data = {
            'fractal': self._config.model_dump(),
            'data_sources': []
        }

        # Encrypt sensitive credential fields before saving
        for ds in self._data_sources:
            ds_data = ds.model_dump()
            encrypted_creds = {}
            for key, value in ds_data.get('credentials', {}).items():
                # Encrypt values that look like secrets
                if key.lower() in ('password', 'api_key', 'token', 'secret', 'key'):
                    encrypted_creds[key] = 'enc:' + self._encrypt(str(value))
                else:
                    encrypted_creds[key] = value
            ds_data['credentials'] = encrypted_creds
            data['data_sources'].append(ds_data)

        self.config_file.write_text(json.dumps(data, indent=2))

    @property
    def fractal(self) -> FractalConfig:
        """Get Fractal configuration."""
        return self._config

    @fractal.setter
    def fractal(self, config: FractalConfig):
        """Set Fractal configuration."""
        self._config = config
        self._save_config()

    @property
    def data_sources(self) -> list[DataSourceConfig]:
        """Get all data source configurations."""
        return self._data_sources

    def get_data_source(self, source_id: str) -> Optional[DataSourceConfig]:
        """Get a specific data source by ID."""
        for ds in self._data_sources:
            if ds.id == source_id:
                return ds
        return None

    def add_data_source(self, config: DataSourceConfig):
        """Add a new data source."""
        # Remove existing with same ID
        self._data_sources = [ds for ds in self._data_sources if ds.id != config.id]
        self._data_sources.append(config)
        self._save_config()

    def remove_data_source(self, source_id: str):
        """Remove a data source by ID."""
        self._data_sources = [ds for ds in self._data_sources if ds.id != source_id]
        self._save_config()

    def update_fractal_url(self, url: str):
        """Update the Fractal server URL."""
        self._config.fractal_url = url
        self._save_config()

    def update_api_key(self, api_key: str):
        """Update the API key."""
        self._config.api_key = api_key
        self._save_config()
