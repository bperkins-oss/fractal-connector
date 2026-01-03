"""Base class for data source plugins."""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncIterator, Optional


class FieldType(str, Enum):
    """Types of credential input fields."""
    TEXT = "text"
    PASSWORD = "password"
    NUMBER = "number"
    SELECT = "select"
    CHECKBOX = "checkbox"
    FILE = "file"
    FOLDER = "folder"


@dataclass
class CredentialField:
    """Definition of a credential input field."""
    name: str
    label: str
    field_type: FieldType = FieldType.TEXT
    required: bool = True
    default: Any = None
    placeholder: str = ""
    help_text: str = ""
    options: list[dict[str, str]] = field(default_factory=list)  # For SELECT type

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'name': self.name,
            'label': self.label,
            'type': self.field_type.value,
            'required': self.required,
            'default': self.default,
            'placeholder': self.placeholder,
            'help_text': self.help_text,
            'options': self.options,
        }


@dataclass
class DataRecord:
    """A single data record to send to Fractal."""
    source_id: str
    source_type: str
    timestamp: str
    data: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)


class DataSourcePlugin(ABC):
    """Base class for all data source plugins."""

    # Plugin metadata - override in subclasses
    plugin_id: str = "base"
    plugin_name: str = "Base Plugin"
    plugin_description: str = "Base data source plugin"
    plugin_icon: str = "extension"  # Material icon name

    def __init__(self, source_id: str, credentials: dict[str, Any]):
        """
        Initialize the plugin.

        Args:
            source_id: Unique identifier for this data source instance
            credentials: Dictionary of credential values
        """
        self.source_id = source_id
        self.credentials = credentials
        self._connected = False

    @classmethod
    @abstractmethod
    def get_credential_fields(cls) -> list[CredentialField]:
        """
        Return the list of credential fields required by this plugin.
        The UI will dynamically render these fields.
        """
        pass

    @classmethod
    def validate_credentials(cls, credentials: dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate the provided credentials.

        Returns:
            Tuple of (is_valid, error_message)
        """
        for cred_field in cls.get_credential_fields():
            if cred_field.required and cred_field.name not in credentials:
                return False, f"Missing required field: {cred_field.label}"
            if cred_field.required and not credentials.get(cred_field.name):
                return False, f"Field '{cred_field.label}' cannot be empty"
        return True, None

    @abstractmethod
    async def connect(self) -> bool:
        """
        Establish connection to the data source.

        Returns:
            True if connection successful, False otherwise
        """
        pass

    @abstractmethod
    async def disconnect(self):
        """Disconnect from the data source."""
        pass

    @abstractmethod
    async def test_connection(self) -> tuple[bool, str]:
        """
        Test the connection with current credentials.

        Returns:
            Tuple of (success, message)
        """
        pass

    @abstractmethod
    async def fetch_data(self) -> AsyncIterator[DataRecord]:
        """
        Fetch data from the source.

        Yields:
            DataRecord objects to be sent to Fractal
        """
        pass

    @property
    def is_connected(self) -> bool:
        """Check if currently connected."""
        return self._connected

    def get_status(self) -> dict[str, Any]:
        """Get current plugin status."""
        return {
            'source_id': self.source_id,
            'plugin_id': self.plugin_id,
            'plugin_name': self.plugin_name,
            'connected': self._connected,
        }
