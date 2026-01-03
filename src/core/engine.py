"""Main connector engine that orchestrates data sources and Fractal communication."""
import asyncio
import logging
from typing import Any, Optional, Type

from .config import ConfigManager, DataSourceConfig
from .websocket_client import FractalClient
from ..plugins.base import DataSourcePlugin, DataRecord

logger = logging.getLogger(__name__)


class ConnectorEngine:
    """Main engine that manages plugins and coordinates data flow to Fractal."""

    def __init__(self, config: ConfigManager):
        self.config = config
        self._plugin_registry: dict[str, Type[DataSourcePlugin]] = {}
        self._active_plugins: dict[str, DataSourcePlugin] = {}
        self._sync_tasks: dict[str, asyncio.Task] = {}
        self._fractal_client: Optional[FractalClient] = None
        self._running = False
        self._status_callbacks: list[callable] = []

    def register_plugin(self, plugin_class: Type[DataSourcePlugin]):
        """Register a data source plugin type."""
        self._plugin_registry[plugin_class.plugin_id] = plugin_class
        logger.info(f"Registered plugin: {plugin_class.plugin_name}")

    def get_registered_plugins(self) -> list[dict[str, Any]]:
        """Get list of all registered plugin types with their credential fields."""
        plugins = []
        for plugin_id, plugin_class in self._plugin_registry.items():
            plugins.append({
                'id': plugin_class.plugin_id,
                'name': plugin_class.plugin_name,
                'description': plugin_class.plugin_description,
                'icon': plugin_class.plugin_icon,
                'credential_fields': [f.to_dict() for f in plugin_class.get_credential_fields()],
            })
        return plugins

    def on_status_change(self, callback: callable):
        """Register a callback for status changes."""
        self._status_callbacks.append(callback)

    def _notify_status(self, status: str, details: Optional[dict] = None):
        """Notify all status callbacks."""
        for callback in self._status_callbacks:
            try:
                callback(status, details or {})
            except Exception as e:
                logger.error(f"Status callback error: {e}")

    async def _on_fractal_message(self, message: dict):
        """Handle incoming messages from Fractal."""
        msg_type = message.get('type')

        if msg_type == 'command':
            await self._handle_command(message)
        elif msg_type == 'config_update':
            await self._handle_config_update(message)
        elif msg_type == 'ping':
            await self._fractal_client.send({'type': 'pong'})

    async def _handle_command(self, message: dict):
        """Handle command from Fractal."""
        command = message.get('command')
        source_id = message.get('source_id')

        if command == 'sync_now' and source_id:
            await self._trigger_sync(source_id)
        elif command == 'reconnect' and source_id:
            await self._reconnect_source(source_id)

    async def _handle_config_update(self, message: dict):
        """Handle configuration update from Fractal."""
        # Could be used for remote configuration updates
        pass

    async def start(self):
        """Start the connector engine."""
        self._running = True
        self._notify_status('starting')

        # Initialize Fractal client
        self._fractal_client = FractalClient(
            url=self.config.fractal.fractal_url,
            api_key=self.config.fractal.api_key,
            on_message=lambda m: asyncio.create_task(self._on_fractal_message(m)),
            on_connect=lambda: self._notify_status('connected'),
            on_disconnect=lambda: self._notify_status('disconnected'),
        )

        # Connect to Fractal
        await self._fractal_client.start()

        # Initialize configured data sources
        for ds_config in self.config.data_sources:
            if ds_config.enabled:
                await self._start_data_source(ds_config)

        self._notify_status('running')
        logger.info("Connector engine started")

    async def stop(self):
        """Stop the connector engine."""
        self._running = False
        self._notify_status('stopping')

        # Stop all sync tasks
        for task in self._sync_tasks.values():
            task.cancel()

        # Disconnect all plugins
        for plugin in self._active_plugins.values():
            try:
                await plugin.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting plugin {plugin.source_id}: {e}")

        # Disconnect from Fractal
        if self._fractal_client:
            await self._fractal_client.stop()

        self._active_plugins.clear()
        self._sync_tasks.clear()

        self._notify_status('stopped')
        logger.info("Connector engine stopped")

    async def _start_data_source(self, ds_config: DataSourceConfig):
        """Start a data source plugin."""
        plugin_class = self._plugin_registry.get(ds_config.plugin_type)
        if not plugin_class:
            logger.error(f"Unknown plugin type: {ds_config.plugin_type}")
            return False

        try:
            # Create plugin instance
            plugin = plugin_class(
                source_id=ds_config.id,
                credentials=ds_config.credentials,
            )

            # Connect to data source
            if await plugin.connect():
                self._active_plugins[ds_config.id] = plugin

                # Start sync task
                self._sync_tasks[ds_config.id] = asyncio.create_task(
                    self._sync_loop(ds_config.id, ds_config.sync_interval)
                )

                logger.info(f"Started data source: {ds_config.name}")
                return True
            else:
                logger.error(f"Failed to connect to data source: {ds_config.name}")
                return False

        except Exception as e:
            logger.error(f"Error starting data source {ds_config.name}: {e}")
            return False

    async def _sync_loop(self, source_id: str, interval: int):
        """Background task to periodically sync data from a source."""
        while self._running and source_id in self._active_plugins:
            try:
                await self._trigger_sync(source_id)
            except Exception as e:
                logger.error(f"Sync error for {source_id}: {e}")

            await asyncio.sleep(interval)

    async def _trigger_sync(self, source_id: str):
        """Trigger a data sync for a specific source."""
        plugin = self._active_plugins.get(source_id)
        if not plugin:
            return

        try:
            async for record in plugin.fetch_data():
                if self._fractal_client and self._fractal_client.is_connected:
                    await self._fractal_client.send_data(
                        source_id=record.source_id,
                        source_type=record.source_type,
                        data=record.data,
                        metadata=record.metadata,
                    )
        except Exception as e:
            logger.error(f"Error fetching data from {source_id}: {e}")

    async def _reconnect_source(self, source_id: str):
        """Reconnect a specific data source."""
        if source_id in self._active_plugins:
            plugin = self._active_plugins[source_id]
            await plugin.disconnect()
            await plugin.connect()

    async def add_data_source(self, plugin_type: str, name: str, credentials: dict[str, Any]) -> tuple[bool, str]:
        """
        Add and start a new data source.

        Returns:
            Tuple of (success, message_or_source_id)
        """
        plugin_class = self._plugin_registry.get(plugin_type)
        if not plugin_class:
            return False, f"Unknown plugin type: {plugin_type}"

        # Validate credentials
        valid, error = plugin_class.validate_credentials(credentials)
        if not valid:
            return False, error

        # Generate unique ID
        import uuid
        source_id = str(uuid.uuid4())[:8]

        # Create config
        ds_config = DataSourceConfig(
            id=source_id,
            plugin_type=plugin_type,
            name=name,
            credentials=credentials,
            enabled=True,
        )

        # Test connection first
        test_plugin = plugin_class(source_id=source_id, credentials=credentials)
        success, message = await test_plugin.test_connection()

        if not success:
            return False, f"Connection test failed: {message}"

        # Save config
        self.config.add_data_source(ds_config)

        # Start if engine is running
        if self._running:
            await self._start_data_source(ds_config)

        return True, source_id

    async def remove_data_source(self, source_id: str) -> bool:
        """Remove a data source."""
        # Stop if running
        if source_id in self._sync_tasks:
            self._sync_tasks[source_id].cancel()
            del self._sync_tasks[source_id]

        if source_id in self._active_plugins:
            await self._active_plugins[source_id].disconnect()
            del self._active_plugins[source_id]

        # Remove config
        self.config.remove_data_source(source_id)

        return True

    async def test_data_source(self, plugin_type: str, credentials: dict[str, Any]) -> tuple[bool, str]:
        """Test connection to a data source without saving."""
        plugin_class = self._plugin_registry.get(plugin_type)
        if not plugin_class:
            return False, f"Unknown plugin type: {plugin_type}"

        valid, error = plugin_class.validate_credentials(credentials)
        if not valid:
            return False, error

        plugin = plugin_class(source_id="test", credentials=credentials)
        return await plugin.test_connection()

    def get_status(self) -> dict[str, Any]:
        """Get current engine status."""
        return {
            'running': self._running,
            'fractal_connected': self._fractal_client.is_connected if self._fractal_client else False,
            'active_sources': [
                plugin.get_status() for plugin in self._active_plugins.values()
            ],
            'registered_plugins': list(self._plugin_registry.keys()),
        }
