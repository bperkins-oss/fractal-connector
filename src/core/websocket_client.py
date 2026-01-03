"""WebSocket client for Fractal Cloud communication."""
import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Callable, Optional

import websockets
from websockets.client import WebSocketClientProtocol

logger = logging.getLogger(__name__)


class FractalClient:
    """WebSocket client for communicating with Fractal Cloud."""

    def __init__(
        self,
        url: str,
        api_key: str,
        on_message: Optional[Callable[[dict], None]] = None,
        on_connect: Optional[Callable[[], None]] = None,
        on_disconnect: Optional[Callable[[], None]] = None,
    ):
        self.url = url
        self.api_key = api_key
        self.on_message = on_message
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect

        self._ws: Optional[WebSocketClientProtocol] = None
        self._connected = False
        self._reconnect_task: Optional[asyncio.Task] = None
        self._receive_task: Optional[asyncio.Task] = None
        self._running = False
        self._reconnect_delay = 1  # Start with 1 second

    @property
    def is_connected(self) -> bool:
        """Check if connected to Fractal."""
        return self._connected and self._ws is not None

    async def connect(self) -> bool:
        """
        Connect to Fractal Cloud.

        Returns:
            True if connection successful
        """
        try:
            headers = {
                'Authorization': f'Bearer {self.api_key}',
                'X-Client-Type': 'fractal-connector',
                'X-Client-Version': '1.0.0',
            }

            self._ws = await websockets.connect(
                self.url,
                additional_headers=headers,
                ping_interval=30,
                ping_timeout=10,
            )

            self._connected = True
            self._reconnect_delay = 1  # Reset delay on successful connect
            logger.info(f"Connected to Fractal at {self.url}")

            # Send authentication message
            await self._send_auth()

            if self.on_connect:
                self.on_connect()

            return True

        except Exception as e:
            logger.error(f"Failed to connect to Fractal: {e}")
            self._connected = False
            return False

    async def _send_auth(self):
        """Send authentication message after connecting."""
        auth_msg = {
            'type': 'auth',
            'api_key': self.api_key,
            'client_info': {
                'type': 'connector',
                'version': '1.0.0',
                'timestamp': datetime.utcnow().isoformat(),
            }
        }
        await self.send(auth_msg)

    async def disconnect(self):
        """Disconnect from Fractal Cloud."""
        self._running = False

        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass

        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass

        if self._ws:
            await self._ws.close()
            self._ws = None

        self._connected = False

        if self.on_disconnect:
            self.on_disconnect()

        logger.info("Disconnected from Fractal")

    async def send(self, message: dict[str, Any]) -> bool:
        """
        Send a message to Fractal.

        Args:
            message: Dictionary to send as JSON

        Returns:
            True if sent successfully
        """
        if not self.is_connected:
            logger.warning("Cannot send message: not connected")
            return False

        try:
            await self._ws.send(json.dumps(message))
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False

    async def send_data(self, source_id: str, source_type: str, data: dict[str, Any], metadata: Optional[dict] = None):
        """
        Send data from a source to Fractal.

        Args:
            source_id: ID of the data source
            source_type: Type of data source (plugin_id)
            data: The actual data to send
            metadata: Optional metadata about the data
        """
        message = {
            'type': 'data',
            'source_id': source_id,
            'source_type': source_type,
            'timestamp': datetime.utcnow().isoformat(),
            'data': data,
            'metadata': metadata or {},
        }
        await self.send(message)

    async def send_status(self, status: str, details: Optional[dict] = None):
        """Send status update to Fractal."""
        message = {
            'type': 'status',
            'status': status,
            'timestamp': datetime.utcnow().isoformat(),
            'details': details or {},
        }
        await self.send(message)

    async def _receive_loop(self):
        """Background task to receive messages."""
        while self._running and self.is_connected:
            try:
                message = await self._ws.recv()
                data = json.loads(message)

                logger.debug(f"Received message: {data.get('type', 'unknown')}")

                if self.on_message:
                    self.on_message(data)

            except websockets.ConnectionClosed:
                logger.warning("Connection closed by server")
                self._connected = False
                break
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON received: {e}")
            except Exception as e:
                logger.error(f"Error receiving message: {e}")
                break

        # Trigger reconnect if we were running
        if self._running:
            asyncio.create_task(self._reconnect())

    async def _reconnect(self):
        """Attempt to reconnect with exponential backoff."""
        while self._running and not self._connected:
            logger.info(f"Attempting reconnect in {self._reconnect_delay}s...")
            await asyncio.sleep(self._reconnect_delay)

            if await self.connect():
                self._receive_task = asyncio.create_task(self._receive_loop())
                break

            # Exponential backoff with max of 60 seconds
            self._reconnect_delay = min(self._reconnect_delay * 2, 60)

    async def start(self):
        """Start the client with automatic reconnection."""
        self._running = True

        if await self.connect():
            self._receive_task = asyncio.create_task(self._receive_loop())
        else:
            # Start reconnection attempts
            asyncio.create_task(self._reconnect())

    async def stop(self):
        """Stop the client."""
        await self.disconnect()
