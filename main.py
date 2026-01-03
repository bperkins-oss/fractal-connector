#!/usr/bin/env python3
"""
Fractal Connector - Universal Data Agent
Connects local data sources to Fractal Cloud

Production-ready features:
- Password-protected web UI
- File logging with rotation
- Offline data queue
- Health check endpoints
- Windows Service support
"""
import argparse
import asyncio
import logging
import signal
import sys
import webbrowser
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Initialize logging FIRST
from src.core.logging_config import setup_logging

from src.core import ConfigManager, ConnectorEngine

# === FILE-BASED PLUGINS ===
from src.plugins.csv_plugin import CSVPlugin
from src.plugins.sftp_plugin import SFTPPlugin

# === API PLUGINS ===
from src.plugins.rest_plugin import RESTPlugin

# === DATABASE PLUGINS ===
from src.plugins.database_plugin import DatabasePlugin
from src.plugins.mssql_plugin import MSSQLPlugin
from src.plugins.postgresql_plugin import PostgreSQLPlugin
from src.plugins.mongodb_plugin import MongoDBPlugin
from src.plugins.snowflake_plugin import SnowflakePlugin
from src.plugins.oracle_plugin import OraclePlugin
from src.plugins.elasticsearch_plugin import ElasticsearchPlugin
from src.plugins.redis_plugin import RedisPlugin

# === CLOUD STORAGE PLUGINS ===
from src.plugins.s3_plugin import S3Plugin
from src.plugins.gcs_plugin import GCSPlugin
from src.plugins.azure_blob_plugin import AzureBlobPlugin

# === PRODUCTIVITY PLUGINS ===
from src.plugins.google_sheets_plugin import GoogleSheetsPlugin
from src.plugins.sharepoint_plugin import SharePointPlugin

# === MESSAGING/STREAMING PLUGINS ===
from src.plugins.kafka_plugin import KafkaPlugin

# === TRADING / MARKET DATA PLUGINS ===
from src.plugins.bloomberg_plugin import BloombergPlugin
from src.plugins.ice_connect_plugin import ICEConnectPlugin
from src.plugins.refinitiv_plugin import RefinitivPlugin
from src.plugins.interactive_brokers_plugin import InteractiveBrokersPlugin
from src.plugins.factset_plugin import FactSetPlugin
from src.plugins.quandl_plugin import QuandlPlugin
from src.plugins.alpha_vantage_plugin import AlphaVantagePlugin
from src.plugins.yahoo_finance_plugin import YahooFinancePlugin

# === CRYPTO PLUGINS ===
from src.plugins.binance_plugin import BinancePlugin
from src.plugins.coinbase_plugin import CoinbasePlugin

# === NEWS / FILINGS PLUGINS ===
from src.plugins.news_api_plugin import NewsAPIPlugin
from src.plugins.sec_edgar_plugin import SECEdgarPlugin

# === UI ===
from src.ui.server import UIServer

logger = logging.getLogger(__name__)


class FractalConnector:
    """Main application class."""

    def __init__(self, config_dir: Path | None = None, ui_port: int = 8765):
        self.config = ConfigManager(config_dir)
        self.engine = ConnectorEngine(self.config)
        self.ui_server = UIServer(
            self.engine,
            port=ui_port,
            config_dir=self.config.config_dir,
        )

        # Register plugins
        self._register_plugins()

        # Status callback
        self.engine.on_status_change(self._on_status_change)

    def _register_plugins(self):
        """Register all available data source plugins."""
        # File-based sources
        self.engine.register_plugin(CSVPlugin)
        self.engine.register_plugin(SFTPPlugin)

        # API sources
        self.engine.register_plugin(RESTPlugin)

        # Databases
        self.engine.register_plugin(DatabasePlugin)
        self.engine.register_plugin(MSSQLPlugin)
        self.engine.register_plugin(PostgreSQLPlugin)
        self.engine.register_plugin(MongoDBPlugin)
        self.engine.register_plugin(SnowflakePlugin)
        self.engine.register_plugin(OraclePlugin)
        self.engine.register_plugin(ElasticsearchPlugin)
        self.engine.register_plugin(RedisPlugin)

        # Cloud Storage
        self.engine.register_plugin(S3Plugin)
        self.engine.register_plugin(GCSPlugin)
        self.engine.register_plugin(AzureBlobPlugin)

        # Productivity
        self.engine.register_plugin(GoogleSheetsPlugin)
        self.engine.register_plugin(SharePointPlugin)

        # Messaging/Streaming
        self.engine.register_plugin(KafkaPlugin)

        # Trading / Market Data
        self.engine.register_plugin(BloombergPlugin)
        self.engine.register_plugin(ICEConnectPlugin)
        self.engine.register_plugin(RefinitivPlugin)
        self.engine.register_plugin(InteractiveBrokersPlugin)
        self.engine.register_plugin(FactSetPlugin)
        self.engine.register_plugin(QuandlPlugin)
        self.engine.register_plugin(AlphaVantagePlugin)
        self.engine.register_plugin(YahooFinancePlugin)

        # Crypto
        self.engine.register_plugin(BinancePlugin)
        self.engine.register_plugin(CoinbasePlugin)

        # News / Filings
        self.engine.register_plugin(NewsAPIPlugin)
        self.engine.register_plugin(SECEdgarPlugin)

        logger.info(f"Registered {len(self.engine.get_registered_plugins())} plugins")

    def _on_status_change(self, status: str, details: dict):
        """Handle engine status changes."""
        logger.info(f"Status: {status}")

    async def start(self, open_browser: bool = True):
        """Start the connector."""
        logger.info("Starting Fractal Connector...")

        # Start web UI
        self.ui_server.start()
        logger.info(f"Web UI available at {self.ui_server.url}")

        # Open browser if requested
        if open_browser:
            webbrowser.open(self.ui_server.url)

        # Start engine
        await self.engine.start()

        logger.info("Fractal Connector is running")

    async def stop(self):
        """Stop the connector."""
        logger.info("Stopping Fractal Connector...")
        await self.engine.stop()
        self.ui_server.stop()
        logger.info("Fractal Connector stopped")

    async def run_forever(self):
        """Run until interrupted."""
        await self.start()

        # Wait for shutdown signal
        stop_event = asyncio.Event()

        def signal_handler():
            stop_event.set()

        # Handle shutdown signals
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, signal_handler)
            except NotImplementedError:
                # Windows doesn't support add_signal_handler
                signal.signal(sig, lambda s, f: signal_handler())

        await stop_event.wait()
        await self.stop()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Fractal Connector - Universal Data Agent'
    )
    parser.add_argument(
        '--port', '-p',
        type=int,
        default=8765,
        help='Port for web UI (default: 8765)'
    )
    parser.add_argument(
        '--config-dir', '-c',
        type=str,
        default=None,
        help='Configuration directory'
    )
    parser.add_argument(
        '--no-browser',
        action='store_true',
        help="Don't open browser on start"
    )
    parser.add_argument(
        '--service',
        action='store_true',
        help="Run as a service (no browser, no console logging)"
    )
    parser.add_argument(
        '--version', '-v',
        action='version',
        version='Fractal Connector 1.0.0'
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(console=not args.service)

    config_dir = Path(args.config_dir) if args.config_dir else None

    connector = FractalConnector(
        config_dir=config_dir,
        ui_port=args.port,
    )

    try:
        asyncio.run(connector.run_forever())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")


if __name__ == '__main__':
    main()
