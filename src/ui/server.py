"""Local web UI server for Fractal Connector configuration."""
import asyncio
import json
import logging
import secrets
import threading
from typing import Optional
from pathlib import Path

from flask import Flask, jsonify, request, render_template, send_from_directory, session, redirect, url_for

from ..core.health import health_monitor
from ..core.queue import OfflineQueue

logger = logging.getLogger(__name__)


def create_app(engine, config_dir: Path = None) -> Flask:
    """Create and configure the Flask application."""
    app = Flask(
        __name__,
        template_folder='templates',
        static_folder='static',
    )

    # Secret key for sessions
    app.secret_key = secrets.token_hex(32)

    # Initialize auth if config_dir provided
    auth = None
    if config_dir:
        from ..core.auth import LocalAuth
        auth = LocalAuth(config_dir)

    # Set engine in health monitor
    health_monitor.set_engine(engine)

    # ==================== AUTH ROUTES ====================

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        """Login page."""
        if request.method == 'POST':
            password = request.form.get('password', '')

            if auth and auth.verify(password):
                session['authenticated'] = True
                return redirect('/')
            else:
                return render_template('login.html', error='Invalid password')

        return render_template('login.html')

    @app.route('/logout')
    def logout():
        """Logout."""
        session.pop('authenticated', None)
        return redirect('/login')

    @app.route('/setup', methods=['GET', 'POST'])
    def setup():
        """Initial password setup."""
        if auth and auth.is_password_set():
            return redirect('/login')

        if request.method == 'POST':
            password = request.form.get('password', '')
            confirm = request.form.get('confirm', '')

            if password != confirm:
                return render_template('setup.html', error='Passwords do not match')

            if len(password) < 6:
                return render_template('setup.html', error='Password must be at least 6 characters')

            if auth:
                auth.set_password(password)
                session['authenticated'] = True
            return redirect('/')

        return render_template('setup.html')

    def check_auth():
        """Check if user is authenticated."""
        if not auth or not auth.is_password_set():
            return True  # No auth configured
        return session.get('authenticated', False)

    # ==================== MAIN ROUTES ====================

    @app.route('/')
    def index():
        """Serve the main UI page."""
        if auth and auth.is_password_set() and not session.get('authenticated'):
            return redirect('/login')
        if auth and not auth.is_password_set():
            return redirect('/setup')
        return render_template('index.html')

    @app.route('/static/<path:filename>')
    def serve_static(filename):
        """Serve static files."""
        return send_from_directory(app.static_folder, filename)

    # ==================== HEALTH ROUTES ====================

    @app.route('/health')
    def health():
        """Health check endpoint."""
        return jsonify(health_monitor.get_health())

    @app.route('/health/ready')
    def readiness():
        """Readiness check endpoint."""
        result = health_monitor.get_readiness()
        status_code = 200 if result['ready'] else 503
        return jsonify(result), status_code

    @app.route('/health/live')
    def liveness():
        """Liveness check endpoint."""
        return jsonify(health_monitor.get_liveness())

    # ==================== API ROUTES ====================

    @app.route('/api/status')
    def get_status():
        """Get current connector status."""
        if not check_auth():
            return jsonify({'error': 'Unauthorized'}), 401
        return jsonify(engine.get_status())

    @app.route('/api/plugins')
    def get_plugins():
        """Get list of available plugins with credential fields."""
        if not check_auth():
            return jsonify({'error': 'Unauthorized'}), 401
        return jsonify(engine.get_registered_plugins())

    @app.route('/api/sources')
    def get_sources():
        """Get configured data sources."""
        if not check_auth():
            return jsonify({'error': 'Unauthorized'}), 401

        sources = []
        for ds in engine.config.data_sources:
            sources.append({
                'id': ds.id,
                'name': ds.name,
                'plugin_type': ds.plugin_type,
                'enabled': ds.enabled,
                'sync_interval': ds.sync_interval,
            })
        return jsonify(sources)

    @app.route('/api/sources', methods=['POST'])
    def add_source():
        """Add a new data source."""
        if not check_auth():
            return jsonify({'error': 'Unauthorized'}), 401

        data = request.json
        plugin_type = data.get('plugin_type')
        name = data.get('name')
        credentials = data.get('credentials', {})

        if not plugin_type or not name:
            return jsonify({'success': False, 'error': 'Missing plugin_type or name'}), 400

        loop = asyncio.new_event_loop()
        try:
            success, result = loop.run_until_complete(
                engine.add_data_source(plugin_type, name, credentials)
            )
        finally:
            loop.close()

        if success:
            return jsonify({'success': True, 'source_id': result})
        else:
            return jsonify({'success': False, 'error': result}), 400

    @app.route('/api/sources/<source_id>', methods=['DELETE'])
    def remove_source(source_id):
        """Remove a data source."""
        if not check_auth():
            return jsonify({'error': 'Unauthorized'}), 401

        loop = asyncio.new_event_loop()
        try:
            success = loop.run_until_complete(engine.remove_data_source(source_id))
        finally:
            loop.close()

        return jsonify({'success': success})

    @app.route('/api/sources/test', methods=['POST'])
    def test_source():
        """Test a data source connection."""
        if not check_auth():
            return jsonify({'error': 'Unauthorized'}), 401

        data = request.json
        plugin_type = data.get('plugin_type')
        credentials = data.get('credentials', {})

        if not plugin_type:
            return jsonify({'success': False, 'error': 'Missing plugin_type'}), 400

        loop = asyncio.new_event_loop()
        try:
            success, message = loop.run_until_complete(
                engine.test_data_source(plugin_type, credentials)
            )
        finally:
            loop.close()

        return jsonify({'success': success, 'message': message})

    @app.route('/api/config/fractal', methods=['GET'])
    def get_fractal_config():
        """Get Fractal configuration (excluding sensitive data)."""
        if not check_auth():
            return jsonify({'error': 'Unauthorized'}), 401

        return jsonify({
            'url': engine.config.fractal.fractal_url,
            'auto_connect': engine.config.fractal.auto_connect,
            'has_api_key': bool(engine.config.fractal.api_key),
        })

    @app.route('/api/config/fractal', methods=['POST'])
    def update_fractal_config():
        """Update Fractal configuration."""
        if not check_auth():
            return jsonify({'error': 'Unauthorized'}), 401

        data = request.json

        if 'url' in data:
            engine.config.update_fractal_url(data['url'])

        if 'api_key' in data:
            engine.config.update_api_key(data['api_key'])

        return jsonify({'success': True})

    @app.route('/api/queue/stats')
    def get_queue_stats():
        """Get offline queue statistics."""
        if not check_auth():
            return jsonify({'error': 'Unauthorized'}), 401

        try:
            queue = OfflineQueue()
            return jsonify(queue.get_stats())
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    @app.route('/api/logs')
    def get_logs():
        """Get recent log entries."""
        if not check_auth():
            return jsonify({'error': 'Unauthorized'}), 401

        try:
            from ..core.logging_config import get_log_dir
            log_file = get_log_dir() / 'fractal-connector.log'

            if log_file.exists():
                lines = log_file.read_text().split('\n')[-100:]  # Last 100 lines
                return jsonify({'logs': lines})
            else:
                return jsonify({'logs': []})
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    return app


class UIServer:
    """Manager for the local web UI server."""

    def __init__(self, engine, host: str = '127.0.0.1', port: int = 8765, config_dir: Path = None):
        self.engine = engine
        self.host = host
        self.port = port
        self.config_dir = config_dir or engine.config.config_dir
        self._app: Optional[Flask] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        """Start the web UI server in a background thread."""
        self._app = create_app(self.engine, self.config_dir)

        def run_server():
            log = logging.getLogger('werkzeug')
            log.setLevel(logging.WARNING)

            self._app.run(
                host=self.host,
                port=self.port,
                debug=False,
                use_reloader=False,
            )

        self._thread = threading.Thread(target=run_server, daemon=True)
        self._thread.start()

        logger.info(f"Web UI started at http://{self.host}:{self.port}")

    def stop(self):
        """Stop the web UI server."""
        pass

    @property
    def url(self) -> str:
        """Get the URL of the web UI."""
        return f"http://{self.host}:{self.port}"
