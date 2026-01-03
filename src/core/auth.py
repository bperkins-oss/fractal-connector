"""Authentication for local web UI."""
import hashlib
import secrets
import os
from pathlib import Path
from functools import wraps
from flask import request, Response, session


class LocalAuth:
    """Simple authentication for the local web UI."""

    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
        self.auth_file = config_dir / '.auth'
        self._password_hash = None
        self._load_or_create_password()

    def _load_or_create_password(self):
        """Load existing password hash or create default."""
        if self.auth_file.exists():
            self._password_hash = self.auth_file.read_text().strip()
        else:
            # First run - no password set yet
            self._password_hash = None

    def _hash_password(self, password: str) -> str:
        """Hash a password with salt."""
        salt = secrets.token_hex(16)
        hashed = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
        return f"{salt}:{hashed.hex()}"

    def _verify_password(self, password: str, stored_hash: str) -> bool:
        """Verify a password against stored hash."""
        try:
            salt, hash_hex = stored_hash.split(':')
            new_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
            return new_hash.hex() == hash_hex
        except:
            return False

    def is_password_set(self) -> bool:
        """Check if a password has been set."""
        return self._password_hash is not None

    def set_password(self, password: str) -> bool:
        """Set or update the password."""
        if len(password) < 6:
            return False
        self._password_hash = self._hash_password(password)
        self.auth_file.write_text(self._password_hash)
        # Restrict file permissions on non-Windows
        if os.name != 'nt':
            os.chmod(self.auth_file, 0o600)
        return True

    def verify(self, password: str) -> bool:
        """Verify a password."""
        if not self._password_hash:
            return False
        return self._verify_password(password, self._password_hash)

    def generate_session_token(self) -> str:
        """Generate a secure session token."""
        return secrets.token_urlsafe(32)


def require_auth(auth: LocalAuth):
    """Decorator to require authentication on routes."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Check if password is set
            if not auth.is_password_set():
                # Allow access during initial setup
                return f(*args, **kwargs)

            # Check session
            if session.get('authenticated'):
                return f(*args, **kwargs)

            # Check for API key in header (for programmatic access)
            api_key = request.headers.get('X-API-Key')
            if api_key and auth.verify(api_key):
                return f(*args, **kwargs)

            # Return 401 for API requests
            if request.is_json or request.path.startswith('/api/'):
                return Response(
                    '{"error": "Authentication required"}',
                    status=401,
                    mimetype='application/json'
                )

            # Redirect to login for browser requests
            return Response(
                'Authentication required',
                status=401,
                headers={'WWW-Authenticate': 'Basic realm="Fractal Connector"'}
            )

        return decorated_function
    return decorator
