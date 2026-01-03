"""Offline data queue for reliability."""
import json
import sqlite3
import threading
import logging
from pathlib import Path
from datetime import datetime
from typing import Any, Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class QueuedRecord:
    """A record waiting to be sent."""
    id: int
    source_id: str
    source_type: str
    timestamp: str
    data: dict
    metadata: dict
    created_at: str
    attempts: int = 0
    last_error: Optional[str] = None


class OfflineQueue:
    """
    SQLite-backed queue for storing data when cloud is unavailable.

    Features:
    - Persists data to disk
    - Automatic retry with exponential backoff
    - Configurable max queue size
    - Thread-safe
    """

    def __init__(
        self,
        db_path: Path = None,
        max_size: int = 100000,
        max_retries: int = 10,
    ):
        if db_path is None:
            from .config import ConfigManager
            import os
            if os.name == 'nt':
                db_path = Path(os.environ.get('APPDATA', '')) / 'FractalConnector' / 'queue.db'
            else:
                db_path = Path.home() / '.fractal-connector' / 'queue.db'

        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.max_size = max_size
        self.max_retries = max_retries
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        """Initialize the SQLite database."""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    data TEXT NOT NULL,
                    metadata TEXT,
                    created_at TEXT NOT NULL,
                    attempts INTEGER DEFAULT 0,
                    last_error TEXT
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_created ON queue(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_attempts ON queue(attempts)')
            conn.commit()
            conn.close()

    def enqueue(
        self,
        source_id: str,
        source_type: str,
        timestamp: str,
        data: dict,
        metadata: dict = None,
    ) -> bool:
        """Add a record to the queue."""
        with self._lock:
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                # Check queue size
                cursor.execute('SELECT COUNT(*) FROM queue')
                count = cursor.fetchone()[0]

                if count >= self.max_size:
                    # Remove oldest records to make room
                    cursor.execute('''
                        DELETE FROM queue WHERE id IN (
                            SELECT id FROM queue ORDER BY created_at ASC LIMIT 100
                        )
                    ''')
                    logger.warning(f"Queue full, removed 100 oldest records")

                cursor.execute('''
                    INSERT INTO queue (source_id, source_type, timestamp, data, metadata, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    source_id,
                    source_type,
                    timestamp,
                    json.dumps(data),
                    json.dumps(metadata or {}),
                    datetime.utcnow().isoformat(),
                ))
                conn.commit()
                conn.close()
                return True

            except Exception as e:
                logger.error(f"Failed to enqueue record: {e}")
                return False

    def dequeue(self, batch_size: int = 100) -> list[QueuedRecord]:
        """Get records from the queue for processing."""
        with self._lock:
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id, source_id, source_type, timestamp, data, metadata, created_at, attempts, last_error
                    FROM queue
                    WHERE attempts < ?
                    ORDER BY created_at ASC
                    LIMIT ?
                ''', (self.max_retries, batch_size))

                records = []
                for row in cursor.fetchall():
                    records.append(QueuedRecord(
                        id=row[0],
                        source_id=row[1],
                        source_type=row[2],
                        timestamp=row[3],
                        data=json.loads(row[4]),
                        metadata=json.loads(row[5]) if row[5] else {},
                        created_at=row[6],
                        attempts=row[7],
                        last_error=row[8],
                    ))

                conn.close()
                return records

            except Exception as e:
                logger.error(f"Failed to dequeue records: {e}")
                return []

    def mark_success(self, record_ids: list[int]):
        """Remove successfully sent records from queue."""
        if not record_ids:
            return

        with self._lock:
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                placeholders = ','.join('?' * len(record_ids))
                cursor.execute(f'DELETE FROM queue WHERE id IN ({placeholders})', record_ids)
                conn.commit()
                conn.close()
                logger.debug(f"Removed {len(record_ids)} records from queue")
            except Exception as e:
                logger.error(f"Failed to mark records as success: {e}")

    def mark_failure(self, record_id: int, error: str):
        """Mark a record as failed and increment retry count."""
        with self._lock:
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE queue SET attempts = attempts + 1, last_error = ?
                    WHERE id = ?
                ''', (error, record_id))
                conn.commit()
                conn.close()
            except Exception as e:
                logger.error(f"Failed to mark record as failure: {e}")

    def get_stats(self) -> dict:
        """Get queue statistics."""
        with self._lock:
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                cursor.execute('SELECT COUNT(*) FROM queue')
                total = cursor.fetchone()[0]

                cursor.execute('SELECT COUNT(*) FROM queue WHERE attempts >= ?', (self.max_retries,))
                dead_letter = cursor.fetchone()[0]

                cursor.execute('SELECT MIN(created_at) FROM queue')
                oldest = cursor.fetchone()[0]

                conn.close()

                return {
                    'total': total,
                    'pending': total - dead_letter,
                    'dead_letter': dead_letter,
                    'oldest_record': oldest,
                }

            except Exception as e:
                logger.error(f"Failed to get queue stats: {e}")
                return {'total': 0, 'pending': 0, 'dead_letter': 0, 'oldest_record': None}

    def clear(self):
        """Clear all records from the queue."""
        with self._lock:
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('DELETE FROM queue')
                conn.commit()
                conn.close()
                logger.info("Queue cleared")
            except Exception as e:
                logger.error(f"Failed to clear queue: {e}")
