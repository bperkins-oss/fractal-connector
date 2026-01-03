"""Health check and monitoring."""
import time
import psutil
import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class HealthMonitor:
    """Monitor system and application health."""

    def __init__(self, engine=None):
        self.engine = engine
        self.start_time = datetime.utcnow()
        self._last_check = None
        self._status = "starting"

    def set_engine(self, engine):
        """Set the connector engine reference."""
        self.engine = engine

    def get_health(self) -> dict[str, Any]:
        """Get comprehensive health status."""
        now = datetime.utcnow()
        uptime = (now - self.start_time).total_seconds()

        health = {
            "status": self._determine_status(),
            "timestamp": now.isoformat(),
            "uptime_seconds": int(uptime),
            "version": "1.0.0",
        }

        # System metrics
        try:
            health["system"] = {
                "cpu_percent": psutil.cpu_percent(interval=0.1),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_percent": psutil.disk_usage('/').percent if hasattr(psutil.disk_usage('/'), 'percent') else None,
            }
        except Exception as e:
            health["system"] = {"error": str(e)}

        # Engine status
        if self.engine:
            try:
                engine_status = self.engine.get_status()
                health["engine"] = {
                    "running": engine_status.get("running", False),
                    "fractal_connected": engine_status.get("fractal_connected", False),
                    "active_sources": len(engine_status.get("active_sources", [])),
                    "registered_plugins": len(engine_status.get("registered_plugins", [])),
                }
            except Exception as e:
                health["engine"] = {"error": str(e)}

        # Queue status
        try:
            from .queue import OfflineQueue
            queue = OfflineQueue()
            health["queue"] = queue.get_stats()
        except Exception as e:
            health["queue"] = {"error": str(e)}

        self._last_check = now
        return health

    def _determine_status(self) -> str:
        """Determine overall health status."""
        if not self.engine:
            return "starting"

        try:
            status = self.engine.get_status()

            if not status.get("running"):
                return "stopped"

            if not status.get("fractal_connected"):
                return "degraded"  # Running but not connected to cloud

            return "healthy"

        except Exception:
            return "unhealthy"

    def get_readiness(self) -> dict[str, Any]:
        """Check if the application is ready to accept traffic."""
        ready = False
        reason = "unknown"

        if not self.engine:
            reason = "engine not initialized"
        elif not self.engine._running:
            reason = "engine not running"
        else:
            ready = True
            reason = "ready"

        return {
            "ready": ready,
            "reason": reason,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def get_liveness(self) -> dict[str, Any]:
        """Check if the application is alive (basic ping)."""
        return {
            "alive": True,
            "timestamp": datetime.utcnow().isoformat(),
        }


# Global health monitor instance
health_monitor = HealthMonitor()
