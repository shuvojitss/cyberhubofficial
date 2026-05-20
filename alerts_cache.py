"""
alerts_cache.py
===============
Materialised in-memory cache for alerts.

Instead of re-parsing the Excel file on every HTTP request, the cache
holds the latest alert list and refreshes only when:
  - the event bus fires an ``alerts.changed`` event, OR
  - the xlsx file's modification time has changed (catches manual edits), OR
  - ``invalidate()`` is called explicitly.
"""

import logging
import os
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("alerts_cache")


class AlertsCache:
    """Thread-safe, lazily-populated cache for the alerts data pipeline."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._alerts: List[Dict[str, str]] = []
        self._overview: Optional[Dict[str, Any]] = None
        self._last_refresh: Optional[datetime] = None
        self._dirty = True
        self._xlsx_path: Optional[str] = None
        self._last_mtime: float = 0.0

        # These are injected by app.py at startup to avoid circular imports
        self._sync_fn = None          # sync_alerts_db_from_excel
        self._load_fn = None          # load_alerts_from_db
        self._status_fns = None       # (fetch_statuses, insert_missing_statuses, get_app_data_db_connection, DEFAULT_ALERT_STATUS)
        self._overview_builder = None # build_overview_payload

    def configure(
        self,
        sync_fn,
        load_fn,
        fetch_statuses_fn,
        insert_missing_fn,
        get_conn_fn,
        default_status: str,
        overview_builder,
        xlsx_path: str = "",
    ) -> None:
        """Wire up the functions from app.py without creating circular imports."""
        self._sync_fn = sync_fn
        self._load_fn = load_fn
        self._status_fns = (fetch_statuses_fn, insert_missing_fn, get_conn_fn, default_status)
        self._overview_builder = overview_builder
        self._xlsx_path = xlsx_path

    def invalidate(self) -> None:
        """Mark the cache as dirty so the next read triggers a refresh."""
        with self._lock:
            self._dirty = True
            self._overview = None

    def _file_changed(self) -> bool:
        """Check if the xlsx file's mtime has changed since last refresh."""
        if not self._xlsx_path:
            return False
        try:
            current_mtime = os.path.getmtime(self._xlsx_path)
            return current_mtime != self._last_mtime
        except OSError:
            return False

    def _refresh_if_needed(self) -> None:
        """Refresh from Excel → DB → memory if dirty or file changed."""
        if not self._dirty and not self._file_changed():
            return

        if self._sync_fn is None:
            logger.warning("AlertsCache not configured yet — skipping refresh")
            return

        try:
            self._sync_fn()
            alerts = self._load_fn()

            # Attach statuses
            fetch_statuses, insert_missing, get_conn, default_status = self._status_fns
            conn = get_conn()
            try:
                threat_ids = [a["id"] for a in alerts]
                status_map = fetch_statuses(conn, threat_ids)
                insert_missing(conn, threat_ids, status_map)
                conn.commit()
            finally:
                conn.close()

            for alert in alerts:
                alert["status"] = status_map.get(alert["id"], default_status)

            self._alerts = alerts
            self._overview = None  # invalidate derived data
            self._last_refresh = datetime.utcnow()
            self._dirty = False

            # Track file mtime
            if self._xlsx_path:
                try:
                    self._last_mtime = os.path.getmtime(self._xlsx_path)
                except OSError:
                    pass

            logger.info("AlertsCache refreshed — %d alerts loaded", len(alerts))
        except FileNotFoundError:
            logger.warning("Alerts xlsx not found — cache empty")
            self._alerts = []
            self._overview = None
            self._dirty = False
            raise
        except Exception:
            logger.exception("AlertsCache refresh failed")
            raise

    def get_alerts(self) -> List[Dict[str, str]]:
        """Return the cached alerts list (refreshing if dirty or file changed)."""
        with self._lock:
            self._refresh_if_needed()
            return list(self._alerts)  # shallow copy

    def get_overview(self) -> Dict[str, Any]:
        """Return the cached overview payload (refreshing if dirty or file changed)."""
        with self._lock:
            self._refresh_if_needed()
            if self._overview is None and self._overview_builder is not None:
                self._overview = self._overview_builder(self._alerts)
            return dict(self._overview) if self._overview else {}


# Module-level singleton
cache = AlertsCache()

