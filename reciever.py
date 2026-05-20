"""
reciever.py
===========
Log-receiver module.  Provides:
    - ``update_sender(ip, log_count)`` – record a sender in app_data.db
    - ``receive_logs_handler(request)`` – async FastAPI handler mounted by app.py

When executed directly it still starts a standalone Flask server for
backward-compatibility during local development, but in production
the handler is imported and mounted inside the main FastAPI app so
that **no extra port** is needed.
"""

import os
import sqlite3
import db_wrapper
from datetime import datetime

from fastapi import Request
from fastapi.responses import JSONResponse
from logger import get_logger
from event_bus import bus as event_bus

logger = get_logger("receiver")

APP_DATA_DB_PATH = os.path.join(os.path.dirname(__file__), "app_data.db")


def init_senders_table():
    """Ensure senders table exists inside the unified app_data.db."""
    conn = db_wrapper.get_app_data_db_connection()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS senders (
            ip TEXT PRIMARY KEY,
            hostname TEXT,
            total_logs INTEGER NOT NULL DEFAULT 0,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


init_senders_table()


def update_sender(ip, log_count):
    now = datetime.utcnow().isoformat() + "Z"
    hostname = ip  # default to IP; could resolve later
    conn = db_wrapper.get_app_data_db_connection()
    try:
        conn.execute(
            """
            INSERT INTO senders (ip, hostname, total_logs, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(ip) DO UPDATE SET
                total_logs = total_logs + excluded.total_logs,
                last_seen  = excluded.last_seen
            """,
            (ip, hostname, log_count, now, now),
        )
        conn.commit()
    finally:
        conn.close()


async def receive_logs_handler(request: Request):
    """FastAPI-compatible handler for POST /receive-logs."""
    body = await request.body()
    logs = body.decode("utf-8", errors="replace")
    source_ip = (
        request.headers.get("X-Source-IP")
        or request.headers.get("X-Source-Machine")
        or request.client.host
        or "unknown"
    )
    lines = [line for line in logs.splitlines() if line.strip()]

    log_path = os.path.join(os.path.dirname(__file__), "input.log")
    with open(log_path, "a", encoding="utf-8") as f:
        for line in lines:
            f.write(f"{line} ||SRC_IP={source_ip}\n")

    # Track the sender in SQLite
    update_sender(source_ip, len(lines))

    logger.info("Received %d new log lines from %s", len(lines), source_ip)
    event_bus.emit_threadsafe("logs.received", {"source": source_ip, "count": len(lines)})
    
    try:
        import supabase_storage
        import asyncio
        asyncio.create_task(asyncio.to_thread(supabase_storage.upload_file, log_path, "input.log"))
    except Exception as e:
        logger.error(f"Failed to sync input.log: {e}")

    return JSONResponse({"status": "ok"}, status_code=200)


# ---------------------------------------------------------------------------
# Standalone mode (backward-compat for local dev / testing)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    from flask import Flask, request as flask_request

    flask_app = Flask(__name__)

    @flask_app.route('/receive-logs', methods=['POST'])
    def _flask_receive_logs():
        logs = flask_request.data.decode("utf-8", errors="replace")
        source_ip = (
            flask_request.headers.get("X-Source-IP")
            or flask_request.headers.get("X-Source-Machine")
            or flask_request.remote_addr
            or "unknown"
        )
        lines = [line for line in logs.splitlines() if line.strip()]

        log_path = os.path.join(os.path.dirname(__file__), "input.log")
        with open(log_path, "a", encoding="utf-8") as f:
            for line in lines:
                f.write(f"{line} ||SRC_IP={source_ip}\n")

        update_sender(source_ip, len(lines))

        print(f"\u2705 Received {len(lines)} new log lines from {source_ip}.")
        return {"status": "ok"}, 200

    flask_app.run(host="127.0.0.1", port=6000)
