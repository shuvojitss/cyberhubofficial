import argparse
from collections import deque
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from config import LEADER_NODE, WORKER_NODES


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_EXCEL_PATH = SCRIPT_DIR / "excel2.xlsx"
POLL_INTERVAL_SECONDS = 2


def verify_node(node_url):
    verify_resp = requests.get(node_url + "/verify", timeout=10)
    verify_resp.raise_for_status()
    return verify_resp.json()


def normalize_row_value(value: Any) -> Any:
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass

    return value


def row_to_payload(row: pd.Series) -> dict[str, Any]:
    return {str(column): normalize_row_value(value) for column, value in row.items()}


def build_alert_from_row(row: pd.Series | dict[str, Any]) -> str:
    values = row.values() if isinstance(row, dict) else row.tolist()
    cleaned_values = [str(value).strip() for value in values if value is not None and str(value).strip()]

    if not cleaned_values:
        return ""

    return " | ".join(cleaned_values)


def load_alerts(excel_path: Path) -> list[dict[str, Any]]:
    df = pd.read_excel(excel_path)
    alerts: list[dict[str, Any]] = []

    for _, row in df.iterrows():
        payload = row_to_payload(row)
        if any(value is not None and str(value).strip() for value in payload.values()):
            alerts.append(payload)

    return alerts


def verify_cluster() -> None:
    try:
        verify_data = verify_node(LEADER_NODE)
    except Exception as e:
        print("Could not verify leader integrity:", e)
        raise SystemExit(1)

    if not verify_data.get("excel_intact", False):
        print("Blocked: alerts.xlsx appears tampered on leader.")
        print("Call POST /migitate (or /mitigate) first, then run broadcast.py again.")
        raise SystemExit(1)

    if not verify_data.get("integrity_ok", False):
        print("Blocked: blockchain integrity is broken on leader.")
        print("Call POST /update on the node that became invalid, then run broadcast.py again.")
        raise SystemExit(1)

    for worker_node in WORKER_NODES:
        try:
            worker_verify = verify_node(worker_node)
        except Exception as e:
            print(f"Could not verify worker integrity at {worker_node}:", e)
            raise SystemExit(1)

        if not worker_verify.get("integrity_ok", False):
            print(f"Blocked: blockchain integrity is broken on worker {worker_node}.")
            print("Call POST /update on that worker, then run broadcast.py again.")
            raise SystemExit(1)


def broadcast_alert(alert_payload: dict[str, Any], index: int) -> None:
    try:
        alert = build_alert_from_row(alert_payload)
        resp = requests.post(
            LEADER_NODE + "/add_alert",
            json={"alert": alert, "row": alert_payload},
            timeout=10,
        )
        result = resp.json()
        print(f"{index}: {result}")
        return True
    except Exception as e:
        print(f"{index}. Error:", e)
        return False


def integrity_is_ok() -> bool:
    try:
        verify_data = verify_node(LEADER_NODE)
    except Exception as exc:
        print("Could not verify leader integrity:", exc)
        return False

    if not verify_data.get("excel_intact", False):
        return False

    if not verify_data.get("integrity_ok", False):
        return False

    for worker_node in WORKER_NODES:
        try:
            worker_verify = verify_node(worker_node)
        except Exception as exc:
            print(f"Could not verify worker integrity at {worker_node}:", exc)
            return False

        if not worker_verify.get("integrity_ok", False):
            return False

    return True


def watch_and_broadcast(excel_path: Path, poll_interval: int) -> None:
    last_seen_row_count = 0
    pending_alerts: deque[dict[str, Any]] = deque()
    was_tampered = False

    print(f"Watching {excel_path} for new rows. Press Ctrl+C to stop.")

    while True:
        try:
            current_alerts = load_alerts(excel_path)
        except FileNotFoundError:
            print(f"Waiting for {excel_path.name} to appear...")
            time.sleep(poll_interval)
            continue
        except Exception as exc:
            print(f"Error reading {excel_path.name}:", exc)
            time.sleep(poll_interval)
            continue

        if len(current_alerts) < last_seen_row_count:
            last_seen_row_count = len(current_alerts)

        new_alerts = current_alerts[last_seen_row_count:]
        if new_alerts:
            pending_alerts.extend(new_alerts)
            last_seen_row_count = len(current_alerts)

        integrity_ok = integrity_is_ok()

        if not integrity_ok:
            was_tampered = True

        if integrity_ok and pending_alerts:
            while pending_alerts:
                alert = pending_alerts.popleft()
                next_index = len(current_alerts) - len(pending_alerts)
                if not broadcast_alert(alert, next_index):
                    pending_alerts.appendleft(alert)
                    break

            if was_tampered and not pending_alerts:
                print("Integrity restored. Pending alerts have been broadcast.")
                was_tampered = False

        time.sleep(poll_interval)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuously broadcast new alerts from Excel.")
    parser.add_argument(
        "--excel",
        type=Path,
        default=DEFAULT_EXCEL_PATH,
        help="Path to the Excel file to watch.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=POLL_INTERVAL_SECONDS,
        help="Polling interval in seconds.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    verify_cluster()
    watch_and_broadcast(args.excel, max(1, args.interval))


if __name__ == "__main__":
    main()

