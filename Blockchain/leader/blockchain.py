import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

try:
    from event_bus import bus as event_bus
except Exception:  # pragma: no cover - optional when running standalone
    event_bus = None


def emit_blockchain_event(trigger: str, extra: Optional[Dict[str, Any]] = None) -> None:
    if event_bus is None:
        return
    payload = {"trigger": trigger}
    if extra:
        payload.update(extra)
    event_bus.emit_threadsafe("blockchain.changed", payload)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- Paths ----------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

EXCEL_PATH = os.path.join(BASE_DIR, "alerts.xlsx")
BACKUP_EXCEL = os.path.join(BASE_DIR, "alerts_backup.xlsx")
BLOCKCHAIN_FILE = os.path.join(BASE_DIR, "blockchain.json")

BASE_WORKER_DIR = os.path.join(PROJECT_ROOT, "worker")
WORKER_REGISTRY_FILE = os.path.join(BASE_DIR, "workers_registry.json")

# ---------------- Worker limits ----------------
MIN_WORKER_ID = 1
MAX_WORKER_ID = 5
MAX_WORKERS = MAX_WORKER_ID

# ---------------- Shared Genesis (MUST MATCH WORKER) ----------------
GENESIS_INDEX = 0
GENESIS_TIMESTAMP = "2025-01-01 00:00:00"
GENESIS_EXCEL_HASH = "GENESIS"
GENESIS_PREV_HASH = "0"


def worker_port(worker_id: int) -> int:
    return 5000 + worker_id


def worker_folder(worker_id: int) -> str:
    return "worker" if worker_id == 1 else f"worker{worker_id}"


def worker_name(worker_id: int) -> str:
    return f"worker {worker_id}"


def worker_url(worker_id: int) -> str:
    return f"http://127.0.0.1:{worker_port(worker_id)}"


# ---------------- Utility Functions ----------------
def compute_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def ensure_excel_exists(columns=None):
    desired_columns = list(columns) if columns is not None else []

    if not os.path.exists(EXCEL_PATH):
        if desired_columns:
            pd.DataFrame(columns=desired_columns).to_excel(EXCEL_PATH, index=False)
        return

    if desired_columns:
        try:
            existing_df = pd.read_excel(EXCEL_PATH)
        except Exception:
            existing_df = pd.DataFrame()

        if existing_df.empty and len(existing_df.columns) == 0:
            pd.DataFrame(columns=desired_columns).to_excel(EXCEL_PATH, index=False)


def normalize_excel_value(value):
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


def normalize_row_payload(row_data):
    if not isinstance(row_data, dict):
        return None

    normalized = {
        str(key): normalize_excel_value(value)
        for key, value in row_data.items()
    }

    if any(value is not None and str(value).strip() for value in normalized.values()):
        return normalized

    return None


def row_payload_to_frame(row_data):
    normalized_row = normalize_row_payload(row_data)
    if normalized_row is None:
        return None

    return pd.DataFrame([normalized_row])


def hash_excel(path: str) -> str:
    df = pd.read_excel(path)
    df = df.sort_index(axis=1)
    return compute_hash(df.to_csv(index=False))


def is_excel_tampered() -> bool:
    if not os.path.exists(EXCEL_PATH):
        return os.path.exists(BACKUP_EXCEL)

    try:
        current_hash = hash_excel(EXCEL_PATH)
    except Exception:
        return True

    if not os.path.exists(BACKUP_EXCEL):
        return False

    try:
        backup_hash = hash_excel(BACKUP_EXCEL)
    except Exception:
        return True

    return current_hash != backup_hash


def restore_excel_from_backup():
    if not os.path.exists(BACKUP_EXCEL):
        return False, "Backup file is missing"

    if os.path.exists(EXCEL_PATH):
        os.remove(EXCEL_PATH)

    shutil.copy2(BACKUP_EXCEL, EXCEL_PATH)
    return True, "Excel restored from backup"


def heal_excel_if_needed():
    if not is_excel_tampered():
        return True, "Excel is already intact"

    ok, message = restore_excel_from_backup()
    if not ok:
        return False, message

    return True, message


def backup_excel():
    if os.path.exists(EXCEL_PATH):
        df = pd.read_excel(EXCEL_PATH)
        df.to_excel(BACKUP_EXCEL, index=False)
        try:
            sys.path.append(os.path.dirname(os.path.dirname(BASE_DIR)))
            import supabase_storage
            import threading
            threading.Thread(target=supabase_storage.upload_file, args=(EXCEL_PATH, f"Blockchain/leader/alerts.xlsx")).start()
            threading.Thread(target=supabase_storage.upload_file, args=(BACKUP_EXCEL, f"Blockchain/leader/alerts_backup.xlsx")).start()
        except Exception:
            pass


def ensure_backup_exists():
    if os.path.exists(EXCEL_PATH) and not os.path.exists(BACKUP_EXCEL):
        backup_excel()


# ---------------- Block ----------------
class Block:
    def __init__(self, index, timestamp, excel_hash, prev_hash, hash_value=None):
        self.index = index
        self.timestamp = timestamp
        self.excel_hash = excel_hash
        self.prev_hash = prev_hash
        self.hash = hash_value or self.calculate_hash()

    def calculate_hash(self):
        raw = f"{self.index}{self.timestamp}{self.excel_hash}{self.prev_hash}"
        return compute_hash(raw)

    def to_dict(self):
        return {
            "index": self.index,
            "timestamp": self.timestamp,
            "excel_hash": self.excel_hash,
            "prev_hash": self.prev_hash,
            "hash": self.hash,
        }

    @staticmethod
    def from_dict(data):
        return Block(
            data["index"],
            data["timestamp"],
            data["excel_hash"],
            data["prev_hash"],
            data["hash"],
        )


# ---------------- Blockchain ----------------
class Blockchain:
    def __init__(self):
        self.chain = []
        self._baseline_signature = None
        self.load_chain()

    def _file_signature(self):
        if not os.path.exists(BLOCKCHAIN_FILE):
            return None

        with open(BLOCKCHAIN_FILE, "rb") as file_obj:
            return hashlib.sha256(file_obj.read()).hexdigest()

    def reload_from_disk(self):
        if not os.path.exists(BLOCKCHAIN_FILE):
            self.chain = []
            return False

        try:
            with open(BLOCKCHAIN_FILE, "r", encoding="utf-8") as file_obj:
                data = json.load(file_obj)
            self.chain = [Block.from_dict(b) for b in data]
            return True
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            self.chain = []
            return False

    def create_genesis(self):
        genesis = Block(
            GENESIS_INDEX,
            GENESIS_TIMESTAMP,
            GENESIS_EXCEL_HASH,
            GENESIS_PREV_HASH,
        )
        self.chain = [genesis]
        self.save_chain()

    def reset_chain(self):
        if os.path.exists(BLOCKCHAIN_FILE):
            os.remove(BLOCKCHAIN_FILE)
        self.create_genesis()

    def load_chain(self):
        if not os.path.exists(BLOCKCHAIN_FILE):
            self.create_genesis()
            return True

        loaded = self.reload_from_disk()
        if loaded:
            self._baseline_signature = self._file_signature()
        return loaded

    def save_chain(self):
        with open(BLOCKCHAIN_FILE, "w", encoding="utf-8") as file_obj:
            json.dump([b.to_dict() for b in self.chain], file_obj, indent=4)
        self._baseline_signature = self._file_signature()
        try:
            sys.path.append(os.path.dirname(os.path.dirname(BASE_DIR)))
            import supabase_storage
            import threading
            threading.Thread(target=supabase_storage.upload_file, args=(BLOCKCHAIN_FILE, f"Blockchain/leader/blockchain.json")).start()
        except Exception:
            pass

    def file_changed(self):
        return self._file_signature() != self._baseline_signature

    def add_block(self, excel_hash):
        if not self.reload_from_disk():
            raise ValueError("Blockchain file is missing or invalid")

        if self.file_changed():
            raise ValueError("Blockchain file was modified externally")

        if not self.chain:
            raise ValueError("Blockchain is unavailable")

        prev = self.chain[-1]
        block = Block(
            prev.index + 1,
            datetime.now(timezone.utc).isoformat(),
            excel_hash,
            prev.hash,
        )
        self.chain.append(block)
        self.save_chain()
        return block

    def verify_chain(self):
        if not self.reload_from_disk():
            return False

        for index in range(1, len(self.chain)):
            curr = self.chain[index]
            prev = self.chain[index - 1]

            if curr.prev_hash != prev.hash:
                return False

            if curr.hash != curr.calculate_hash():
                return False

        return True

    def verify_excel(self):
        if not self.reload_from_disk():
            return False

        if not self.chain:
            return False

        if not os.path.exists(EXCEL_PATH):
            return False

        try:
            return hash_excel(EXCEL_PATH) == self.chain[-1].excel_hash
        except Exception:
            return False

    @staticmethod
    def validate_chain_data(chain_data):
        try:
            chain = [Block.from_dict(block) for block in chain_data]
        except (KeyError, TypeError, ValueError):
            return False

        if not chain:
            return False

        for index in range(1, len(chain)):
            curr = chain[index]
            prev = chain[index - 1]

            if curr.prev_hash != prev.hash:
                return False

            if curr.hash != curr.calculate_hash():
                return False

        return True

    def replace_chain(self, chain_data):
        blocks = [Block.from_dict(block) for block in chain_data]

        if os.path.exists(BLOCKCHAIN_FILE):
            os.remove(BLOCKCHAIN_FILE)

        self.chain = blocks
        self.save_chain()


# ---------------- Worker Manager ----------------
class WorkerManager:
    def __init__(self):
        self.registry: Dict[str, Any] = {"workers": []}
        self.processes: Dict[int, subprocess.Popen] = {}
        self.load_registry()

    def _default_registry(self) -> Dict[str, Any]:
        return {
            "workers": [
                {
                    "id": 1,
                    "name": worker_name(1),
                    "port": worker_port(1),
                    "folder": worker_folder(1),
                    "removable": False,
                }
            ]
        }

    def save_registry(self):
        with open(WORKER_REGISTRY_FILE, "w", encoding="utf-8") as file_obj:
            json.dump(self.registry, file_obj, indent=4)
        try:
            sys.path.append(os.path.dirname(os.path.dirname(BASE_DIR)))
            import supabase_storage
            import threading
            threading.Thread(target=supabase_storage.upload_file, args=(WORKER_REGISTRY_FILE, f"Blockchain/leader/workers_registry.json")).start()
        except Exception:
            pass

    def load_registry(self):
        if os.path.exists(WORKER_REGISTRY_FILE):
            try:
                with open(WORKER_REGISTRY_FILE, "r", encoding="utf-8") as file_obj:
                    data = json.load(file_obj)
                workers = data.get("workers", []) if isinstance(data, dict) else []
            except (json.JSONDecodeError, OSError):
                workers = []
        else:
            workers = []

        normalized: Dict[int, Dict[str, Any]] = {}

        for raw in workers:
            if not isinstance(raw, dict):
                continue
            worker_id = raw.get("id")
            if not isinstance(worker_id, int):
                continue
            if worker_id < MIN_WORKER_ID or worker_id > MAX_WORKERS:
                continue

            normalized[worker_id] = {
                "id": worker_id,
                "name": worker_name(worker_id),
                "port": worker_port(worker_id),
                "folder": worker_folder(worker_id),
                "removable": worker_id != 1,
            }

        normalized[1] = {
            "id": 1,
            "name": worker_name(1),
            "port": worker_port(1),
            "folder": worker_folder(1),
            "removable": False,
        }

        for worker_id in range(2, MAX_WORKERS + 1):
            folder_path = os.path.join(PROJECT_ROOT, worker_folder(worker_id))
            if os.path.isdir(folder_path):
                normalized[worker_id] = {
                    "id": worker_id,
                    "name": worker_name(worker_id),
                    "port": worker_port(worker_id),
                    "folder": worker_folder(worker_id),
                    "removable": True,
                }

        self.registry = {
            "workers": [normalized[key] for key in sorted(normalized.keys())]
        }
        self.save_registry()

    def get_worker(self, worker_id: int) -> Optional[Dict[str, Any]]:
        for worker in self.registry.get("workers", []):
            if worker["id"] == worker_id:
                return worker
        return None

    def worker_urls(self) -> List[str]:
        workers = self.registry.get("workers", [])
        return [worker_url(worker["id"]) for worker in workers]

    def is_worker_running(self, worker_id: int) -> bool:
        try:
            response = requests.get(worker_url(worker_id) + "/status", timeout=1.5)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def _start_process(self, worker: Dict[str, Any]) -> int:
        folder_path = os.path.join(PROJECT_ROOT, worker["folder"])
        if not os.path.isdir(folder_path):
            raise FileNotFoundError(f"Worker folder missing: {worker['folder']}")

        env = os.environ.copy()
        env["WORKER_PORT"] = str(worker["port"])
        env["WORKER_NAME"] = worker["name"]
        env["WORKER_CHAIN_FILE"] = "worker_blockchain.json"

        process = subprocess.Popen(
            [sys.executable, "blockchain.py"],
            cwd=folder_path,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.processes[worker["id"]] = process
        return process.pid

    def _wait_until_running(self, worker_id: int, timeout_seconds: float = 8.0) -> bool:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if self.is_worker_running(worker_id):
                return True
            time.sleep(0.3)
        return False

    def ensure_started(self, worker_id: int):
        worker = self.get_worker(worker_id)
        if not worker:
            return False, "Worker does not exist"

        if self.is_worker_running(worker_id):
            return True, "already running"

        try:
            self._start_process(worker)
        except Exception as exc:
            return False, str(exc)

        if not self._wait_until_running(worker_id):
            return False, "worker process started but status endpoint is not healthy"

        return True, "started"

    def start_worker(self, worker_id: int):
        if worker_id == 1:
            return False, "Worker 1 is always running and cannot be manually started"

        return self.ensure_started(worker_id)

    def stop_worker(self, worker_id: int):
        if worker_id == 1:
            return False, "Worker 1 cannot be stopped"

        worker = self.get_worker(worker_id)
        if not worker:
            return False, "Worker does not exist"

        running_before = self.is_worker_running(worker_id)

        if running_before:
            try:
                requests.post(worker_url(worker_id) + "/admin/shutdown", timeout=2)
            except requests.RequestException:
                pass

        process = self.processes.get(worker_id)
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()

        self.processes.pop(worker_id, None)

        deadline = time.time() + 4
        while time.time() < deadline:
            if not self.is_worker_running(worker_id):
                return True, "stopped"
            time.sleep(0.2)

        if not running_before:
            return True, "already stopped"

        return False, "stop command sent but worker is still running"

    def create_worker(self):
        existing_ids = {worker["id"] for worker in self.registry.get("workers", [])}

        new_id = None
        for candidate in range(2, MAX_WORKERS + 1):
            if candidate not in existing_ids:
                new_id = candidate
                break

        if new_id is None:
            return None, "Maximum of 5 workers reached"

        source = BASE_WORKER_DIR
        target = os.path.join(PROJECT_ROOT, worker_folder(new_id))
        if not os.path.isdir(source):
            return None, "Base worker folder is missing"

        if not os.path.isdir(target):
            shutil.copytree(source, target)

        worker = {
            "id": new_id,
            "name": worker_name(new_id),
            "port": worker_port(new_id),
            "folder": worker_folder(new_id),
            "removable": True,
        }

        self.registry["workers"].append(worker)
        self.registry["workers"] = sorted(self.registry["workers"], key=lambda item: item["id"])
        self.save_registry()

        started, start_message = self.ensure_started(new_id)
        if not started:
            return worker, f"created but not running: {start_message}"

        return worker, "created and started"

    def delete_worker(self, worker_id: int):
        if worker_id == 1:
            return False, "Worker 1 cannot be removed"

        worker = self.get_worker(worker_id)
        if not worker:
            return False, "Worker does not exist"

        self.stop_worker(worker_id)

        folder_path = os.path.join(PROJECT_ROOT, worker["folder"])
        if os.path.isdir(folder_path):
            shutil.rmtree(folder_path)

        self.registry["workers"] = [item for item in self.registry["workers"] if item["id"] != worker_id]
        self.save_registry()
        self.processes.pop(worker_id, None)

        return True, "removed"

    def list_workers(self) -> List[Dict[str, Any]]:
        workers = []
        for worker in sorted(self.registry.get("workers", []), key=lambda item: item["id"]):
            worker_id = worker["id"]
            running = self.is_worker_running(worker_id)
            verify_payload: Dict[str, Any] = {}
            status_payload: Dict[str, Any] = {}

            if running:
                try:
                    verify_resp = requests.get(worker_url(worker_id) + "/verify", timeout=2)
                    if verify_resp.status_code == 200:
                        verify_payload = verify_resp.json()
                except requests.RequestException:
                    verify_payload = {}

                try:
                    status_resp = requests.get(worker_url(worker_id) + "/status", timeout=2)
                    if status_resp.status_code == 200:
                        status_payload = status_resp.json()
                except requests.RequestException:
                    status_payload = {}

            workers.append(
                {
                    **worker,
                    "url": worker_url(worker_id),
                    "running": running,
                    "verify": verify_payload,
                    "status": status_payload,
                }
            )

        return workers

    def bootstrap(self):
        self.load_registry()
        self.ensure_started(1)

        for worker in self.registry.get("workers", []):
            if worker["id"] >= 2:
                self.ensure_started(worker["id"])

    def reset_all(self):
        removed_ids = []
        for worker in sorted(self.registry.get("workers", []), key=lambda item: item["id"], reverse=True):
            worker_id = worker["id"]
            if worker_id == 1:
                continue
            ok, _ = self.delete_worker(worker_id)
            if ok:
                removed_ids.append(worker_id)

        worker_one_chain = os.path.join(PROJECT_ROOT, worker_folder(1), "worker_blockchain.json")
        if os.path.exists(worker_one_chain):
            os.remove(worker_one_chain)

        self.registry = self._default_registry()
        self.save_registry()

        started, start_message = self.ensure_started(1)
        worker_one_reset = {"started": started, "message": start_message}
        if started:
            try:
                reset_resp = requests.post(worker_url(1) + "/admin/reset", timeout=5)
                worker_one_reset["reset_status_code"] = reset_resp.status_code
            except requests.RequestException as exc:
                worker_one_reset["reset_error"] = str(exc)

        return {
            "removed_workers": sorted(removed_ids),
            "worker1": worker_one_reset,
        }


blockchain = Blockchain()
ensure_excel_exists()
ensure_backup_exists()
worker_manager = WorkerManager()


# ---------------- Broadcast ----------------
def broadcast_block(block):
    for node in worker_manager.worker_urls():
        try:
            response = requests.post(
                node + "/sync_block",
                json=block.to_dict(),
                timeout=10,
            )
            if response.status_code == 200:
                print(f"Synced with {node}")
            else:
                print(f"Rejected by {node}")
        except Exception as exc:
            print(f"Failed to reach {node}: {exc}")


@app.on_event("startup")
def on_startup():
    worker_manager.bootstrap()


# ---------------- Core API ----------------
@app.post("/add_alert")
async def add_alert(request: Request):
    data = await request.json()
    alert = data.get("alert")
    row_data = normalize_row_payload(data.get("row") or data.get("alert_data"))

    if row_data is None:
        if not alert:
            return JSONResponse(content={"error": "Invalid alert format"}, status_code=400)

        row_data = {"Alert": alert}

    if not blockchain.reload_from_disk():
        return JSONResponse(content={"error": "Blockchain file is missing or invalid"}, status_code=500)

    if blockchain.file_changed():
        return JSONResponse(content={"error": "Blockchain file was modified externally"}, status_code=409)

    ensure_excel_exists(row_data.keys())
    ensure_backup_exists()

    if is_excel_tampered():
        return JSONResponse(
            content={"error": "Excel is tampered. Call /migitate (or /mitigate) before adding alerts."},
            status_code=409,
        )

    df = pd.read_excel(EXCEL_PATH)

    row_frame = row_payload_to_frame(row_data)
    if row_frame is None:
        return JSONResponse(content={"error": "Invalid row payload"}, status_code=400)

    if not df.empty:
        comparison_columns = [column for column in row_frame.columns if column in df.columns]
        if comparison_columns:
            duplicate_mask = pd.Series(True, index=df.index)
            for column in comparison_columns:
                duplicate_mask &= df[column].astype(str) == row_frame.iloc[0][column].__str__()

            if duplicate_mask.any():
                return {"status": "duplicate", "alert": alert or row_data}

    df = pd.concat([df, row_frame], ignore_index=True)
    df.to_excel(EXCEL_PATH, index=False)

    excel_hash = hash_excel(EXCEL_PATH)
    try:
        block = blockchain.add_block(excel_hash)
    except ValueError as exc:
        return JSONResponse(content={"error": str(exc)}, status_code=500)

    backup_excel()
    broadcast_block(block)

    emit_blockchain_event("add_alert", {"block": block.to_dict()})

    return {"status": "alert added", "block": block.to_dict()}


@app.get("/verify")
def verify():
    tampered = is_excel_tampered()
    blockchain_valid = blockchain.verify_chain()
    file_changed = blockchain.file_changed()
    return {
        "blockchain_valid": blockchain_valid,
        "file_changed": file_changed,
        "integrity_ok": blockchain_valid and not file_changed and not tampered,
        "excel_intact": not tampered,
        "excel_matches_chain": blockchain.verify_excel(),
        "blocks": len(blockchain.chain),
    }


@app.post("/mitigate")
@app.post("/migitate")
@app.get("/mitigate")
@app.get("/migitate")
def mitigate():
    ensure_excel_exists()
    ensure_backup_exists()

    if not is_excel_tampered():
        return {"status": "already intact", "excel_intact": True}

    ok, heal_message = heal_excel_if_needed()
    if not ok:
        return JSONResponse(content={"error": heal_message}, status_code=500)

    if heal_message == "Excel is already intact":
        return {"status": "already intact", "excel_intact": True}

    return {
        "status": "healed",
        "excel_intact": True,
        "message": heal_message,
        "restored_from": os.path.basename(BACKUP_EXCEL),
    }


@app.post("/update")
@app.get("/update")
def update():
    local_valid = blockchain.verify_chain() and not blockchain.file_changed()
    if local_valid:
        return JSONResponse(
            content={"status": "already valid", "message": "Leader blockchain is already valid"},
            status_code=409,
        )

    last_error = "No valid worker blockchain found"

    for node in worker_manager.worker_urls():
        try:
            verify_resp = requests.get(node + "/verify", timeout=10)
            verify_resp.raise_for_status()
            verify_data = verify_resp.json()

            if not isinstance(verify_data, dict) or not verify_data.get("integrity_ok", False):
                last_error = f"{node} is not valid"
                continue

            chain_resp = requests.get(node + "/get_chain", timeout=10)
            chain_resp.raise_for_status()
            chain_payload = chain_resp.json()
            chain_data = chain_payload.get("chain") if isinstance(chain_payload, dict) else None

            if not isinstance(chain_data, list) or not Blockchain.validate_chain_data(chain_data):
                last_error = f"{node} returned an invalid chain"
                continue

            blockchain.replace_chain(chain_data)
            return {
                "status": "updated",
                "source": node,
                "blocks": len(blockchain.chain),
            }
        except Exception as exc:
            last_error = str(exc)

    return JSONResponse(content={"error": last_error}, status_code=503)


@app.get("/get_chain")
def get_chain():
    if not blockchain.reload_from_disk():
        return JSONResponse(content={"error": "Blockchain file is missing or invalid"}, status_code=500)

    return {"chain": [b.to_dict() for b in blockchain.chain]}


@app.get("/download_excel")
def download_excel():
    ensure_excel_exists()
    return FileResponse(EXCEL_PATH, filename="alerts.xlsx")


# ---------------- Dashboard API ----------------
@app.get("/api/dashboard")
def api_dashboard():
    return {
        "leader": verify(),
        "workers": worker_manager.list_workers(),
        "max_workers": MAX_WORKERS,
    }


@app.post("/api/leader/verify")
def api_leader_verify():
    result = verify()
    emit_blockchain_event("leader.verify")
    return result


@app.post("/api/leader/mitigate")
def api_leader_mitigate():
    result = mitigate()
    emit_blockchain_event("leader.mitigate")
    return result


@app.post("/api/leader/update")
def api_leader_update():
    result = update()
    if not isinstance(result, JSONResponse) or result.status_code < 400:
        emit_blockchain_event("leader.update")
    if isinstance(result, JSONResponse):
        payload = json.loads(result.body.decode("utf-8"))
        return JSONResponse(content=payload, status_code=result.status_code)
    return result


@app.post("/api/workers/add")
def api_add_worker():
    worker, message = worker_manager.create_worker()
    if worker is None:
        return JSONResponse(content={"error": message}, status_code=400)

    emit_blockchain_event("workers.add", {"worker": worker})
    return {
        "status": message,
        "worker": worker,
        "workers": worker_manager.list_workers(),
    }


@app.post("/api/workers/{worker_id}/start")
def api_start_worker(worker_id: int):
    ok, message = worker_manager.start_worker(worker_id)
    status_code = 200 if ok else 400
    if ok:
        emit_blockchain_event("workers.start", {"worker_id": worker_id})
    return JSONResponse(
        content={
            "ok": ok,
            "message": message,
            "workers": worker_manager.list_workers(),
        },
        status_code=status_code,
    )


@app.post("/api/workers/{worker_id}/stop")
def api_stop_worker(worker_id: int):
    ok, message = worker_manager.stop_worker(worker_id)
    status_code = 200 if ok else 400
    if ok:
        emit_blockchain_event("workers.stop", {"worker_id": worker_id})
    return JSONResponse(
        content={
            "ok": ok,
            "message": message,
            "workers": worker_manager.list_workers(),
        },
        status_code=status_code,
    )


@app.delete("/api/workers/{worker_id}")
def api_delete_worker(worker_id: int):
    ok, message = worker_manager.delete_worker(worker_id)
    status_code = 200 if ok else 400
    if ok:
        emit_blockchain_event("workers.delete", {"worker_id": worker_id})
    return JSONResponse(
        content={
            "ok": ok,
            "message": message,
            "workers": worker_manager.list_workers(),
        },
        status_code=status_code,
    )


@app.post("/api/workers/{worker_id}/verify")
def api_verify_worker(worker_id: int):
    worker = worker_manager.get_worker(worker_id)
    if not worker:
        return JSONResponse(content={"error": "Worker not found"}, status_code=404)

    try:
        response = requests.get(worker_url(worker_id) + "/verify", timeout=5)
        payload = response.json()
        if response.status_code < 400:
            emit_blockchain_event("workers.verify", {"worker_id": worker_id})
        return JSONResponse(
            content={"worker": worker_name(worker_id), "result": payload},
            status_code=response.status_code,
        )
    except requests.RequestException as exc:
        return JSONResponse(content={"error": str(exc)}, status_code=503)


@app.post("/api/workers/{worker_id}/update")
def api_update_worker(worker_id: int):
    worker = worker_manager.get_worker(worker_id)
    if not worker:
        return JSONResponse(content={"error": "Worker not found"}, status_code=404)

    try:
        response = requests.get(worker_url(worker_id) + "/update", timeout=8)
        payload = response.json()
        if response.status_code < 400:
            emit_blockchain_event("workers.update", {"worker_id": worker_id})
        return JSONResponse(
            content={"worker": worker_name(worker_id), "result": payload},
            status_code=response.status_code,
        )
    except requests.RequestException as exc:
        return JSONResponse(content={"error": str(exc)}, status_code=503)


@app.post("/api/reset_all")
def api_reset_all():
    manager_summary = worker_manager.reset_all()

    for path in [EXCEL_PATH, BACKUP_EXCEL, BLOCKCHAIN_FILE]:
        if os.path.exists(path):
            os.remove(path)

    ensure_excel_exists()
    backup_excel()
    blockchain.reset_chain()

    emit_blockchain_event("reset_all")
    return {
        "status": "reset complete",
        "manager": manager_summary,
        "workers": worker_manager.list_workers(),
    }


# ---------------- Root ----------------
@app.get("/")
def dashboard_home():
    return PlainTextResponse("Leader Node Running")

@app.get("/health")
def home_health():
    return PlainTextResponse("Leader Node Running")


# ---------------- Run ----------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
