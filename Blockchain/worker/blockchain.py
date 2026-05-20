import hashlib
import json
import os
import threading
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
import requests
import uvicorn

from config import LEADER_NODE

app = FastAPI()

# ---------------- Configuration ----------------
WORKER_PORT = int(os.getenv("WORKER_PORT", "5001"))
WORKER_NAME = os.getenv("WORKER_NAME", f"worker-{WORKER_PORT}")
BLOCKCHAIN_FILE = os.getenv("WORKER_CHAIN_FILE", "worker_blockchain.json")

GENESIS_BLOCK = {
    "index": 0,
    "timestamp": "2025-01-01 00:00:00",
    "excel_hash": "GENESIS",
    "prev_hash": "0",
    "hash": "1301cfea5156f7ad5befde096452c7152bb9ae9813ca9a9da555e40cc355fb54",
}

# ---------------- Utility ----------------
def compute_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()

# ---------------- Block ----------------
class Block:
    def __init__(self, index, timestamp, excel_hash, prev_hash, block_hash=None):
        self.index = index
        self.timestamp = timestamp
        self.excel_hash = excel_hash
        self.prev_hash = prev_hash
        self.hash = block_hash or self.calculate_hash()

    def calculate_hash(self) -> str:
        raw = f"{self.index}{self.timestamp}{self.excel_hash}{self.prev_hash}"
        return compute_hash(raw)

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "timestamp": self.timestamp,
            "excel_hash": self.excel_hash,
            "prev_hash": self.prev_hash,
            "hash": self.hash
        }

    @staticmethod
    def from_dict(data: dict):
        return Block(
            data["index"],
            data["timestamp"],
            data["excel_hash"],
            data["prev_hash"],
            data["hash"]
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

        with open(BLOCKCHAIN_FILE, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()

    def reload_from_disk(self):
        if not os.path.exists(BLOCKCHAIN_FILE):
            self.chain = []
            return False

        try:
            with open(BLOCKCHAIN_FILE, "r") as f:
                data = json.load(f)
            self.chain = [Block.from_dict(b) for b in data]
            return True
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            self.chain = []
            return False

    def create_genesis(self):
        genesis = Block.from_dict(GENESIS_BLOCK)
        self.chain = [genesis]
        self.save_chain()

    def load_chain(self):
        if not os.path.exists(BLOCKCHAIN_FILE):
            self.create_genesis()
            return True

        loaded = self.reload_from_disk()

        # Enforce a deterministic genesis block at position 0.
        if loaded and (not self.chain or self.chain[0].to_dict() != GENESIS_BLOCK):
            self.create_genesis()
            loaded = True

        if loaded:
            self._baseline_signature = self._file_signature()

        return loaded

    def save_chain(self):
        with open(BLOCKCHAIN_FILE, "w") as f:
            json.dump([b.to_dict() for b in self.chain], f, indent=4)
        self._baseline_signature = self._file_signature()

    def file_changed(self):
        return self._file_signature() != self._baseline_signature

    def add_block_from_leader(self, data: dict):
        if not self.reload_from_disk():
            return False, "Blockchain file is missing or invalid"

        if self.file_changed():
            return False, "Blockchain file was modified externally"

        last = self.chain[-1]

        # ---- Strict validation ----
        if data["index"] != last.index + 1:
            return False, "Invalid block index"

        if data["prev_hash"] != last.hash:
            return False, "Previous hash mismatch"

        block = Block(
            data["index"],
            data["timestamp"],
            data["excel_hash"],
            data["prev_hash"],
            data["hash"]
        )

        if block.hash != block.calculate_hash():
            return False, "Invalid block hash"

        self.chain.append(block)
        self.save_chain()
        return True, block.index

    def verify_chain(self) -> bool:
        if not self.reload_from_disk():
            return False

        for i in range(1, len(self.chain)):
            curr = self.chain[i]
            prev = self.chain[i - 1]

            if curr.prev_hash != prev.hash:
                return False

            if curr.hash != curr.calculate_hash():
                return False

        return True

    @staticmethod
    def validate_chain_data(chain_data):
        try:
            chain = [Block.from_dict(block) for block in chain_data]
        except (KeyError, TypeError, ValueError):
            return False

        if not chain:
            return False

        for i in range(1, len(chain)):
            curr = chain[i]
            prev = chain[i - 1]

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

    def reset_chain(self):
        self.create_genesis()

# ---------------- Initialize ----------------
blockchain = Blockchain()

# ---------------- Worker API ----------------

@app.post("/sync_block")
async def sync_block(request: Request):
    data = await request.json()
    ok, result = blockchain.add_block_from_leader(data)

    if not ok:
        return JSONResponse(content={"status": "rejected", "reason": result}, status_code=400)

    print(f"📦 Worker accepted block #{result}")
    return {"status": "block synced", "index": result}

@app.get("/get_chain")
def get_chain():
    if not blockchain.reload_from_disk():
        return JSONResponse(content={"error": "Blockchain file is missing or invalid"}, status_code=500)

    return {"chain": [b.to_dict() for b in blockchain.chain]}

@app.post("/update")
@app.get("/update")
def update():
    local_valid = blockchain.verify_chain() and not blockchain.file_changed()
    if local_valid:
        return JSONResponse(
            content={"status": "already valid", "message": "Worker blockchain is already valid"},
            status_code=409,
        )

    try:
        verify_resp = requests.get(LEADER_NODE + "/verify", timeout=10)
        verify_resp.raise_for_status()
        verify_data = verify_resp.json()
    except Exception as exc:
        return JSONResponse(content={"error": str(exc)}, status_code=503)

    if not isinstance(verify_data, dict) or not verify_data.get("integrity_ok", False):
        return JSONResponse(
            content={"error": "Leader blockchain is not valid yet"},
            status_code=503,
        )

    try:
        chain_resp = requests.get(LEADER_NODE + "/get_chain", timeout=10)
        chain_resp.raise_for_status()
        chain_payload = chain_resp.json()
    except Exception as exc:
        return JSONResponse(content={"error": str(exc)}, status_code=503)

    chain_data = chain_payload.get("chain") if isinstance(chain_payload, dict) else None
    if not isinstance(chain_data, list) or not Blockchain.validate_chain_data(chain_data):
        return JSONResponse(content={"error": "Leader chain payload is invalid"}, status_code=503)

    blockchain.replace_chain(chain_data)
    return {
        "status": "updated",
        "source": LEADER_NODE,
        "blocks": len(blockchain.chain),
    }

@app.get("/status")
def status():
    if not blockchain.reload_from_disk() or not blockchain.chain:
        return JSONResponse(content={"error": "Blockchain file is missing or invalid"}, status_code=500)

    last = blockchain.chain[-1]
    return {
        "node": WORKER_NAME,
        "port": WORKER_PORT,
        "chain_length": len(blockchain.chain),
        "last_index": last.index,
        "last_hash": last.hash
    }

@app.post("/admin/reset")
def admin_reset():
    blockchain.reset_chain()
    return {
        "status": "reset",
        "node": WORKER_NAME,
        "blocks": len(blockchain.chain),
    }

@app.post("/admin/shutdown")
def admin_shutdown():
    def shutdown_later():
        os._exit(0)

    threading.Timer(0.2, shutdown_later).start()
    return {
        "status": "stopping",
        "node": WORKER_NAME,
    }

@app.get("/verify")
def verify():
    chain_valid = blockchain.verify_chain()
    file_changed = blockchain.file_changed()
    return {
        "chain_valid": chain_valid,
        "file_changed": file_changed,
        "integrity_ok": chain_valid and not file_changed,
    }

@app.get("/")
def home():
    return PlainTextResponse(f"{WORKER_NAME} running on port {WORKER_PORT}")

# ---------------- Run ----------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=WORKER_PORT)
