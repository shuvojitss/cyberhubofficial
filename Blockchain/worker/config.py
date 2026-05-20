NODE_NAME = "Node2"  # Change to "Node2" on Worker

# IPs or URLs of nodes
# The leader blockchain is now mounted inside the main app at /blockchain
LEADER_NODE = "http://127.0.0.1:8000/blockchain"  # Leader Node (via main app)
WORKER_NODES = [
    "http://127.0.0.1:5001",  # Worker Node
]
ALL_NODES = [LEADER_NODE] + WORKER_NODES