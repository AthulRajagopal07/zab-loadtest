from locust import User, task, between
from kazoo.client import KazooClient
import time
import uuid
import csv
from datetime import datetime

class ZookeeperUser(User):
    wait_time = between(1, 2)
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.zk = None
        self.test_start = time.time()
        self.cluster_size = 3  # CHANGE THIS WHEN TESTING 5/7 NODES
        
    def on_start(self):
        self.zk = KazooClient(hosts=self.host)
        self.zk.start()
        
    def on_stop(self):
        self.zk.stop()

    @task
    def write_znode(self):
        path = f"/test_{uuid.uuid4()}"
        data = b"locust_test_data"
        start_time = time.time()
        
        try:
            # Determine if leader handled request (ZooKeeper-specific)
            stats = self.zk.server_stats()
            is_leader = stats['mode'] == 'leader'
            
            self.zk.create(path, data)
            success = True
        except Exception as e:
            is_leader = False
            success = False
            
        response_time = (time.time() - start_time) * 1000  # ms
        
        # Write per-request data
        with open(f"zk_{self.cluster_size}nodes.csv", "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now().isoformat(),  # Timestamp
                time.time() - self.test_start,  # Test elapsed time
                response_time,
                success,
                is_leader,
                self.environment.runner.user_count,  # Current clients
                self.cluster_size
            ])

# Initialize CSV (run once)
with open(f"zk_3nodes.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "timestamp", "elapsed_sec", "response_ms", 
        "success", "is_leader", "current_clients", "cluster_size"
    ])
