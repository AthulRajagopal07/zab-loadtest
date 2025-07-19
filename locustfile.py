from locust import User, task, between, tag
from kazoo.client import KazooClient
import time
import uuid
import csv
from datetime import datetime
import logging

class ZookeeperUser(User):
    """
    Locust user class for testing ZooKeeper Zab protocol performance
    """
    # Set default host (override with --host parameter)
    host = "127.0.0.1:2181"
    wait_time = between(0.1, 0.5)  # Adjust based on your test needs
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.zk = None
        self.test_start = time.time()
        self.csv_file = f"zk_results_{self.host.replace(',','_')}.csv"
        self._init_csv()
        
    def _init_csv(self):
        """Initialize CSV file with headers if it doesn't exist"""
        try:
            with open(self.csv_file, 'x', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp", "elapsed_sec", "response_ms",
                    "success", "is_leader", "server_id",
                    "current_clients", "operation", "znode_path"
                ])
        except FileExistsError:
            pass
            
    def on_start(self):
        """Connect to ZooKeeper when a simulated user starts"""
        try:
            self.zk = KazooClient(
                hosts=self.host,
                timeout=10,  # Connection timeout in seconds
                connection_retry=dict(max_delay=5, max_tries=3)
            )
            self.zk.start()
            logging.info(f"Connected to ZooKeeper at {self.host}")
        except Exception as e:
            logging.error(f"Connection failed: {str(e)}")
            raise
            
    def on_stop(self):
        """Clean up when a simulated user stops"""
        if self.zk:
            self.zk.stop()
            
    def _record_metrics(self, operation, path, start_time, success, is_leader, server_id):
        """Record metrics for each operation"""
        response_time = (time.time() - start_time) * 1000  # Convert to ms
        with open(self.csv_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now().isoformat(),
                time.time() - self.test_start,
                response_time,
                success,
                is_leader,
                server_id,
                self.environment.runner.user_count,
                operation,
                path
            ])

    @tag('write')
    @task(weight=3)  # Higher weight for more write operations
    def write_znode(self):
        """Test write performance through Zab protocol"""
        path = f"/locust_test_{uuid.uuid4()}"
        start_time = time.time()
        is_leader = False
        server_id = -1
        success = False
        
        try:
            stats = self.zk.server_stats()
            is_leader = stats['mode'] == 'leader'
            server_id = stats['server_id']
            
            # Create ephemeral node (auto-cleaned after test)
            self.zk.create(path, b"test_data", ephemeral=True)
            success = True
        except Exception as e:
            logging.error(f"Write failed: {str(e)}")
            
        self._record_metrics("write", path, start_time, success, is_leader, server_id)

    @tag('read')
    @task
    def read_znode(self):
        """Test read performance (can be served by any node)"""
        path = "/locust_test_read"
        start_time = time.time()
        is_leader = False
        server_id = -1
        success = False
        
        try:
            # Ensure test node exists
            if not self.zk.exists(path):
                self.zk.create(path, b"read_test_data")
                
            stats = self.zk.server_stats()
            is_leader = stats['mode'] == 'leader'
            server_id = stats['server_id']
            
            # Perform read
            self.zk.get(path)
            success = True
        except Exception as e:
            logging.error(f"Read failed: {str(e)}")
            
        self._record_metrics("read", path, start_time, success, is_leader, server_id)

    @tag('mixed')
    @task
    def mixed_operation(self):
        """Mixed read-write operation"""
        if int(time.time()) % 2 == 0:
            self.write_znode()
        else:
            self.read_znode()

def setup_test():
    """Initial setup when running in distributed mode"""
    logging.basicConfig(level=logging.INFO)
    logging.info("Initializing ZooKeeper performance test")

# Run setup when module loads
setup_test()
