from locust import User, task, between, events
from kazoo.client import KazooClient
import time
import csv
import os

class ZookeeperUser(User):
    host = "http://dummy"  # Required by Locust
    wait_time = between(0.1, 0.5)

    def on_start(self):
        self.zk = KazooClient(hosts="zookeeper-2.zookeeper-headless.zk-test.svc.cluster.local:2181")
        self.zk.start()

        # Unique file for this test run (client group)
        self.start_time = time.time()
        self.result_file = f"zookeeper_results/raw_latencies_{int(self.start_time)}.csv"

        if not os.path.exists("zookeeper_results"):
            os.makedirs("zookeeper_results")

        # Create the CSV file with headers
        with open(self.result_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["RequestNumber", "Timestamp", "Latency (ms)"])

        # Manual request count (for marking 1 to N)
        self.request_count = 0

    def on_stop(self):
        self.zk.stop()
        self.zk.close()

    @task
    def create_ephemeral_node(self):
        start = time.time()
        try:
            node_path = f"/locust-node-{int(start * 1000)}"
            self.zk.create(node_path, b"load", ephemeral=True)
            latency_ms = (time.time() - start) * 1000
            self.request_count += 1

            # Fire Locust event for dashboard (optional)
            events.request.fire(
                request_type="ZK",
                name="create",
                response_time=latency_ms,
                response_length=0,
                exception=None,
            )

            # ✅ Manually log the raw latency and timestamp per request
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            with open(self.result_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([self.request_count, timestamp, round(latency_ms, 2)])

        except Exception as e:
            events.request.fire(
                request_type="ZK",
                name="create",
                response_time=0,
                response_length=0,
                exception=e,
            )
