from locust import User, task, between, events
from kazoo.client import KazooClient
import time
import csv
import os
import uuid  # For unique node names

class ZookeeperUser(User):
    host = "http://dummy"  # Required by Locust UI
    wait_time = between(0.1, 0.5)

    def on_start(self):
        self.zk = KazooClient(hosts="zookeeper-2.zookeeper-headless.zk-test.svc.cluster.local:2181")
        self.zk.start()

        # Request counter for unique node paths & logging
        self.request_count = 0

        # Ensure results directory exists
        if not os.path.exists("zookeeper_results"):
            os.makedirs("zookeeper_results")

        # Create a unique filename based on start time (epoch)
        self.start_time = time.time()
        self.result_file = f"zookeeper_results/locust_results_{int(self.start_time)}.csv"

        # Print full path so you know where CSV will be saved
        print(f"[INFO] Saving results to: {os.path.abspath(self.result_file)}")

        # Create CSV file with headers
        with open(self.result_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["RequestNumber", "Timestamp", "Latency (ms)"])

    def on_stop(self):
        self.zk.stop()
        self.zk.close()

    @task
    def create_ephemeral_node(self):
        start = time.time()
        try:
            # Use UUID for unique ephemeral node path, avoids NodeExistsError
            node_path = f"/locust-node-{uuid.uuid4()}"

            self.zk.create(node_path, b"load", ephemeral=True)
            latency_ms = (time.time() - start) * 1000

            self.request_count += 1

            # Fire event to Locust dashboard
            events.request.fire(
                request_type="ZK",
                name="create",
                response_time=latency_ms,
                response_length=0,
                exception=None,
            )

            # Log each request latency with request number and timestamp
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            with open(self.result_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([self.request_count, timestamp, round(latency_ms, 2)])

        except Exception as e:
            # Fire failure event for Locust dashboard
            events.request.fire(
                request_type="ZK",
                name="create",
                response_time=0,
                response_length=0,
                exception=e,
            )
