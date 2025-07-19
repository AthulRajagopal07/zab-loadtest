from locust import User, task, between, events
from kazoo.client import KazooClient
import time
import csv
import os

class ZookeeperUser(User):
    host = "http://dummy"  # Required for Locust UI
    wait_time = between(0.1, 0.5)

    def on_start(self):
        # ✅ Connect only to the LEADER node (zookeeper-2)
        self.zk = KazooClient(hosts="zookeeper-2.zookeeper-headless.zk-test.svc.cluster.local:2181")
        self.zk.start()
        self.start_time = time.time()

        # Directory for results
        if not os.path.exists("zookeeper_results"):
            os.makedirs("zookeeper_results")

        # Create a new results CSV file for this run
        self.result_file = f"zookeeper_results/locust_results_{int(self.start_time)}.csv"
        with open(self.result_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "Latency (ms)"])

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

            # ✅ Report to Locust dashboard
            events.request.fire(
                request_type="ZK",
                name="create",
                response_time=latency_ms,
                response_length=0,
                exception=None,
            )

            # ✅ Log to CSV
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            with open(self.result_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([timestamp, round(latency_ms, 2)])

        except Exception as e:
            events.request.fire(
                request_type="ZK",
                name="create",
                response_time=0,
                response_length=0,
                exception=e,
            )
