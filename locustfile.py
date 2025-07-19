from locust import User, task, between, events
from kazoo.client import KazooClient
import time
import csv
import os

class ZookeeperUser(User):
    host = "http://dummy"  # Required by Locust, but not used
    wait_time = between(0.1, 0.3)  # Best for your test goals

    def on_start(self):
        self.zk = KazooClient(
            hosts="zookeeper-0.zookeeper-headless.zk-test.svc.cluster.local:2181"
        )
        self.zk.start()
        self.start_time = time.time()

        # Ensure results folder exists
        if not os.path.exists("zookeeper_results"):
            os.makedirs("zookeeper_results")

        # Create CSV file
        self.result_file = f"zookeeper_results/locust_results_{int(self.start_time)}.csv"
        if not os.path.exists(self.result_file):
            with open(self.result_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Timestamp", "Latency_ms", "Outstanding", "Zxid", "Mode"])

    def on_stop(self):
        self.zk.stop()
        self.zk.close()

    @task
    def get_stat(self):
        start = time.time()
        try:
            # Get binary response from stat command
            stat_output_bytes = self.zk.command("stat")
            stat_output = stat_output_bytes.decode("utf-8")  # ✅ FIXED

            latency_ms = (time.time() - start) * 1000

            # Parse metrics from output
            lines = stat_output.split("\n")
            mode = ""
            zxid = ""
            outstanding = ""
            for line in lines:
                if "Mode:" in line:
                    mode = line.split(":")[1].strip()
                elif "Zxid:" in line:
                    zxid = line.split(":")[1].strip()
                elif "Outstanding:" in line:
                    outstanding = line.split(":")[1].strip()

            # Fire custom event to show in Locust UI
            events.request.fire(
                request_type="ZK",
                name="stat",
                response_time=latency_ms,
                response_length=0,
                exception=None,
            )

            # Save to CSV for plotting
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            with open(self.result_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([timestamp, round(latency_ms, 2), outstanding, zxid, mode])

        except Exception as e:
            events.request.fire(
                request_type="ZK",
                name="stat",
                response_time=0,
                response_length=0,
                exception=e,
            )
