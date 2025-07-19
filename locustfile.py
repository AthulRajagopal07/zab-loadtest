from locust import User, task, between, events
from kazoo.client import KazooClient
import time
import csv
import os

class ZookeeperUser(User):
    host = "http://dummy"  # Required for Locust UI, not used
    wait_time = between(0.1, 0.5)  # Adjust depending on how aggressive you want your test

    def on_start(self):
        self.zk = KazooClient(hosts="zookeeper-0.zookeeper-headless.zk-test.svc.cluster.local:2181")
        self.zk.start()
        self.start_time = time.time()

        # Create results directory
        if not os.path.exists("zookeeper_results"):
            os.makedirs("zookeeper_results")

        # Create unique CSV file
        self.result_file = f"zookeeper_results/locust_results_{int(self.start_time)}.csv"
        with open(self.result_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "Latency (ms)", "Outstanding Requests", "Zxid", "Mode"])

    def on_stop(self):
        self.zk.stop()
        self.zk.close()

    @task
    def get_stat(self):
        start = time.time()
        try:
            # 🛠️ FIXED: Use byte input for .command()
            stat_output_bytes = self.zk.command(b"stat")
            stat_output = stat_output_bytes.decode("utf-8")
            latency_ms = (time.time() - start) * 1000

            # Extract values from stat output
            mode = zxid = outstanding = ""
            for line in stat_output.split("\n"):
                if "Mode:" in line:
                    mode = line.split(":")[1].strip()
                elif "Zxid:" in line:
                    zxid = line.split(":")[1].strip()
                elif "Outstanding:" in line:
                    outstanding = line.split(":")[1].strip()

            # Report to Locust dashboard
            events.request.fire(
                request_type="ZK",
                name="stat",
                response_time=latency_ms,
                response_length=0,
                exception=None,
            )

            # Log to CSV
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
