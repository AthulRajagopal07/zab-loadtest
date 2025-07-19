from locust import User, task, between, events
from kazoo.client import KazooClient
import time
import csv
import os

class ZookeeperUser(User):
    host = "http://dummy"  # Required by Locust
    wait_time = between(1, 2)  # Adjust as needed

    def on_start(self):
        self.zk = KazooClient(
            hosts="zookeeper-0.zookeeper-headless.zk-test.svc.cluster.local:2181"
        )
        self.zk.start()
        self.start_time = time.time()

        # Create folder + CSV filename per test run
        os.makedirs("zookeeper_results", exist_ok=True)
        self.result_file = f"zookeeper_results/locust_metrics_{int(self.start_time)}.csv"

        # Write headers
        with open(self.result_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp", "elapsed_sec", "latency_ms",
                "outstanding", "zxid", "mode", "user_count"
            ])

    def on_stop(self):
        self.zk.stop()
        self.zk.close()

    @task
    def gather_metrics(self):
        start = time.time()
        try:
            # Get raw stats from ZooKeeper
            stat_output = self.zk.command("stat")
            stat_output = stat_output.decode()  # FIX: decode bytes to string

            # Parse needed metrics
            mode, zxid, outstanding = None, None, None
            for line in stat_output.splitlines():
                if "Mode:" in line:
                    mode = line.split(":")[1].strip()
                elif "Zxid:" in line:
                    zxid = line.split(":")[1].strip()
                elif "Outstanding:" in line:
                    outstanding = line.split(":")[1].strip()

            latency_ms = (time.time() - start) * 1000
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

            # Log custom metric to Locust UI
            events.request.fire(
                request_type="ZK",
                name="stat",
                response_time=latency_ms,
                response_length=0,
                exception=None,
            )

            # Save to CSV
            with open(self.result_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp,
                    round(time.time() - self.start_time, 2),
                    round(latency_ms, 2),
                    outstanding,
                    zxid,
                    mode,
                    self.environment.runner.user_count if self.environment.runner else 0
                ])

        except Exception as e:
            events.request.fire(
                request_type="ZK",
                name="stat",
                response_time=0,
                response_length=0,
                exception=e,
            )
