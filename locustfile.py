from locust import User, task, between
from kazoo.client import KazooClient
import time
import csv
import os

class ZookeeperUser(User):
    wait_time = between(1, 2.5)

    def on_start(self):
        # Hardcoded ZooKeeper host (replace with your own service address if needed)
        self.zk = KazooClient(hosts="zookeeper-0.zookeeper-headless.zk-test.svc.cluster.local:2181")
        self.zk.start()
        self.start_time = time.time()
        self.results = []

        # Prepare result file and write header if not exists
        self.result_file = f"locust_results_{int(self.start_time)}.csv"
        if not os.path.exists(self.result_file):
            with open(self.result_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Timestamp", "Latency (ms)", "Outstanding Requests", "Zxid", "Mode"])

    def on_stop(self):
        self.zk.stop()
        self.zk.close()

    def save_results(self):
        with open(self.result_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(self.results)
        self.results = []

    @task
    def get_stat(self):
        start = time.time()
        try:
            stat_output = self.zk.command("stat")
            latency_ms = (time.time() - start) * 1000

            # Parse required metrics
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

            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self.results.append([timestamp, round(latency_ms, 2), outstanding, zxid, mode])

            # Save every 10 requests
            if len(self.results) % 10 == 0:
                self.save_results()

        except Exception as e:
            print(f"Error during get_stat: {e}")
