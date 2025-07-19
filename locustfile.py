from locust import HttpUser, task, between, events, tag
from kazoo.client import KazooClient
import uuid
import time
import logging
import csv
import os

class ZookeeperUser(HttpUser):
    wait_time = between(1, 2)

    def on_start(self):
        self.zk = KazooClient(hosts="zookeeper-0.zookeeper-headless.zk-test.svc.cluster.local:2181")
        self.zk.start()
        self._init_results_csv()

    def on_stop(self):
        self.zk.stop()
        self.zk.close()

    def _init_results_csv(self):
        self.results_file = "locust_results.csv"
        if not os.path.exists(self.results_file):
            with open(self.results_file, mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["timestamp", "operation", "znode_path", "response_time", "success", "is_leader", "server_id"])

    def _record_metrics(self, operation, znode_path, start_time, success, is_leader, server_id):
        response_time = (time.time() - start_time) * 1000  # in ms
        with open(self.results_file, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([
                time.strftime("%Y-%m-%d %H:%M:%S"),
                operation,
                znode_path,
                round(response_time, 2),
                success,
                is_leader,
                server_id
            ])

    @tag('write')
    @task(weight=3)
    def write_znode(self):
        path = f"/locust_test_{uuid.uuid4()}"
        start_time = time.time()
        is_leader = None
        server_id = None
        success = False

        try:
            self.zk.create(path, b"test_data", ephemeral=True)
            success = True
        except Exception as e:
            logging.error(f"Write failed: {str(e)}")

        self._record_metrics("write", path, start_time, success, is_leader, server_id)

    @tag('read')
    @task(weight=1)
    def read_znode(self):
        children = self.zk.get_children("/")
        path = f"/{children[0]}" if children else "/"
        start_time = time.time()
        is_leader = None
        server_id = None
        success = False

        try:
            if self.zk.exists(path):
                self.zk.get(path)
                success = True
        except Exception as e:
            logging.error(f"Read failed: {str(e)}")

        self._record_metrics("read", path, start_time, success, is_leader, server_id)
