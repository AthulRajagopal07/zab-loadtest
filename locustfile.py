from locust import User, task, between, events
from kazoo.client import KazooClient
import time
import uuid
import csv

class ZookeeperUser(User):
    wait_time = between(1, 2)

    def on_start(self):
        self.zk = KazooClient(hosts='zookeeper.zk-test.svc.cluster.local:2181')
        self.zk.start()

    def on_stop(self):
        self.zk.stop()

    @task
    def write_znode(self):
        path = f"/test_{uuid.uuid4()}"
        data = b"some test data"
        start_time = time.time()
        try:
            self.zk.create(path, data)
            total_time = (time.time() - start_time) * 1000
            self.environment.events.request.fire(
                request_type="znode",
                name="create",
                response_time=total_time,
                response_length=len(data),
                exception=None
            )
        except Exception as e:
            total_time = (time.time() - start_time) * 1000
            self.environment.events.request.fire(
                request_type="znode",
                name="create",
                response_time=total_time,
                response_length=0,
                exception=e
            )

# 🚀 NEW: Per-request logging for analysis

results = []

@events.request.add_listener
def log_request(request_type, name, response_time, response_length, exception, **kwargs):
    results.append({
        "timestamp": time.time(),
        "request_type": request_type,
        "name": name,
        "response_time": response_time,
        "exception": str(exception) if exception else None
    })

@events.quitting.add_listener
def save_results(environment, **kwargs):
    if not results:
        print("⚠️ No requests captured to save.")
        return
    keys = results[0].keys()
    filename = f"locust_per_request_{int(time.time())}.csv"
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(results)
    print(f"✅ Per-request results saved to {filename}")
