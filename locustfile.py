from kazoo.client import KazooClient
from locust import User, task, between, events
import time, uuid

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
        start = time.time()
        try:
            self.zk.create(path, data)
            total_time = (time.time() - start) * 1000
            events.request_success.fire(
                request_type="znode",
                name="create",
                response_time=total_time,
                response_length=len(data)
            )
        except Exception as e:
            total_time = (time.time() - start) * 1000
            events.request_failure.fire(
                request_type="znode",
                name="create",
                response_time=total_time,
                exception=e
            )
