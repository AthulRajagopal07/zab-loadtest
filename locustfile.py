from kazoo.client import KazooClient
from locust import User, task, between
import time, uuid

class ZookeeperUser(User):
    wait_time = between(1, 2)

    def on_start(self):
        self.client = KazooClient(hosts='zookeeper-0.zookeeper-headless.zk-test.svc.cluster.local:2181')
        self.client.start()

    def on_stop(self):
        self.client.stop()

    @task
    def write_znode(self):
        path = f"/test_{uuid.uuid4()}"
        data = b"some test data"
        start = time.time()
        try:
            self.client.create(path, data)
            self.environment.events.request_success.fire(
                request_type="znode",
                name="create",
                response_time=(time.time() - start) * 1000,
                response_length=len(data)
            )
        except Exception as e:
            self.environment.events.request_failure.fire(
                request_type="znode",
                name="create",
                response_time=(time.time() - start) * 1000,
                exception=e
            )
