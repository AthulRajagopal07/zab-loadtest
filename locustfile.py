from locust import User, task, between
from kazoo.client import KazooClient
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
