@task
def write_znode(self):
    path = f"/test_{uuid.uuid4()}"
    data = b"some test data"
    start = time.time()
    try:
        self.zk.create(path, data)
        total_time = (time.time() - start) * 1000
        self.environment.events.request_success.fire(
            request_type="znode",
            name="create",
            response_time=total_time,
            response_length=len(data)
        )
    except Exception as e:
        total_time = (time.time() - start) * 1000
        self.environment.events.request_failure.fire(
            request_type="znode",
            name="create",
            response_time=total_time,
            exception=e
        )
