FROM python:3.10-slim

WORKDIR /app

COPY locustfile.py /app/

RUN pip install locust kazoo

CMD ["locust"]
