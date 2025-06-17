# Use a minimal Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install pip dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Locustfile
COPY locustfile.py .

# Set the default command to run Locust
CMD ["locust"]
