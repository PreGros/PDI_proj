# Use an official Python runtime as the base image
FROM python:3.9-slim

# Install netcat-openbsd (nc) to be used by wait-for-it.sh
RUN apt-get update && apt-get install -y netcat-openbsd

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python script and the wait-for-it.sh script into the container
COPY send_data.py .
COPY wait-for-it.sh /app/wait-for-it.sh

# Make sure wait-for-it.sh is executable
RUN chmod +x /app/wait-for-it.sh

# Set the entrypoint for the container to wait for Kafka and then run the Python script
CMD ["/app/wait-for-it.sh", "kafka:9093", "--", "python", "/app/send_data.py"]
