# Use the official Kafka Docker image as the base
FROM apache/kafka:latest

# Switch to root user to ensure permissions for apk
USER root

# Install Python and necessary dependencies using apk (for Alpine)
RUN apk update && apk add --no-cache python3 py3-pip

# Create a virtual environment for Python dependencies
RUN python3 -m venv /app/venv

# Copy the requirements.txt and install the necessary dependencies
COPY requirements.txt /app/requirements.txt
RUN . /app/venv/bin/activate && pip3 install --no-cache-dir -r /app/requirements.txt

# Copy your Python scripts to the container
COPY producer_script.py /app/producer_script.py
COPY scraper_script.py /app/scraper_script.py
COPY rest_api.py /app/rest_api.py

# Copy the KRaft mode server configuration file
COPY kraft-server.properties /opt/kafka/config/kraft-server.properties

# Set the working directory for Kafka scripts
WORKDIR /opt/kafka

# Expose ports for Kafka and REST API
EXPOSE 9092
EXPOSE 5000

# Start Kafka in KRaft mode, run the scraper in the background, and start the API
CMD ["sh", "-c", "./bin/kafka-storage.sh format --config /opt/kafka/config/kraft-server.properties --cluster-id $(./bin/kafka-storage.sh random-uuid) && ./bin/kafka-server-start.sh /opt/kafka/config/kraft-server.properties & sleep 10 && cd /app && . /app/venv/bin/activate && python3 /app/scraper_script.py & . /app/venv/bin/activate && python3 /app/rest_api.py"]
