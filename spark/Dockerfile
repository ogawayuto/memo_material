# Apache Spark 4.0.1 with Python dependencies
FROM apache/spark:4.0.1-scala2.13-java17-python3-ubuntu

# Switch to root to install packages
USER root

# Copy requirements.txt
COPY jobs/requirements.txt /tmp/requirements.txt

# Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Switch back to spark user
USER spark

# Set working directory
WORKDIR /opt/spark
