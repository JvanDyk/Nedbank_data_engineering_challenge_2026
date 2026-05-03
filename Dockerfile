FROM nedbank-de-challenge/base:1.0

WORKDIR /app

# Install system utilities required by Spark and compression libraries
RUN apt-get update && apt-get install -y procps libzstd1 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Delta Lake JARs — required for Spark to load Delta extensions at runtime
COPY jars/delta-spark_2.12-3.1.0.jar /usr/local/lib/python3.11/site-packages/pyspark/jars/
COPY jars/delta-storage-3.1.0.jar /usr/local/lib/python3.11/site-packages/pyspark/jars/

COPY pipeline/ pipeline/
COPY config/ config/

ENV PYTHONPATH=/app
# Base image SPARK_HOME points to dist-packages (incorrect for pip). Override to site-packages.
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
# Scoring system runs --network=none; prevent Spark JVM from calling DNS
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_HOSTNAME=localhost

CMD ["python", "pipeline/run_all.py"]