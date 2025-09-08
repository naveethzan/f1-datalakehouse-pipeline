FROM apache/airflow:2.7.2-python3.10

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    openjdk-11-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Install additional Airflow providers
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.6.0 \
    apache-airflow-providers-amazon==8.7.1 \
    apache-airflow-providers-apache-spark==4.1.3

# Set Spark environment variables
ENV SPARK_HOME=/home/airflow/.local/lib/python3.10/site-packages/pyspark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Create necessary directories
RUN mkdir -p /opt/airflow/dags \
    /opt/airflow/jobs \
    /opt/airflow/config \
    /opt/airflow/sql \
    /opt/airflow/logs \
    /opt/airflow/utils

# Set working directory
WORKDIR /opt/airflow