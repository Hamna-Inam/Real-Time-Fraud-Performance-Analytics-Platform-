FROM apache/airflow:latest

USER root

# Install Java
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add airflow user to docker group for socket access
RUN groupadd -f docker && usermod -aG docker airflow

# Switch to airflow user and install packages
USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.3.0 \
    pymongo \
    pandas \
    apache-airflow-providers-apache-spark

WORKDIR /opt/airflow
