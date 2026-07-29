FROM apache/airflow:3.2.2-python3.10
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow
ADD requirements.txt .
RUN pip install apache-airflow==3.2.2 -r requirements.txt
