FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz | tar -xz -C /opt && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY start-master.sh /opt/spark/sbin/start-master-wrapper.sh
COPY start-worker.sh /opt/spark/sbin/start-worker-wrapper.sh
RUN chmod +x /opt/spark/sbin/start-master-wrapper.sh /opt/spark/sbin/start-worker-wrapper.sh

WORKDIR /app

CMD ["tail", "-f", "/dev/null"]
