FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# JAR : S3A (MinIO), Kafka, Delta Lake
RUN cd /opt/spark/jars && \
    M=https://repo1.maven.org/maven2 && \
    curl -fLO $M/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fLO $M/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fLO $M/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -fLO $M/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -fLO $M/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fLO $M/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fLO $M/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -fLO $M/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

# PySpark fourni par l'image (pas de téléchargement pip)
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

ENV PIP_DEFAULT_TIMEOUT=300
ENV PIP_RETRIES=10

RUN pip3 install --no-cache-dir --no-deps delta-spark==3.2.0 && \
    pip3 install --no-cache-dir importlib-metadata minio kafka-python

RUN pip3 install --no-cache-dir pandas matplotlib networkx

RUN pip3 install --no-cache-dir jupyterlab

WORKDIR /workspace