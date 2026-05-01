FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y python3 python3-pip curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY spark_jobs/ /app/spark_jobs/

# ✅ Download all jars directly into Spark's jars folder at build time
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -L -o /opt/spark/jars/delta-storage-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar && \
    curl -L -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["tail", "-f", "/dev/null"]