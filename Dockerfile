FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y python3 python3-pip curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY spark_jobs/ /app/spark_jobs/

# Hadoop AWS + AWS SDK jars
RUN curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# ✅ Pre-download Delta Lake jars
RUN pip3 install delta-spark==3.1.0
RUN python3 -c "from pyspark.sql import SparkSession; from delta import configure_spark_with_delta_pip; spark = configure_spark_with_delta_pip(SparkSession.builder.appName('warmup').master('local[1]')).getOrCreate(); spark.stop(); print('Delta jars cached')"

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["tail", "-f", "/dev/null"]