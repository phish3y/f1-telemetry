FROM sbtscala/scala-sbt:openjdk-17.0.2_1.8.1_2.12.17 AS builder

WORKDIR /app

COPY build.sbt ./
COPY project/ ./project/
RUN sbt update

COPY src/ ./src/

RUN sbt package

FROM bitnamilegacy/spark:3.5.6

USER root

RUN apt-get update && apt-get install -y curl && apt-get clean && rm -rf /var/lib/apt/lists/*

# TODO this is done in two places
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar $SPARK_HOME/jars 
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar  $SPARK_HOME/jars
# Kafka
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar $SPARK_HOME/jars
# Iceberg
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0/iceberg-spark-runtime-3.5_2.12-1.10.0.jar $SPARK_HOME/jars
# OTEL - Updated to match java agent 2.20.1
ADD https://repo1.maven.org/maven2/io/opentelemetry/opentelemetry-api/1.46.0/opentelemetry-api-1.46.0.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/io/opentelemetry/opentelemetry-context/1.46.0/opentelemetry-context-1.46.0.jar $SPARK_HOME/jars

RUN mkdir -p /opt/jars
ADD https://repo1.maven.org/maven2/io/opentelemetry/javaagent/opentelemetry-javaagent/2.20.1/opentelemetry-javaagent-2.20.1.jar /opt/jars

RUN chmod 644 $SPARK_HOME/jars/*.jar && \
    chmod 644 /opt/jars/*.jar && \
    mkdir -p /opt/warehouse && chmod -R 0777 /opt/warehouse

COPY --from=builder /app/target/scala-2.12/consumer_2.12-1.0.jar /opt/spark/consumer.jar

ENTRYPOINT ["spark-submit", \
    "--class", "Consumer", \
    "--master", "spark://spark-master:7077", \
    "--conf", "spark.driver.extraJavaOptions=-javaagent:/opt/jars/opentelemetry-javaagent-2.20.1.jar", \
    "--conf", "spark.executor.extraJavaOptions=-javaagent:/opt/jars/opentelemetry-javaagent-2.20.1.jar", \
    "/opt/spark/consumer.jar"]
