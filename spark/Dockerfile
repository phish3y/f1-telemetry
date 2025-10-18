FROM bitnamilegacy/spark:3.5.6

USER root

RUN apt-get update && apt-get install -y curl && apt-get clean && rm -rf /var/lib/apt/lists/*

# TODO this is done in two places
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar $SPARK_HOME/jars 
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar  $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0/iceberg-spark-runtime-3.5_2.12-1.10.0.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/io/opentelemetry/opentelemetry-api/1.54.1/opentelemetry-api-1.54.1.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/io/opentelemetry/opentelemetry-context/1.54.1/opentelemetry-context-1.54.1.jar $SPARK_HOME/jars

RUN mkdir -p /opt/jars
ADD https://repo1.maven.org/maven2/io/opentelemetry/javaagent/opentelemetry-javaagent/2.20.1/opentelemetry-javaagent-2.20.1.jar /opt/jars

RUN chmod 644 $SPARK_HOME/jars/*.jar && \
    chmod 644 /opt/jars/*.jar && \
    mkdir -p /opt/warehouse && chmod -R 0777 /opt/warehouse
