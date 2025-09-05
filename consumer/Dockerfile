FROM sbtscala/scala-sbt:openjdk-17.0.2_1.8.1_2.12.17 AS builder

WORKDIR /app

COPY build.sbt ./
COPY project/ ./project/
RUN sbt update

COPY src/ ./src/

RUN sbt package

FROM bitnami/spark:3.5.6

USER root

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar $SPARK_HOME/jars 
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar  $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar $SPARK_HOME/jars

RUN chmod 644 $SPARK_HOME/jars/*.jar

COPY --from=builder /app/target/scala-2.12/consumer_2.12-1.0.jar /opt/spark/consumer.jar

ENTRYPOINT ["spark-submit", \
    "--class", "Consumer", \
    "--master", "spark://spark-master:7077", \
    "/opt/spark/consumer.jar"]
