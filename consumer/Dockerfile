FROM sbtscala/scala-sbt:openjdk-17.0.2_1.8.1_2.12.17 AS builder

WORKDIR /app

COPY build.sbt ./
COPY project/ ./project/
RUN sbt update

COPY src/ ./src/

RUN sbt package

FROM bitnami/spark:3.5.6

COPY --from=builder /app/target/scala-2.12/f1-telemetry-consumer_2.12-1.0.jar /opt/spark/f1-telemetry-consumer.jar

ENV HOME=/opt/spark
ENV IVY_HOME=/opt/spark/.ivy2
ENV MAVEN_OPTS="-Duser.home=/opt/spark"

ENTRYPOINT ["spark-submit", \
    "--class", "F1TelemetrySubscriber", \
    "--master", "spark://spark-master:7077", \
    # "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6", \
    "/opt/spark/f1-telemetry-consumer.jar"]