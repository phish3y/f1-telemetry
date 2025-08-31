FROM bitnami/spark:3.5.6

USER root

RUN apt-get update && apt-get install -y curl && apt-get clean && rm -rf /var/lib/apt/lists/*

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar $SPARK_HOME/jars 
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar  $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar $SPARK_HOME/jars

RUN chmod 644 $SPARK_HOME/jars/hadoop-common-3.3.4.jar
RUN chmod 644 $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar
RUN chmod 644 $SPARK_HOME/jars/commons-pool2-2.12.1.jar
RUN chmod 644 $SPARK_HOME/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar
RUN chmod 644 $SPARK_HOME/jars/kafka-clients-3.5.2.jar
