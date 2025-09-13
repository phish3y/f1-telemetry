import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoders
import org.apache.log4j.Logger
import java.util.Properties
import java.time.format.DateTimeFormatter
import scala.util.Random
import java.time.Instant
import java.time.temporal.ChronoUnit
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.SerializationFeature
import java.sql.Timestamp
import org.apache.log4j.Level
import org.apache.spark.sql.streaming.Trigger

import packet.Header
import packet.payload.{Lap, PacketLap, CarTelemetry, PacketCarTelemetry}
import aggregation.SpeedAggregation
import aggregation.RPMAggregation

object Consumer {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.WARN)

    val warehousePath = sys.env.get("WAREHOUSE_PATH") match {
      case Some(path) => path
      case None => throw new IllegalArgumentException("WAREHOUSE_PATH environment variable required")
    }

    val kafkaBroker = sys.env.get("KAFKA_BROKER") match {
      case Some(broker) => broker
      case None => throw new IllegalArgumentException("KAFKA_BROKER environment variable required")
    }

    // val lapTopic = sys.env.get("LAP_TOPIC") match {
    //   case Some(broker) => broker
    //   case None => throw new IllegalArgumentException("LAP_TOPIC environment variable required")
    // }

    val carTelemetryTopic = sys.env.get("CAR_TELEMETRY_TOPIC") match {
      case Some(broker) => broker
      case None =>
        throw new IllegalArgumentException("CAR_TELEMETRY_TOPIC environment variable required")
    }

    val speedAggregationTopic = sys.env.get("SPEED_AGGREGATION_TOPIC") match {
      case Some(topic) => topic
      case None =>
        throw new IllegalArgumentException("SPEED_AGGREGATION_TOPIC environment variable required")
    }

    val rpmAggregationTopic = sys.env.get("RPM_AGGREGATION_TOPIC") match {
      case Some(topic) => topic
      case None =>
        throw new IllegalArgumentException("RPM_AGGREGATION_TOPIC environment variable required")
    }

    implicit val spark: SparkSession = SparkSession.builder
      .appName("f1-telemetry-consumer")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") 
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", warehousePath)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO
      .config("spark.sql.adaptive.enabled", "false") // TODO
      .config("fs.permissions.umask-mode", "000")
      .getOrCreate()

    import spark.implicits._

    def createIcebergTables(): Unit = {
      spark.sql("""
        CREATE TABLE IF NOT EXISTS local.speed_aggregations (
          window_start timestamp,
          window_end timestamp,
          session_uid bigint,
          avg_speed double,
          min_speed int,
          max_speed int,
          sample_count bigint,
          date string,
          hour int
        ) USING iceberg
        PARTITIONED BY (date, hour)
      """)
      
      spark.sql("""
        CREATE TABLE IF NOT EXISTS local.rpm_aggregations (
          window_start timestamp,
          window_end timestamp,
          session_uid bigint,
          avg_rpm double,
          min_rpm int,
          max_rpm int,
          sample_count bigint,
          date string,
          hour int
        ) USING iceberg
        PARTITIONED BY (date, hour)
      """)
    }

    createIcebergTables()

    val carTelemetrySchema = Encoders.product[PacketCarTelemetry].schema
    val carTelemetryStream: Dataset[PacketCarTelemetry] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", carTelemetryTopic)
      .load()
      .select(from_json($"value".cast("string"), carTelemetrySchema).as("data"))
      .select($"data.*")
      .as[PacketCarTelemetry]

    val speedAggregation = SpeedAggregation.calculate(carTelemetryStream)
    val rpmAggregation = RPMAggregation.calculate(carTelemetryStream)

    val speedAggregationWithPartitions = speedAggregation
      .withColumn("date", date_format($"window_start", "yyyyMMdd"))
      .withColumn("hour", hour($"window_start"))

    val rpmAggregationWithPartitions = rpmAggregation
      .withColumn("date", date_format($"window_start", "yyyyMMdd"))
      .withColumn("hour", hour($"window_start"))

    // Kafka
    speedAggregation
      .select(
        $"session_uid".cast("string").as("key"),
        to_json(struct($"*")).as("value")
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", speedAggregationTopic)
      .option("checkpointLocation", "/tmp/spark-checkpoints/speed-kafka")
      .outputMode("append")
      .start()

    rpmAggregation
      .select(
        $"session_uid".cast("string").as("key"),
        to_json(struct($"*")).as("value")
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", rpmAggregationTopic)
      .option("checkpointLocation", "/tmp/spark-checkpoints/rpm-kafka")
      .outputMode("append")
      .start()

    // Iceberg
    speedAggregationWithPartitions
      .writeStream
      .format("iceberg")
      .outputMode("append")
      .option("path", s"$warehousePath/speed_aggregations")
      .option("table", "local.speed_aggregations")
      .option("checkpointLocation", "/tmp/spark-checkpoints/speed-iceberg")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    rpmAggregationWithPartitions
      .writeStream
      .format("iceberg")
      .outputMode("append")
      .option("path", s"$warehousePath/rpm_aggregations")
      .option("table", "local.rpm_aggregations")
      .option("checkpointLocation", "/tmp/spark-checkpoints/rpm-iceberg")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    spark.streams.awaitAnyTermination()

    spark.stop()
  }
}
