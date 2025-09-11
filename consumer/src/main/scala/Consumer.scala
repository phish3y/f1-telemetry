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

object Consumer {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.WARN)

    val kafkaBroker = sys.env.get("KAFKA_BROKER") match {
      case Some(broker) => broker
      case None => throw new IllegalArgumentException("KAFKA_BROKER environment variable required")
    }

    val lapTopic = sys.env.get("LAP_TOPIC") match {
      case Some(broker) => broker
      case None => throw new IllegalArgumentException("LAP_TOPIC environment variable required")
    }

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

    implicit val spark: SparkSession = SparkSession.builder
      .appName("f1-telemetry-consumer")
      .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
      .getOrCreate()

    import spark.implicits._

    val carTelemetrySchema = Encoders.product[PacketCarTelemetry].schema
    val carTelemetryStream: Dataset[PacketCarTelemetry] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", carTelemetryTopic)
      .load()
      .select(from_json($"value".cast("string"), carTelemetrySchema).as("data"))
      .select($"data.*")
      .as[PacketCarTelemetry]

    SpeedAggregation.calculate(carTelemetryStream)
      .select(
        $"session_uid".cast("string").as("key"),
        to_json(struct($"*")).as("value")
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", speedAggregationTopic)
      .outputMode("append")
      .start()
      .awaitTermination()

    spark.stop()
  }
}
