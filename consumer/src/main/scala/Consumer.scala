import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoders
import org.apache.log4j.Logger
import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.format.DateTimeFormatter
import scala.util.Random
import java.time.Instant
import java.time.temporal.ChronoUnit
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.SerializationFeature
import java.sql.Timestamp

case class Kafka(
    key: Option[String],
    value: String,
    headers: Option[Array[String]],
    topic: Option[String],
    partition: Option[Int]
)

// case class F1TelemetryHeader(
//     m_packet_format: Int,
//     m_game_year: Int,
//     m_game_major_version: Int,
//     m_game_minor_version: Int,
//     m_packet_version: Int,
//     m_packet_id: Int,
//     m_session_uid: Long,
//     m_session_time: Float,
//     m_frame_identifier: Long,
//     m_overall_frame_identifier: Long,
//     m_player_car_index: Int,
//     m_secondary_player_car_index: Int
// )

// case class F1CarTelemetry(
//     m_header: F1TelemetryHeader,
// )

object F1TelemetrySubscriber {

  def main(args: Array[String]): Unit = {
    val kafkaBroker = sys.env.get("KAFKA_BROKER") match {
      case Some(broker) => broker
      case None => throw new IllegalArgumentException("KAFKA_BROKER environment variable required")
    }

    val lapTopic = sys.env.get("LAP_TOPIC") match {
      case Some(broker) => broker
      case None => throw new IllegalArgumentException("LAP_TOPIC environment variable required")
    }

    val spark: SparkSession = SparkSession.builder
      .appName("f1-telemetry-subscriber")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
      )
      .getOrCreate()

    import spark.implicits._

    // val schema = Encoders.product[F1CarTelemetry].schema

    // First, read the raw Kafka message with all metadata
    val kafkaStream: Dataset[Kafka] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", lapTopic)
      .load()
      .select(
        $"key".cast("string").as("key"),
        $"value".cast("string").as("value"),
        $"headers".as("headers"),
        $"topic".as("topic"),
        $"partition".as("partition")
      )
      .as[Kafka]

    // Then extract the event data from the value field
    // val eventStream: Dataset[F1CarTelemetry] = kafkaStream
    //   .select(from_json($"value", schema).as("data"))
    //   .select($"data.*")
    //   .as[F1CarTelemetry]

    // Query to show raw Kafka messages with metadata
    val kafkaQuery = kafkaStream.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .queryName("kafka-metadata")
      .start()

    // val rawQuery = eventStream.writeStream
    //   .outputMode("append")
    //   .format("console")
    //   .option("truncate", "false")
    //   .queryName("raw")
    //   .start()

    // val windowAgg = eventStream
    //   .withWatermark("timestamp", "5 seconds")
    //   .groupBy(
    //     window($"timestamp", "30 seconds"),
    //     $"vehicle"
    //   )
    //   .agg(
    //     count("*").as("count"),
    //     avg($"speed").as("avg_speed")
    //   )

    // val aggQuery = windowAgg.writeStream
    //   .outputMode("append")
    //   .format("console")
    //   .option("truncate", "false")
    //   .queryName("windowAgg")
    //   .start()

    // rawQuery.awaitTermination()
    kafkaQuery.awaitTermination()
    // aggQuery.awaitTermination()

    spark.stop()
  }
}
