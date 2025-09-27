package aggregation

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

import packet.payload.PacketCarTelemetry

case class SpeedAggregation(
    window_start: java.sql.Timestamp,
    window_end: java.sql.Timestamp,
    session_uid: Long,
    avg_speed: Double,
    min_speed: Int,
    max_speed: Int,
    sample_count: Long
)

object SpeedAggregation {
  def calculate(
      ds: Dataset[PacketCarTelemetry]
  )(implicit spark: SparkSession): Dataset[SpeedAggregation] = {
    import spark.implicits._

    val playerCarTelemetry = ds
      .withColumn("timestamp", current_timestamp())
      .select(
        $"timestamp",
        $"m_header.m_session_uid".as("session_uid"),
        $"m_car_telemetry_data" ($"m_header.m_player_car_index")("m_speed").as("speed")
      )

    playerCarTelemetry
      .withWatermark("timestamp", "10 seconds")
      .groupBy(
        window($"timestamp", "3 seconds"),
        $"session_uid"
      )
      .agg(
        avg($"speed").as("avg_speed"),
        min($"speed").as("min_speed"),
        max($"speed").as("max_speed"),
        count("*").as("sample_count")
      )
      .select(
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"session_uid",
        $"avg_speed",
        $"min_speed",
        $"max_speed",
        $"sample_count"
      )
      .as[SpeedAggregation]
  }
}
