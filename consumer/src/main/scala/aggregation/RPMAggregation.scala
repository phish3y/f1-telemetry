package aggregation

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

import packet.payload.PacketCarTelemetry

case class RPMAggregation(
    window_start: java.sql.Timestamp,
    window_end: java.sql.Timestamp,
    session_uid: Long,
    avg_rpm: Double,
    min_rpm: Int,
    max_rpm: Int,
    sample_count: Long
)

object RPMAggregation {
  def calculate(
      ds: Dataset[PacketCarTelemetry]
  )(implicit spark: SparkSession): Dataset[RPMAggregation] = {
    import spark.implicits._

    val playerCarTelemetry = ds
      .withColumn("timestamp", current_timestamp())
      .select(
        $"timestamp",
        $"m_header.m_session_uid".as("session_uid"),
        $"m_car_telemetry_data" ($"m_header.m_player_car_index")("m_engine_rpm").as("rpm")
      )

    playerCarTelemetry
      .withWatermark("timestamp", "10 seconds")
      .groupBy(
        window($"timestamp", "3 seconds"),
        $"session_uid"
      )
      .agg(
        avg($"rpm").as("avg_rpm"),
        min($"rpm").as("min_rpm"),
        max($"rpm").as("max_rpm"),
        count("*").as("sample_count")
      )
      .select(
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"session_uid",
        $"avg_rpm",
        $"min_rpm",
        $"max_rpm",
        $"sample_count"
      )
      .as[RPMAggregation]
  }
}
