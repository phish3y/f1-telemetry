package aggregation

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

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

case class TracedSpeedAggregation(
    aggregation: SpeedAggregation,
    traceparents: Seq[String]
)

object SpeedAggregation {
  def calculate(
      df: DataFrame
  )(implicit spark: SparkSession): Dataset[TracedSpeedAggregation] = {
    import spark.implicits._

    val playerCarTelemetry = df
      .withColumn("timestamp", current_timestamp())
      .select(
        $"timestamp",
        $"m_header.m_session_uid".as("session_uid"),
        $"m_car_telemetry_data" ($"m_header.m_player_car_index")("m_speed").as("speed"),
        $"traceparent"
      )

    playerCarTelemetry
      .withWatermark("timestamp", "5 seconds")
      .groupBy(
        window($"timestamp", "10 seconds"),
        $"session_uid"
      )
      .agg(
        avg($"speed").as("avg_speed"),
        min($"speed").as("min_speed"),
        max($"speed").as("max_speed"),
        count("*").as("sample_count"),
        collect_set($"traceparent").as("traceparents")
      )
      .select(
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"session_uid",
        $"avg_speed",
        $"min_speed",
        $"max_speed",
        $"sample_count",
        $"traceparents"
      )
      .as[(java.sql.Timestamp, java.sql.Timestamp, Long, Double, Int, Int, Long, Seq[String])]
      .map { case (window_start, window_end, session_uid, avg_speed, min_speed, max_speed, sample_count, traceparents) =>
        TracedSpeedAggregation(
          aggregation = SpeedAggregation(
            window_start = window_start,
            window_end = window_end,
            session_uid = session_uid,
            avg_speed = avg_speed,
            min_speed = min_speed,
            max_speed = max_speed,
            sample_count = sample_count
          ),
          traceparents = traceparents
        )
      }
  }
}

