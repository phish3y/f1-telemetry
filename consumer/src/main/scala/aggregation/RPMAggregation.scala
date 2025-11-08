package aggregation

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import packet.payload.PacketCarTelemetry

case class RPMAggregation(
    window_start: java.sql.Timestamp,
    window_end: java.sql.Timestamp,
    session_uid: Long,
    session_time: Float,
    avg_rpm: Double,
    min_rpm: Int,
    max_rpm: Int,
    sample_count: Long
) extends Aggregation

case class TracedRPMAggregation(
    aggregation: RPMAggregation,
    traceparents: Seq[String]
) extends TracedAggregation[RPMAggregation]

object RPMAggregation {
  def calculate(
      df: DataFrame
  )(implicit spark: SparkSession): Dataset[TracedRPMAggregation] = {
    import spark.implicits._

    val playerCarTelemetry = df
      .withColumn("timestamp", current_timestamp())
      .select(
        $"timestamp",
        $"m_header.m_session_uid".as("session_uid"),
        $"m_header.m_session_time".as("session_time"),
        $"m_car_telemetry_data" ($"m_header.m_player_car_index")("m_engine_rpm").as("rpm"),
        $"traceparent"
      )

    playerCarTelemetry
      .withWatermark("timestamp", "1 second")
      .groupBy(
        window($"timestamp", "1 second"),
        $"session_uid"
      )
      .agg(
        avg($"rpm").as("avg_rpm"),
        min($"rpm").as("min_rpm"),
        max($"rpm").as("max_rpm"),
        count("*").as("sample_count"),
        max($"session_time").as("session_time"),
        collect_set($"traceparent").as("traceparents")
      )
      .select(
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"session_uid",
        $"session_time",
        $"avg_rpm",
        $"min_rpm",
        $"max_rpm",
        $"sample_count",
        $"traceparents"
      )
      .as[(java.sql.Timestamp, java.sql.Timestamp, Long, Float, Double, Int, Int, Long, Seq[String])]
      .map { case (window_start, window_end, session_uid, session_time, avg_rpm, min_rpm, max_rpm, sample_count, traceparents) =>
        TracedRPMAggregation(
          aggregation = RPMAggregation(
            window_start = window_start,
            window_end = window_end,
            session_uid = session_uid,
            session_time = session_time,
            avg_rpm = avg_rpm,
            min_rpm = min_rpm,
            max_rpm = max_rpm,
            sample_count = sample_count
          ),
          traceparents = traceparents
        )
      }
  }
}
