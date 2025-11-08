package aggregation

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

case class LapAggregation(
    window_start: java.sql.Timestamp,
    window_end: java.sql.Timestamp,
    session_uid: Long,
    session_time: Float,
    car_index: Int,
    current_lap_num: Int,
    last_lap_time_ms: Long,
    current_lap_time_ms: Long,
    sector1_time_ms: Long,
    sector2_time_ms: Long,
    car_position: Int,
    speed_trap_fastest_speed: Float,
    num_pit_stops: Int,
    penalties: Int,
    total_warnings: Int,
    corner_cutting_warnings: Int,
    grid_position: Int,
    sample_count: Long
) extends Aggregation

case class TracedLapAggregation(
    aggregation: LapAggregation,
    traceparents: Seq[String]
) extends TracedAggregation[LapAggregation]

object LapAggregation {
  def calculate(
      df: DataFrame
  )(implicit spark: SparkSession): Dataset[TracedLapAggregation] = {
    import spark.implicits._

    val playerLapData = df
      .withColumn("timestamp", current_timestamp())
      .select(
        $"timestamp",
        $"m_header.m_session_uid".as("session_uid"),
        $"m_header.m_session_time".as("session_time"),
        $"m_header.m_player_car_index".as("car_index"),
        $"m_lap_data"($"m_header.m_player_car_index").as("lap_data"),
        $"traceparent"
      )
      .select(
        $"timestamp",
        $"session_uid",
        $"session_time",
        $"car_index",
        $"lap_data.m_current_lap_num".as("current_lap_num"),
        $"lap_data.m_last_lap_time_in_ms".as("last_lap_time_ms"),
        $"lap_data.m_current_lap_time_in_ms".as("current_lap_time_ms"),
        ($"lap_data.m_sector1_time_minutes_part" * 60000L + $"lap_data.m_sector1_time_ms_part").as("sector1_time_ms"),
        ($"lap_data.m_sector2_time_minutes_part" * 60000L + $"lap_data.m_sector2_time_ms_part").as("sector2_time_ms"),
        $"lap_data.m_car_position".as("car_position"),
        $"lap_data.m_speed_trap_fastest_speed".as("speed_trap_fastest_speed"),
        $"lap_data.m_num_pit_stops".as("num_pit_stops"),
        $"lap_data.m_penalties".as("penalties"),
        $"lap_data.m_total_warnings".as("total_warnings"),
        $"lap_data.m_corner_cutting_warnings".as("corner_cutting_warnings"),
        $"lap_data.m_grid_position".as("grid_position"),
        $"traceparent"
      )

    playerLapData
      .withWatermark("timestamp", "1 second")
      .groupBy(
        window($"timestamp", "5 seconds"),
        $"session_uid",
        $"car_index"
      )
      .agg(
        max($"session_time").as("session_time"),
        max($"current_lap_num").as("current_lap_num"),
        max($"last_lap_time_ms").as("last_lap_time_ms"),
        max($"current_lap_time_ms").as("current_lap_time_ms"),
        max($"sector1_time_ms").as("sector1_time_ms"),
        max($"sector2_time_ms").as("sector2_time_ms"),
        max($"car_position").as("car_position"),
        max($"speed_trap_fastest_speed").as("speed_trap_fastest_speed"),
        max($"num_pit_stops").as("num_pit_stops"),
        max($"penalties").as("penalties"),
        max($"total_warnings").as("total_warnings"),
        max($"corner_cutting_warnings").as("corner_cutting_warnings"),
        max($"grid_position").as("grid_position"),
        count("*").as("sample_count"),
        collect_set($"traceparent").as("traceparents")
      )
      .select(
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"session_uid",
        $"session_time",
        $"car_index",
        $"current_lap_num",
        $"last_lap_time_ms",
        $"current_lap_time_ms",
        $"sector1_time_ms",
        $"sector2_time_ms",
        $"car_position",
        $"speed_trap_fastest_speed",
        $"num_pit_stops",
        $"penalties",
        $"total_warnings",
        $"corner_cutting_warnings",
        $"grid_position",
        $"sample_count",
        $"traceparents"
      )
      .as[(java.sql.Timestamp, java.sql.Timestamp, Long, Float, Int, Int, Long, Long, Long, Long, Int, Float, Int, Int, Int, Int, Int, Long, Seq[String])]
      .map { case (window_start, window_end, session_uid, session_time, car_index, current_lap_num, 
                    last_lap_time_ms, current_lap_time_ms, sector1_time_ms, sector2_time_ms, 
                    car_position, speed_trap_fastest_speed, num_pit_stops, penalties, 
                    total_warnings, corner_cutting_warnings, grid_position, sample_count, traceparents) =>
        TracedLapAggregation(
          aggregation = LapAggregation(
            window_start = window_start,
            window_end = window_end,
            session_uid = session_uid,
            session_time = session_time,
            car_index = car_index,
            current_lap_num = current_lap_num,
            last_lap_time_ms = last_lap_time_ms,
            current_lap_time_ms = current_lap_time_ms,
            sector1_time_ms = sector1_time_ms,
            sector2_time_ms = sector2_time_ms,
            car_position = car_position,
            speed_trap_fastest_speed = speed_trap_fastest_speed,
            num_pit_stops = num_pit_stops,
            penalties = penalties,
            total_warnings = total_warnings,
            corner_cutting_warnings = corner_cutting_warnings,
            grid_position = grid_position,
            sample_count = sample_count
          ),
          traceparents = traceparents
        )
      }
  }
}
