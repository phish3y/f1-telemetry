package model

import packet.payload.{PacketLap, PacketLobbyInfo}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import java.time.Instant
import java.sql.Timestamp
import org.apache.spark.sql.SparkSession

case class Lap(
    timestamp: java.sql.Timestamp,
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
    grid_position: Int
)

case class TracedLap(
    packet: Lap,
    traceparent: Option[String]
) extends TracedPacket[Lap]

object Lap {
  def fromPacket(
      lapStream: Dataset[(PacketLap, String)]
  )(implicit spark: SparkSession): Dataset[TracedLap] = {
    import spark.implicits._

    lapStream
      .withColumn("timestamp", current_timestamp())
      .select(
        col("timestamp"),
        col("_1.m_header.m_session_uid").as("session_uid"),
        col("_1.m_header.m_session_time").as("session_time"),
        posexplode(col("_1.m_lap_data")).as(Seq("car_index", "lap_data")),
        col("_2").as("traceparent")
      )
      .select(
        col("timestamp"),
        col("session_uid"),
        col("session_time"),
        col("car_index"),
        col("lap_data.m_current_lap_num").as("current_lap_num"),
        col("lap_data.m_last_lap_time_in_ms").as("last_lap_time_ms"),
        col("lap_data.m_current_lap_time_in_ms").as("current_lap_time_ms"),
        (col("lap_data.m_sector1_time_minutes_part") * 60000L + col(
          "lap_data.m_sector1_time_ms_part"
        )).as("sector1_time_ms"),
        (col("lap_data.m_sector2_time_minutes_part") * 60000L + col(
          "lap_data.m_sector2_time_ms_part"
        )).as("sector2_time_ms"),
        col("lap_data.m_car_position").as("car_position"),
        col("lap_data.m_speed_trap_fastest_speed").as("speed_trap_fastest_speed"),
        col("lap_data.m_num_pit_stops").as("num_pit_stops"),
        col("lap_data.m_penalties").as("penalties"),
        col("lap_data.m_total_warnings").as("total_warnings"),
        col("lap_data.m_corner_cutting_warnings").as("corner_cutting_warnings"),
        col("lap_data.m_grid_position").as("grid_position"),
        col("traceparent")
      )
      .as[(java.sql.Timestamp, Long, Float, Int, Int, Long, Long, Long, Long, Int, Float, Int, Int, Int, Int, Int, String)]
      .map { case (timestamp, session_uid, session_time, car_index, current_lap_num, last_lap_time_ms, 
                    current_lap_time_ms, sector1_time_ms, sector2_time_ms, car_position, 
                    speed_trap_fastest_speed, num_pit_stops, penalties, total_warnings, 
                    corner_cutting_warnings, grid_position, traceparent) =>
        TracedLap(
          packet = Lap(
            timestamp = timestamp,
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
            grid_position = grid_position
          ),
          traceparent = Option(traceparent)
        )
      }
  }
}
