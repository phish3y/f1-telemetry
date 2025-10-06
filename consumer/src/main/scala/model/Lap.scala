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
    date: Int,
    hour: Int
)

case class TracedLap(
    packet: Lap,
    traceparent: String
) extends TracedPacket[Lap]

object Lap {
  def fromPacket(
      lapStreamRaw: org.apache.spark.sql.DataFrame
  )(implicit spark: SparkSession): Dataset[TracedLap] = {
    import spark.implicits._

    val lapStream = lapStreamRaw
      .select(
        col("data.*"),
        col("traceparent")
      )
      .as[(PacketLap, String)]

    lapStream
      .withColumn("timestamp", current_timestamp())
      .select(
        col("timestamp"),
        col("_1.m_header.m_session_uid").as("session_uid"),
        posexplode(col("_1.m_lap_data")).as(Seq("car_index", "lap_data")),
        col("_2").as("traceparent")
      )
      .select(
        col("timestamp"),
        col("session_uid"),
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
        date_format(col("timestamp"), "yyyyMMdd").cast("int").as("date"),
        hour(col("timestamp")).as("hour"),
        col("traceparent")
      )
      .as[(java.sql.Timestamp, Long, Int, Int, Long, Long, Long, Long, Int, Float, Int, Int, Int, Int, Int, Int, Int, String)]
      .map { case (timestamp, session_uid, car_index, current_lap_num, last_lap_time_ms, 
                    current_lap_time_ms, sector1_time_ms, sector2_time_ms, car_position, 
                    speed_trap_fastest_speed, num_pit_stops, penalties, total_warnings, 
                    corner_cutting_warnings, grid_position, date, hour, traceparent) =>
        TracedLap(
          packet = Lap(
            timestamp = timestamp,
            session_uid = session_uid,
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
            date = date,
            hour = hour
          ),
          traceparent = traceparent
        )
      }
  }
}
