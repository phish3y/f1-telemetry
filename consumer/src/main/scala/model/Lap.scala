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
    player_name: String,
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

object Lap {
  def fromPacket(lapStream: Dataset[PacketLap], lobbyStream: Dataset[PacketLobbyInfo])(implicit
      spark: SparkSession
  ): Dataset[Lap] = {
    import spark.implicits._

    val humanPlayersDF = lobbyStream
      .withColumn("timestamp", current_timestamp())
      .withWatermark("timestamp", "30 seconds")
      .select(
        col("timestamp"),
        col("m_header.m_session_uid").as("session_uid"),
        posexplode(col("m_lobby_players")).as(Seq("car_index", "lobby_data"))
      )
      .filter(col("lobby_data.m_ai_controlled") === 0)
      .select(
        col("timestamp"),
        col("session_uid"),
        col("car_index"),
        regexp_replace(
          expr("cast(lobby_data.m_name as string)"),
          "\\x00.*",
          ""
        ).as("player_name")
      )

    val allLapData = lapStream
      .withColumn("timestamp", current_timestamp())
      .withWatermark("timestamp", "30 seconds")
      .select(
        col("timestamp"),
        col("m_header.m_session_uid").as("session_uid"),
        posexplode(col("m_lap_data")).as(Seq("car_index", "lap_data"))
      )

    allLapData
      .alias("lap")
      .join(
        humanPlayersDF.alias("lobby"),
        expr("""
                    lap.session_uid = lobby.session_uid AND 
                    lap.car_index = lobby.car_index AND 
                    lap.timestamp >= lobby.timestamp - interval 1 minutes AND 
                    lap.timestamp <= lobby.timestamp + interval 1 minutes
                """)
      )
      .select(
        col("lap.timestamp"),
        col("lap.session_uid"),
        col("lap.car_index"),
        col("player_name"),
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
        date_format(col("lap.timestamp"), "yyyyMMdd").cast("int").as("date"),
        hour(col("lap.timestamp")).as("hour")
      )
      .as[Lap]
  }
}
