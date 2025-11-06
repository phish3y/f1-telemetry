package model

import packet.payload.PacketLobbyInfo
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

case class LobbyInfo(
    timestamp: java.sql.Timestamp,
    session_uid: Long,
    session_time: Float,
    car_index: Int,
    ai_controlled: Int,
    team_id: Int,
    nationality: Int,
    name: String,
    car_number: Int,
    platform: Int,
    ready_status: Int
)

case class TracedLobbyInfo(
    packet: LobbyInfo,
    traceparent: Option[String]
) extends TracedPacket[LobbyInfo]

object LobbyInfo {
  def fromPacket(
      lobbyInfoStream: Dataset[(PacketLobbyInfo, String)]
  )(implicit spark: SparkSession): Dataset[TracedLobbyInfo] = {
    import spark.implicits._

    lobbyInfoStream
      .withColumn("timestamp", current_timestamp())
      .select(
        col("timestamp"),
        col("_1.m_header.m_session_uid").as("session_uid"),
        col("_1.m_header.m_session_time").as("session_time"),
        col("_1.m_header.m_player_car_index").as("car_index"),
        col("_1.m_lobby_players")(col("_1.m_header.m_player_car_index")).as("lobby_data"),
        col("_2").as("traceparent")
      )
      .select(
        col("timestamp"),
        col("session_uid"),
        col("session_time"),
        col("car_index"),
        col("lobby_data.m_ai_controlled").as("ai_controlled"),
        col("lobby_data.m_team_id").as("team_id"),
        col("lobby_data.m_nationality").as("nationality"),
        regexp_replace(
          expr("cast(lobby_data.m_name as string)"),
          "\\x00.*",
          ""
        ).as("name"),
        col("lobby_data.m_car_number").as("car_number"),
        col("lobby_data.m_platform").as("platform"),
        col("lobby_data.m_ready_status").as("ready_status"),
        col("traceparent")
      )
      .as[(java.sql.Timestamp, Long, Float, Int, Int, Int, Int, String, Int, Int, Int, String)]
      .map { case (timestamp, session_uid, session_time, car_index, ai_controlled, team_id, nationality, 
                    name, car_number, platform, ready_status, traceparent) =>
        TracedLobbyInfo(
          packet = LobbyInfo(
            timestamp = timestamp,
            session_uid = session_uid,
            session_time = session_time,
            car_index = car_index,
            ai_controlled = ai_controlled,
            team_id = team_id,
            nationality = nationality,
            name = name,
            car_number = car_number,
            platform = platform,
            ready_status = ready_status
          ),
          traceparent = Option(traceparent)
        )
      }
  }
}
