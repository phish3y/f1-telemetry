package model

import packet.payload.PacketLobbyInfo
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

case class LobbyInfo(
    timestamp: java.sql.Timestamp,
    session_uid: Long,
    car_index: Int,
    ai_controlled: Int,
    team_id: Int,
    nationality: Int,
    name: String,
    car_number: Int,
    platform: Int,
    ready_status: Int,
    date: Int,
    hour: Int
)

case class TracedLobbyInfo(
    packet: LobbyInfo,
    traceparent: String
) extends TracedPacket[LobbyInfo]

object LobbyInfo {
  def fromPacket(
      lobbyInfoStreamRaw: org.apache.spark.sql.DataFrame
  )(implicit spark: SparkSession): Dataset[TracedLobbyInfo] = {
    import spark.implicits._

    val lobbyInfoStream = lobbyInfoStreamRaw
      .select(
        col("data.*"),
        col("traceparent")
      )
      .as[(PacketLobbyInfo, String)]

    lobbyInfoStream
      .withColumn("timestamp", current_timestamp())
      .select(
        col("timestamp"),
        col("_1.m_header.m_session_uid").as("session_uid"),
        posexplode(col("_1.m_lobby_players")).as(Seq("car_index", "lobby_data")),
        col("_2").as("traceparent")
      )
      .select(
        col("timestamp"),
        col("session_uid"),
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
        date_format(col("timestamp"), "yyyyMMdd").cast("int").as("date"),
        hour(col("timestamp")).as("hour"),
        col("traceparent")
      )
      .as[(java.sql.Timestamp, Long, Int, Int, Int, Int, String, Int, Int, Int, Int, Int, String)]
      .map { case (timestamp, session_uid, car_index, ai_controlled, team_id, nationality, 
                    name, car_number, platform, ready_status, date, hour, traceparent) =>
        TracedLobbyInfo(
          packet = LobbyInfo(
            timestamp = timestamp,
            session_uid = session_uid,
            car_index = car_index,
            ai_controlled = ai_controlled,
            team_id = team_id,
            nationality = nationality,
            name = name,
            car_number = car_number,
            platform = platform,
            ready_status = ready_status,
            date = date,
            hour = hour
          ),
          traceparent = traceparent
        )
      }
  }
}
