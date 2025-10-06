package model

import packet.payload.PacketParticipants
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

case class Participant(
    timestamp: java.sql.Timestamp,
    session_uid: Long,
    car_index: Int,
    ai_controlled: Int,
    driver_id: Int,
    team_id: Int,
    race_number: Int,
    nationality: Int,
    name: String,
    platform: Int,
    date: Int,
    hour: Int
)

case class TracedParticipant(
    packet: Participant,
    traceparent: String
) extends TracedPacket[Participant]

object Participant {
  def fromPacket(
      participantsStreamRaw: org.apache.spark.sql.DataFrame
  )(implicit spark: SparkSession): Dataset[TracedParticipant] = {
    import spark.implicits._

    val participantsStream = participantsStreamRaw
      .select(
        col("data.*"),
        col("traceparent")
      )
      .as[(PacketParticipants, String)]

    participantsStream
      .withColumn("timestamp", current_timestamp())
      .select(
        col("timestamp"),
        col("_1.m_header.m_session_uid").as("session_uid"),
        posexplode(col("_1.m_participants")).as(Seq("car_index", "participant_data")),
        col("_2").as("traceparent")
      )
      .select(
        col("timestamp"),
        col("session_uid"),
        col("car_index"),
        col("participant_data.m_ai_controlled").as("ai_controlled"),
        col("participant_data.m_driver_id").as("driver_id"),
        col("participant_data.m_team_id").as("team_id"),
        col("participant_data.m_race_number").as("race_number"),
        col("participant_data.m_nationality").as("nationality"),
        regexp_replace(
          expr("cast(participant_data.m_name as string)"),
          "\\x00.*",
          ""
        ).as("name"),
        col("participant_data.m_platform").as("platform"),
        date_format(col("timestamp"), "yyyyMMdd").cast("int").as("date"),
        hour(col("timestamp")).as("hour"),
        col("traceparent")
      )
      .as[(java.sql.Timestamp, Long, Int, Int, Int, Int, Int, Int, String, Int, Int, Int, String)]
      .map { case (timestamp, session_uid, car_index, ai_controlled, driver_id, team_id, 
                    race_number, nationality, name, platform, date, hour, traceparent) =>
        TracedParticipant(
          packet = Participant(
            timestamp = timestamp,
            session_uid = session_uid,
            car_index = car_index,
            ai_controlled = ai_controlled,
            driver_id = driver_id,
            team_id = team_id,
            race_number = race_number,
            nationality = nationality,
            name = name,
            platform = platform,
            date = date,
            hour = hour
          ),
          traceparent = traceparent
        )
      }
  }
}
