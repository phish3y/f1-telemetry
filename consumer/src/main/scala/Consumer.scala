import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoders
import org.apache.log4j.Logger
import java.util.Properties
import java.time.format.DateTimeFormatter
import scala.util.Random
import java.time.Instant
import java.time.temporal.ChronoUnit
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.SerializationFeature
import java.sql.Timestamp
import org.apache.log4j.Level
import org.apache.spark.sql.streaming.Trigger

import packet.Header
import packet.payload.{Lap, PacketLap, CarTelemetry, PacketCarTelemetry}
import aggregation.SpeedAggregation
import aggregation.RPMAggregation
import packet.payload.PacketParticipants
import packet.payload.LobbyInfo
import packet.payload.PacketLobbyInfo

object Consumer {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.WARN)

    val warehousePath = sys.env.get("WAREHOUSE_PATH") match {
      case Some(path) => path
      case None =>
        throw new IllegalArgumentException("WAREHOUSE_PATH environment variable required")
    }

    val kafkaBroker = sys.env.get("KAFKA_BROKER") match {
      case Some(broker) => broker
      case None => throw new IllegalArgumentException("KAFKA_BROKER environment variable required")
    }

    val participantsTopic = sys.env.get("PARTICIPANTS_TOPIC") match {
      case Some(broker) => broker
      case None =>
        throw new IllegalArgumentException("PARTICIPANTS_TOPIC environment variable required")
    }

    val lobbyInfoTopic = sys.env.get("LOBBY_INFO_TOPIC") match {
      case Some(broker) => broker
      case None =>
        throw new IllegalArgumentException("LOBBY_INFO_TOPIC environment variable required")
    }

    val lapTopic = sys.env.get("LAP_TOPIC") match {
      case Some(broker) => broker
      case None => throw new IllegalArgumentException("LAP_TOPIC environment variable required")
    }

    val carTelemetryTopic = sys.env.get("CAR_TELEMETRY_TOPIC") match {
      case Some(broker) => broker
      case None =>
        throw new IllegalArgumentException("CAR_TELEMETRY_TOPIC environment variable required")
    }

    val speedAggregationTopic = sys.env.get("SPEED_AGGREGATION_TOPIC") match {
      case Some(topic) => topic
      case None =>
        throw new IllegalArgumentException("SPEED_AGGREGATION_TOPIC environment variable required")
    }

    val rpmAggregationTopic = sys.env.get("RPM_AGGREGATION_TOPIC") match {
      case Some(topic) => topic
      case None =>
        throw new IllegalArgumentException("RPM_AGGREGATION_TOPIC environment variable required")
    }

    implicit val spark: SparkSession = SparkSession.builder
      .appName("f1-telemetry-consumer")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", warehousePath)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO
      .config("spark.sql.adaptive.enabled", "false")                            // TODO
      .config("fs.permissions.umask-mode", "000")
      .getOrCreate()

    import spark.implicits._

    def createIcebergTables(): Unit = {
      spark.sql("""
        CREATE TABLE IF NOT EXISTS local.lap (
          timestamp timestamp,
          session_uid bigint,

          car_index int,
          player_name string,

          current_lap_num int,
          last_lap_time_ms bigint,
          current_lap_time_ms bigint,

          sector1_time_ms bigint,
          sector2_time_ms bigint,

          car_position int,

          speed_trap_fastest_speed float,

          num_pit_stops int,
          penalties int,
          total_warnings int,
          corner_cutting_warnings int,

          grid_position int,
          
          date int,
          hour int
        ) USING iceberg
        PARTITIONED BY (date, hour)
      """)
    }

    createIcebergTables()

    // real time
    val carTelemetrySchema = Encoders.product[PacketCarTelemetry].schema
    val carTelemetryStream: Dataset[PacketCarTelemetry] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", carTelemetryTopic)
      .load()
      .select(from_json($"value".cast("string"), carTelemetrySchema).as("data"))
      .select($"data.*")
      .as[PacketCarTelemetry]

    val speedAggregation = SpeedAggregation.calculate(carTelemetryStream)
    val rpmAggregation   = RPMAggregation.calculate(carTelemetryStream)

    val speedAggregationWithPartitions = speedAggregation
      .withColumn("date", date_format($"window_start", "yyyyMMdd"))
      .withColumn("hour", hour($"window_start"))

    val rpmAggregationWithPartitions = rpmAggregation
      .withColumn("date", date_format($"window_start", "yyyyMMdd"))
      .withColumn("hour", hour($"window_start"))

    speedAggregation
      .select(
        $"session_uid".cast("string").as("key"),
        to_json(struct($"*")).as("value")
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", speedAggregationTopic)
      .option("checkpointLocation", "/tmp/spark-checkpoints/speed-kafka")
      .outputMode("append")
      .start()

    rpmAggregation
      .select(
        $"session_uid".cast("string").as("key"),
        to_json(struct($"*")).as("value")
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", rpmAggregationTopic)
      .option("checkpointLocation", "/tmp/spark-checkpoints/rpm-kafka")
      .outputMode("append")
      .start()

    // historial
    val lapSchema = Encoders.product[PacketLap].schema
    val lapStream: Dataset[PacketLap] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", lapTopic)
      .load()
      .select(from_json($"value".cast("string"), lapSchema).as("data"))
      .select($"data.*")
      .as[PacketLap]

    val participantsSchema = Encoders.product[PacketParticipants].schema
    val participantsStream: Dataset[PacketParticipants] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", participantsTopic)
      .load()
      .select(from_json($"value".cast("string"), participantsSchema).as("data"))
      .select($"data.*")
      .as[PacketParticipants]

    val lobbyInfoSchema = Encoders.product[PacketLobbyInfo].schema
    val lobbyInfoStream: Dataset[PacketLobbyInfo] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", lobbyInfoTopic)
      .load()
      .select(from_json($"value".cast("string"), lobbyInfoSchema).as("data"))
      .select($"data.*")
      .as[PacketLobbyInfo]

    model.Lap
      .fromPacket(lapStream, lobbyInfoStream)
      .writeStream
      .format("iceberg")
      .outputMode("append")
      .option("path", s"$warehousePath/lap")
      .option("table", "local.lap")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .option("checkpointLocation", "/tmp/spark-checkpoints/lap")
      .start()

    // Iceberg
    // enrichedLaps
    //   .select(
    //     $"timestamp",
    //     $"session_uid",
    //     $"player_car_index",
    //     $"m_participants".getItem($"player_car_index").getField("m_name").as("driver_name"),
    //     $"m_participants".getItem($"player_car_index").getField("m_team_id").as("team_id"),
    //     $"m_participants".getItem($"player_car_index").getField("m_race_number").as("race_number"),
    //     $"m_participants".getItem($"player_car_index").getField("m_nationality").as("nationality"),
    //     $"m_last_lap_time_in_ms".as("last_lap_time_ms"),
    //     $"m_current_lap_time_in_ms".as("current_lap_time_ms"),
    //     $"m_lap_distance".as("lap_distance"),
    //     $"m_total_distance".as("total_distance"),
    //     $"m_car_position".as("car_position"),
    //     $"m_current_lap_num".as("current_lap_num"),
    //     $"m_pit_status".as("pit_status"),
    //     $"m_sector".as("sector"),
    //     date_format($"timestamp", "yyyyMMdd").cast("int").as("date"),
    //     hour($"timestamp").as("hour")
    //   )
    //   .writeStream
    //   .format("iceberg")
    //   .outputMode("append")
    //   .option("path", s"$warehousePath/enriched_laps")
    //   .option("table", "local.enriched_laps")
    //   .trigger(Trigger.ProcessingTime("5 seconds"))
    //   .option("checkpointLocation", "/tmp/spark-checkpoints/enriched-laps")
    //   .start()

    spark.streams.awaitAnyTermination()

    spark.stop()
  }
}
