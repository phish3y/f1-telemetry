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
import aggregation.{SpeedAggregation, TracedSpeedAggregation, RPMAggregation, TracedRPMAggregation, TracedAggregation}
import model.{Participant => ParticipantModel, TracedParticipant}
import model.{LobbyInfo => LobbyInfoModel, TracedLobbyInfo}
import model.TracedPacket
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
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "false")
      .config("fs.permissions.umask-mode", "000")
      .getOrCreate()

    import spark.implicits._

    def createIcebergTables(): Unit = {
      spark.sql("""
        CREATE TABLE IF NOT EXISTS local.lap (
          window_start timestamp,
          window_end timestamp,
          session_uid bigint,
          session_time float,
          car_index int,
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
          sample_count bigint
        ) USING iceberg
        PARTITIONED BY (session_uid)
      """)

      spark.sql("""
        CREATE TABLE IF NOT EXISTS local.speed_aggregation (
          window_start timestamp,
          window_end timestamp,
          session_uid bigint,
          session_time float,
          avg_speed double,
          min_speed int,
          max_speed int,
          sample_count bigint
        ) USING iceberg
        PARTITIONED BY (session_uid)
      """)

      spark.sql("""
        CREATE TABLE IF NOT EXISTS local.rpm_aggregation (
          window_start timestamp,
          window_end timestamp,
          session_uid bigint,
          session_time float,
          avg_rpm double,
          min_rpm int,
          max_rpm int,
          sample_count bigint
        ) USING iceberg
        PARTITIONED BY (session_uid)
      """)

      spark.sql("""
        CREATE TABLE IF NOT EXISTS local.participants (
          timestamp timestamp,
          session_uid bigint,
          session_time float,
          car_index int,
          ai_controlled int,
          driver_id int,
          team_id int,
          race_number int,
          nationality int,
          name string,
          platform int
        ) USING iceberg
        PARTITIONED BY (session_uid)
      """)

      spark.sql("""
        CREATE TABLE IF NOT EXISTS local.lobby_info (
          timestamp timestamp,
          session_uid bigint,
          session_time float,
          car_index int,
          ai_controlled int,
          team_id int,
          nationality int,
          name string,
          car_number int,
          platform int,
          ready_status int
        ) USING iceberg
        PARTITIONED BY (session_uid)
      """)
    }

    createIcebergTables()
    
    // real time aggregates - Both Kafka and Iceberg
    val carTelemetrySchema = Encoders.product[PacketCarTelemetry].schema
    
    val carTelemetryStreamRaw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", carTelemetryTopic)
      .option("includeHeaders", "true")
      .load()
    
    val carTelemetryWithTrace = carTelemetryStreamRaw
      .select(
        from_json($"value".cast("string"), carTelemetrySchema).as("data"),
        expr("filter(headers, x -> x.key = 'traceparent')[0].value").cast("string").as("traceparent")
      )
      .select($"data.*", $"traceparent")
    
    val carTelemetryStream = carTelemetryWithTrace.as[PacketCarTelemetry]

    val speedAggregation = SpeedAggregation.calculate(carTelemetryWithTrace)
    val rpmAggregation   = RPMAggregation.calculate(carTelemetryWithTrace)

    speedAggregation
      .writeStream
      .foreachBatch { (batchDF: Dataset[TracedSpeedAggregation], _: Long) =>
        // Write to Kafka
        TracedAggregation.writeWithTracing(batchDF, kafkaBroker, speedAggregationTopic, "speed-aggregation-window")
        
        // Write to Iceberg
        import batchDF.sparkSession.implicits._
        val aggregations = batchDF.map(_.aggregation)
        aggregations
          .write
          .format("iceberg")
          .mode("append")
          .option("path", s"$warehousePath/speed_aggregation")
          .option("table", "local.speed_aggregation")
          .save()
      }
      .option("checkpointLocation", "/tmp/spark-checkpoints/speed-kafka")
      .outputMode("append")
      .start()

    rpmAggregation
      .writeStream
      .foreachBatch { (batchDF: Dataset[TracedRPMAggregation], _: Long) =>
        // Write to Kafka
        TracedAggregation.writeWithTracing(batchDF, kafkaBroker, rpmAggregationTopic, "rpm-aggregation-window")
        
        // Write to Iceberg
        import batchDF.sparkSession.implicits._
        val aggregations = batchDF.map(_.aggregation)
        aggregations
          .write
          .format("iceberg")
          .mode("append")
          .option("path", s"$warehousePath/rpm_aggregation")
          .option("table", "local.rpm_aggregation")
          .save()
      }
      .option("checkpointLocation", "/tmp/spark-checkpoints/rpm-kafka")
      .outputMode("append")
      .start()

    // lap aggregation - Iceberg only
    val lapSchema = Encoders.product[PacketLap].schema
    val lapStreamRaw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", lapTopic)
      .option("includeHeaders", "true")
      .load()
    
    val lapWithTrace = lapStreamRaw
      .select(
        from_json($"value".cast("string"), lapSchema).as("data"),
        expr("filter(headers, x -> x.key = 'traceparent')[0].value").cast("string").as("traceparent")
      )
      .select($"data.*", $"traceparent")

    val lapAggregation = aggregation.LapAggregation.calculate(lapWithTrace)

    lapAggregation
      .writeStream
      .foreachBatch { (batchDF: Dataset[aggregation.TracedLapAggregation], _: Long) =>
        import batchDF.sparkSession.implicits._
        val aggregations = batchDF.map(_.aggregation)
        aggregations
          .write
          .format("iceberg")
          .mode("append")
          .option("path", s"$warehousePath/lap")
          .option("table", "local.lap")
          .save()
      }
      .option("checkpointLocation", "/tmp/spark-checkpoints/lap")
      .outputMode("append")
      .start()

    // historical only - Iceberg
    val participantsSchema = Encoders.product[PacketParticipants].schema
    val participantsStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", participantsTopic)
      .option("includeHeaders", "true")
      .load()
      .select(
        from_json($"value".cast("string"), participantsSchema).as("_1"),
        expr("filter(headers, x -> x.key = 'traceparent')[0].value").cast("string").as("_2")
      )
      .as[(PacketParticipants, String)]

    val lobbyInfoSchema = Encoders.product[PacketLobbyInfo].schema
    val lobbyInfoStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", lobbyInfoTopic)
      .option("includeHeaders", "true")
      .load()
      .select(
        from_json($"value".cast("string"), lobbyInfoSchema).as("_1"),
        expr("filter(headers, x -> x.key = 'traceparent')[0].value").cast("string").as("_2")
      )
      .as[(PacketLobbyInfo, String)]

    ParticipantModel
      .fromPacket(participantsStream)
      .writeStream
      .foreachBatch { (batchDF: Dataset[TracedParticipant], _: Long) =>
        TracedPacket.writeWithTracing(batchDF, "local.participants", s"$warehousePath/participants", "participants-write")
      }
      .option("checkpointLocation", "/tmp/spark-checkpoints/participants")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    LobbyInfoModel
      .fromPacket(lobbyInfoStream)
      .writeStream
      .foreachBatch { (batchDF: Dataset[TracedLobbyInfo], _: Long) =>
        TracedPacket.writeWithTracing(batchDF, "local.lobby_info", s"$warehousePath/lobby_info", "lobby-info-write")
      }
      .option("checkpointLocation", "/tmp/spark-checkpoints/lobby-info")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    spark.streams.awaitAnyTermination()

    spark.stop()
  }
}
