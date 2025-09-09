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

case class Header(
    m_packet_format: Int,
    m_game_year: Int,
    m_game_major_version: Int,
    m_game_minor_version: Int,
    m_packet_version: Int,
    m_packet_id: Int,
    m_session_uid: Long,
    m_session_time: Float,
    m_frame_identifier: Long,
    m_overall_frame_identifier: Long,
    m_player_car_index: Int,
    m_secondary_player_car_index: Int
)

case class Lap(
    m_last_lap_time_in_ms: Long,
    m_current_lap_time_in_ms: Long,
    m_sector1_time_ms_part: Int,
    m_sector1_time_minutes_part: Int,
    m_sector2_time_ms_part: Int,
    m_sector2_time_minutes_part: Int,
    m_delta_to_car_in_front_ms_part: Int,
    m_delta_to_car_in_front_minutes_part: Int,
    m_delta_to_race_leader_ms_part: Int,
    m_delta_to_race_leader_minutes_part: Int,
    m_lap_distance: Float,
    m_total_distance: Float,
    m_safety_car_delta: Float,
    m_car_position: Int,
    m_current_lap_num: Int,
    m_pit_status: Int,
    m_num_pit_stops: Int,
    m_sector: Int,
    m_current_lap_invalid: Int,
    m_penalties: Int,
    m_total_warnings: Int,
    m_corner_cutting_warnings: Int,
    m_num_unserved_drive_through_pens: Int,
    m_num_unserved_stop_go_pens: Int,
    m_grid_position: Int,
    m_driver_status: Int,
    m_result_status: Int,
    m_pit_lane_timer_active: Int,
    m_pit_lane_time_in_lane_in_ms: Int,
    m_pit_stop_timer_in_ms: Int,
    m_pit_stop_should_serve_pen: Int,
    m_speed_trap_fastest_speed: Float,
    m_speed_trap_fastest_lap: Int
)

case class PacketLap(
    m_header: Header,
    m_lap_data: Array[Lap],
    m_time_trial_pb_car_idx: Int,
    m_time_trial_rival_car_idx: Int
)

case class CarTelemetry(
    m_speed: Int,
    m_throttle: Float,
    m_steer: Float,
    m_brake: Float,
    m_clutch: Int,
    m_gear: Int,
    m_engine_rpm: Int,
    m_drs: Int,
    m_rev_lights_percent: Int,
    m_rev_lights_bit_value: Int,
    m_brakes_temperature: Array[Int],
    m_tyres_surface_temperature: Array[Int],
    m_tyres_inner_temperature: Array[Int],
    m_engine_temperature: Int,
    m_tyres_pressure: Array[Float],
    m_surface_type: Array[Int]
)

case class PacketCarTelemetry(
    m_header: Header,
    m_car_telemetry_data: Array[CarTelemetry],
    m_mfd_panel_index: Int,
    m_mfd_panel_index_secondary_player: Int,
    m_suggested_gear: Int
)

case class SpeedAggregation(
    window_start: java.sql.Timestamp,
    window_end: java.sql.Timestamp,
    session_uid: Long,
    avg_speed: Double,
    min_speed: Int,
    max_speed: Int,
    sample_count: Long
)

object Consumer {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.WARN)

    val kafkaBroker = sys.env.get("KAFKA_BROKER") match {
      case Some(broker) => broker
      case None => throw new IllegalArgumentException("KAFKA_BROKER environment variable required")
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

    val spark: SparkSession = SparkSession.builder
      .appName("f1-telemetry-consumer")
      // .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      // .config(
      //   "spark.hadoop.fs.s3a.aws.credentials.provider",
      //   "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
      // )
      .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
      .getOrCreate()

    import spark.implicits._

    // val lapSchema = Encoders.product[PacketLap].schema

    // val lapStream: Dataset[PacketLap] = spark.readStream
    //   .format("kafka")
    //   .option("kafka.bootstrap.servers", kafkaBroker)
    //   .option("subscribe", lapTopic)
    //   .load()
    //   .select(from_json($"value".cast("string"), lapSchema).as("data"))
    //   .select($"data.*")
    //   .as[PacketLap]

    val carTelemetrySchema = Encoders.product[PacketCarTelemetry].schema

    val carTelemetryStream: Dataset[PacketCarTelemetry] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", carTelemetryTopic)
      .load()
      .select(from_json($"value".cast("string"), carTelemetrySchema).as("data"))
      .select($"data.*")
      .as[PacketCarTelemetry]

    val playerCarTelemetry = carTelemetryStream
      .withColumn("timestamp", current_timestamp())
      .select(
        $"timestamp",
        $"m_header.m_session_uid".as("session_uid"),
        $"m_car_telemetry_data" ($"m_header.m_player_car_index")("m_speed").as("speed")
      )

    val speedAggregation = playerCarTelemetry
      .withWatermark("timestamp", "10 seconds")
      .groupBy(
        window($"timestamp", "3 seconds"),
        $"session_uid"
      )
      .agg(
        avg($"speed").as("avg_speed"),
        min($"speed").as("min_speed"),
        max($"speed").as("max_speed"),
        count("*").as("sample_count")
      )
      .select(
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"session_uid",
        $"avg_speed",
        $"min_speed",
        $"max_speed",
        $"sample_count"
      )
      .as[SpeedAggregation]

    val kafkaOutput = speedAggregation
      .select(
        $"session_uid".cast("string").as("key"),
        to_json(struct($"*")).as("value")
      )

    val kafkaQuery = kafkaOutput.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", speedAggregationTopic)
      .outputMode("append")
      // .trigger(Trigger.ProcessingTime("5 seconds"))
      // .queryName("speed-aggregation")
      .start()

    // val kafkaQuery = kafkaOutput.writeStream
    //   .format("kafka")
    //   .option("kafka.bootstrap.servers", kafkaBroker)
    //   .option("topic", speedAggregationTopic)
    //   .outputMode("append")
    //   .trigger(Trigger.ProcessingTime("5 seconds"))
    //   .queryName("speed-aggregation")
    //   .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
    //     try {
    //       println(s"Attempting to write batch $batchId to Kafka")
    //       batchDF.write
    //         .format("kafka")
    //         .option("kafka.bootstrap.servers", kafkaBroker)
    //         .option("topic", speedAggregationTopic)
    //         .save()
    //       println(s"Successfully wrote ${batchDF.count()} records to Kafka")
    //     } catch {
    //       case e: Exception => 
    //         println(s"Failed to write to Kafka: ${e.getMessage}")
    //         e.printStackTrace()
    //     }
    //   }
    //   .start()

    kafkaQuery.awaitTermination()

    spark.stop()
  }
}

// /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic speed-aggregation --from-beginning