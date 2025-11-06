package aggregation

import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql.functions._
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{SpanKind, SpanContext, TraceFlags, TraceState}

trait Aggregation {
  def window_start: java.sql.Timestamp
  def window_end: java.sql.Timestamp
  def session_uid: Long
  def sample_count: Long
}

trait TracedAggregation[T <: Aggregation] {
  def aggregation: T
  def traceparents: Seq[String]
}

object TracedAggregation {
  private val TRACER_NAME = "f1-telemetry-consumer"
  
  def writeWithTracing[T <: Aggregation](
    batchDF: Dataset[_ <: TracedAggregation[T]],
    kafkaBroker: String,
    topic: String,
    spanName: String
  )(implicit encoder: org.apache.spark.sql.Encoder[T]): Unit = {
    val tracer = GlobalOpenTelemetry.getTracer(TRACER_NAME)
    import batchDF.sparkSession.implicits._
    
    batchDF.collect().foreach { traced =>
      val validTraceparents = traced.traceparents.filter(_ != null)
      
      if (validTraceparents.nonEmpty) {
        val spanBuilder = tracer.spanBuilder(spanName)
          .setSpanKind(SpanKind.CONSUMER)
        
        // Add links to all producer spans that contributed to this aggregation
        validTraceparents.foreach { traceparent =>
          val parts = traceparent.split("-")
          require(parts.length == 4, s"invalid traceparent format: $traceparent")
          
          val traceId = parts(1)
          val spanId = parts(2)
          
          val remoteSpanContext = SpanContext.createFromRemoteParent(
            traceId,
            spanId,
            TraceFlags.getSampled(),
            TraceState.getDefault()
          )
          
          spanBuilder.addLink(remoteSpanContext)
        }
        
        val span = spanBuilder.startSpan()
        
        span.setAttribute("session_uid", traced.aggregation.session_uid)
        span.setAttribute("window_start", traced.aggregation.window_start.toString)
        span.setAttribute("window_end", traced.aggregation.window_end.toString)
        span.setAttribute("sample_count", traced.aggregation.sample_count)
        
        span.addEvent(s"${spanName}_complete")
        span.end()
      }
    }
    
    batchDF
      .map(_.aggregation)
      .selectExpr("CAST(session_uid AS STRING) AS key", "to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", topic)
      .save()
  }
}
