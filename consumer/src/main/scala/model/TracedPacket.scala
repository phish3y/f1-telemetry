package model

import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql.functions._
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{SpanKind, SpanContext, TraceFlags, TraceState}

trait TracedPacket[T] {
  def packet: T
  def traceparent: String
}

object TracedPacket {
  private val TRACER_NAME = "f1-telemetry-consumer"
  
  def writeWithTracing[T](
    batchDF: Dataset[_ <: TracedPacket[T]],
    table: String,
    path: String,
    spanName: String
  )(implicit encoder: org.apache.spark.sql.Encoder[T]): Unit = {
    val tracer = GlobalOpenTelemetry.getTracer(TRACER_NAME)
    
    val uniqueTraceparents = batchDF.select("traceparent").distinct().collect().map(_.getString(0))
    uniqueTraceparents.foreach { traceparent =>
      val spanBuilder = tracer.spanBuilder(spanName)
        .setSpanKind(SpanKind.CONSUMER)
      
      val parts = traceparent.split("-")
      require(parts.length == 4, s"invalid traceparent format: ${traceparent}")
      
      val traceId = parts(1)
      val spanId = parts(2)
      
      val remoteSpanContext = SpanContext.createFromRemoteParent(
        traceId,
        spanId,
        TraceFlags.getSampled(),
        TraceState.getDefault()
      )
      
      spanBuilder.addLink(remoteSpanContext)
      
      val span = spanBuilder.startSpan()
      
      span.addEvent(s"${spanName}_complete")
      span.end()
    }
    
    // Write packets to Iceberg
    import batchDF.sparkSession.implicits._
    val packets = batchDF.map(_.packet)
    
    packets
      .write
      .format("iceberg")
      .mode("append")
      .option("path", path)
      .option("table", table)
      .save()
  }
}
