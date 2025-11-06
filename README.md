# 🏎️ F1 Telemetry Streaming Pipeline

F1 Telemetry is a streaming data platform that captures, processes, and visualizes Formula 1 game telemetry data in real-time

### Tech
- Rust (UDP parsing + API)
- Apache Spark with Scala
- Apache Kafka
- Apache Iceberg
- Vue.js with TypeScript
- Docker (Compose)
- OpenTelemetry + Jaeger

### Run

```bash
make up
```

### Useful Links

- F1 Dashboard: http://localhost:80
- Spark Dashboard: http://localhost:81
- Jaeger Dashboard: http://localhost:82

### TODO
- expose spark UI
- balance topic partitions with size of spark cluster
