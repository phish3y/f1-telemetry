# f1-telemetry

F1 Telemetry is currently a Rust binary that subscribes to a UDP stream being produce by the game F1 2025. A subset of the packets are parsed and sent to a configured Kakfa topic for further processing.

## Running locally

#### Install dependencies
- docker
- docker compose
- cargo
- sbt

#### Run
```sh
make up
```
