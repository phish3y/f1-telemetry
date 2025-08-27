FROM rust:1.88 AS builder

RUN apt-get update && apt-get install -y cmake

WORKDIR /usr/src/f1-telemetry

RUN mkdir -p src && echo "fn main() {}" > src/main.rs
COPY Cargo.lock Cargo.toml ./
RUN cargo build --release --target-dir /usr/local/cargo/bin
COPY . .
RUN touch -a -m src/main.rs
RUN cargo build --release --target-dir /usr/local/cargo/bin

FROM debian:stable-slim
RUN apt-get update && apt-get install -y lsb-release ca-certificates && apt-get clean all
COPY --from=builder /usr/local/cargo/bin/release/f1-telemetry /usr/local/bin/f1-telemetry
ENTRYPOINT ["/usr/local/bin/f1-telemetry"]
