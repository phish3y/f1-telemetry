FROM rust:1.88 as builder

RUN apt-get update && apt-get install -y cmake

WORKDIR /usr/src/socket

RUN mkdir -p src && echo "fn main() {}" > src/main.rs
COPY Cargo.lock Cargo.toml ./
RUN cargo build --release --target-dir /usr/local/cargo/bin
COPY . .
RUN touch -a -m src/main.rs
RUN cargo build --release --target-dir /usr/local/cargo/bin

FROM debian:stable-slim

RUN apt-get update && apt-get install -y lsb-release ca-certificates && apt-get clean all
COPY --from=builder /usr/local/cargo/bin/release/socket /usr/local/bin/socket

ENTRYPOINT ["/usr/local/bin/socket"]
