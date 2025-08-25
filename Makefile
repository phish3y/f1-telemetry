.PHONY: fmt
fmt:
	cargo fmt

.PHONY: build
build:
	cargo build --release

.PHONY: run
run: fmt build
	RUST_LOG=info cargo run --release
