.PHONY: fmt
fmt:
	cd producer && cargo fmt
	cd consumer && sbt scalafmtAll

.PHONY: build
build: fmt
	cd producer && cargo build --release
	cd consumer && sbt package

.PHONY: up
up:
	docker compose up --build

.PHONY: down
down:
	docker compose down