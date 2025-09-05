.PHONY: fmt
fmt:
	cd producer && cargo fmt
	cd socket && cargo fmt
	cd consumer && sbt scalafmtAll
	cd web/f1-telemetry && npm run format

.PHONY: build
build: fmt
	cd producer && cargo build --release
	cd socket && cargo build --release
	cd consumer && sbt package

.PHONY: up
up:
	docker compose up --build

.PHONY: down
down:
	docker compose down