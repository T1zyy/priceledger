.PHONY: up down test integration demo verify
up:
	docker compose up -d --build pricing
down:
	docker compose down
test:
	go test -race ./...
integration:
	docker compose --profile test run --rm --build tests go test -count=1 -v ./internal/...
demo: up
	docker compose --profile test run --rm --build -e LIVE_DEMO=1 tests go test -count=1 -v ./integration
verify: integration demo
