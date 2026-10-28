.PHONY: run test race bench pprof docker-up docker-down lint

run:
	go run ./cmd/alerter/main.go

test:
	go test -v ./...

race:
	go test -race -v ./...

bench:
	go test -bench=. -benchmem ./internal/engine/... ./internal/cache/...

pprof-cpu:
	go tool pprof http://localhost:8080/debug/pprof/profile?seconds=10

pprof-heap:
	go tool pprof http://localhost:8080/debug/pprof/heap

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v
