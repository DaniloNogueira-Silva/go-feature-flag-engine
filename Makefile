.PHONY: build test bench up down clean

build:
	go build -o bin/control-plane ./cmd/control-plane
	go build -o bin/data-plane ./cmd/data-plane
	go build -o bin/demo-app ./cmd/demo-app

test:
	go test -v -race ./internal/... ./pkg/...

bench:
	go test -bench=. -benchmem ./internal/domain/flag ./pkg/sdk

up:
	docker compose -f deployments/docker-compose.yml up --build -d

down:
	docker compose -f deployments/docker-compose.yml down -v

clean:
	rm -rf bin/ *.exe

