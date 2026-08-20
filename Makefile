.PHONY: build test clean cluster docker-up docker-down demo

# Build targets
build:
	@echo "Building server and client..."
	@mkdir -p bin
	go build -o bin/dkv-server ./cmd/server/
	go build -o bin/dkv-client ./cmd/client/
	@echo "Build complete: bin/dkv-server, bin/dkv-client"

# Test targets
test:
	go test ./internal/... -race -count=1 -timeout 60s

test-v:
	go test ./internal/... -v -race -count=1 -timeout 60s

test-cover:
	go test ./internal/... -race -count=1 -timeout 60s -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Clean
clean:
	rm -rf bin/ data/ coverage.out coverage.html
	@echo "Cleaned build artifacts"

# Run a single node (for testing)
run:
	go run ./cmd/server/ --port 7070 --data-dir ./data/node1

# Run a 3-node cluster locally
cluster:
	@./scripts/run_cluster.sh

# Docker targets
docker-build:
	docker build -t dkv-store -f docker/Dockerfile .

docker-up:
	docker-compose -f docker/docker-compose.yml up -d

docker-down:
	docker-compose -f docker/docker-compose.yml down -v

# Demo: scripted demonstration
demo:
	@echo "=== Distributed KV Store Demo ==="
	@echo ""
	@echo "Starting single-node server..."
	@mkdir -p data/demo
	@go run ./cmd/server/ --port 7070 --data-dir ./data/demo &
	@sleep 1
	@echo ""
	@echo "--- Sending commands ---"
	@echo "SET name Rahil" | go run ./cmd/client/ --addr localhost:7070
	@echo "SET language Go" | go run ./cmd/client/ --addr localhost:7070
	@echo "GET name" | go run ./cmd/client/ --addr localhost:7070
	@echo "GET language" | go run ./cmd/client/ --addr localhost:7070
	@echo ""
	@echo "--- Killing server ---"
	@pkill -f "dkv-server.*7070" || true
	@sleep 1
	@echo ""
	@echo "--- Restarting server (WAL replay) ---"
	@go run ./cmd/server/ --port 7070 --data-dir ./data/demo &
	@sleep 1
	@echo ""
	@echo "--- Verifying persistence ---"
	@echo "GET name" | go run ./cmd/client/ --addr localhost:7070
	@echo "GET language" | go run ./cmd/client/ --addr localhost:7070
	@echo ""
	@pkill -f "dkv-server.*7070" || true
	@rm -rf data/demo
	@echo "Demo complete!"
