.PHONY: build test test-race lint bench clean run

build:
	go build -o bin/broker ./cmd/broker
	go build -o bin/producer ./cmd/producer
	go build -o bin/consumer ./cmd/consumer

test:
	go test ./... -count=1

test-race:
	go test ./... -race -count=1

test-integration:
	go test ./test/integration/... -tags=integration -count=1

lint:
	gofmt -l .
	go vet ./...
	golangci-lint run

bench:
	go test ./benchmark/... -bench=. -benchmem -benchtime=10s

cover:
	go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html

run: build
	./bin/broker --config config/broker.yaml

clean:
	rm -rf bin/ coverage.out coverage.html /tmp/mini-kafka-*
