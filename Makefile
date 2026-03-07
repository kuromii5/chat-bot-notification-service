.PHONY: run build lint fmt tidy

run:
	go run cmd/main.go

build:
	go build -o bin/notification-service ./cmd

lint:
	golangci-lint run

fmt:
	golangci-lint fmt

tidy:
	go mod tidy
