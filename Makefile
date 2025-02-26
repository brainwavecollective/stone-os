.PHONY: all build clean test lint deps docker docker-run

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOLINT=golangci-lint

# Binary name
BINARY_NAME=dbos-cli
BINARY_UNIX=$(BINARY_NAME)_unix

# Build directory
BUILD_DIR=./bin

# Main package path
MAIN_PACKAGE=./cmd/dbos-cli

# Docker parameters
DOCKER_IMAGE=dbos-cli
DOCKER_TAG=latest

all: deps lint test build

build:
	mkdir -p $(BUILD_DIR)
	CGO_ENABLED=1 $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) -v $(MAIN_PACKAGE)

clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)

test:
	$(GOTEST) -v ./...

lint:
	$(GOLINT) run ./...

deps:
	$(GOMOD) download
	$(GOMOD) tidy

# Cross compilation
build-linux:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_UNIX) -v $(MAIN_PACKAGE)

docker:
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) -f deployments/docker/Dockerfile .

docker-run:
	docker run -it --rm -v $(PWD)/data:/data $(DOCKER_IMAGE):$(DOCKER_TAG)

run:
	$(BUILD_DIR)/$(BINARY_NAME)

# Install development tools
install-tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Generate Go module files
init-module:
	$(GOMOD) init github.com/brainwavecollective/stone-os
	$(GOMOD) tidy

# Generate initial schema migration
generate-schema:
	go run scripts/generate-schema.go > pkg/schema/migrations/000001_initial_schema.up.sql

# Help
help:
	@echo "Make targets:"
	@echo "  build         - Build the binary"
	@echo "  clean         - Clean build files"
	@echo "  test          - Run tests"
	@echo "  lint          - Run linters"
	@echo "  deps          - Update dependencies"
	@echo "  build-linux   - Cross-compile for Linux"
	@echo "  docker        - Build Docker image"
	@echo "  docker-run    - Run Docker container"
	@echo "  run           - Run the binary"
	@echo "  install-tools - Install development tools"
	@echo "  init-module   - Initialize Go module"
	@echo "  generate-schema - Generate initial schema migration"