FROM golang:1.22-alpine AS builder

# Install git and build dependencies
RUN apk add --no-cache git build-base

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the application
RUN CGO_ENABLED=1 go build -o /bin/dbos-cli ./cmd/dbos-cli

# Create a minimal runtime image
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates sqlite

# Copy the built binary
COPY --from=builder /bin/dbos-cli /usr/local/bin/dbos-cli

# Create data directory
RUN mkdir -p /data

# Set working directory
WORKDIR /data

# Set environment variables
ENV DBOS_DB_TYPE=sqlite
ENV DBOS_DB_PATH=/data/dbos.db

# Expose any necessary ports
# EXPOSE 8080

# Set entrypoint
ENTRYPOINT ["dbos-cli"]

# Default command
CMD ["-i"]