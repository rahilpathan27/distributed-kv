# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /dkv-server ./cmd/server/
RUN CGO_ENABLED=0 GOOS=linux go build -o /dkv-client ./cmd/client/

# Runtime stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

COPY --from=builder /dkv-server /usr/local/bin/dkv-server
COPY --from=builder /dkv-client /usr/local/bin/dkv-client

EXPOSE 7070 7071

VOLUME ["/data"]

ENTRYPOINT ["dkv-server"]
CMD ["--port", "7070", "--repl-port", "7071", "--data-dir", "/data"]
