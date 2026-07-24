FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build all binaries
RUN CGO_ENABLED=0 GOOS=linux go build -o /mini-kafka-broker ./cmd/broker
RUN CGO_ENABLED=0 GOOS=linux go build -o /mini-kafka-producer ./cmd/producer
RUN CGO_ENABLED=0 GOOS=linux go build -o /mini-kafka-consumer ./cmd/consumer

FROM alpine:latest
RUN apk add --no-cache ca-certificates
WORKDIR /root/
COPY --from=builder /mini-kafka-broker .
COPY --from=builder /mini-kafka-producer .
COPY --from=builder /mini-kafka-consumer .
COPY config/broker.yaml /etc/mini-kafka/broker.yaml

EXPOSE 9092
CMD ["./mini-kafka-broker", "-config", "/etc/mini-kafka/broker.yaml"]
