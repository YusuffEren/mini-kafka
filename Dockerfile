FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build all binaries
RUN CGO_ENABLED=0 GOOS=linux go build -o /mini-kafka-broker ./cmd/broker && \
    CGO_ENABLED=0 GOOS=linux go build -o /mini-kafka-producer ./cmd/producer && \
    CGO_ENABLED=0 GOOS=linux go build -o /mini-kafka-consumer ./cmd/consumer

FROM alpine:latest
RUN apk add --no-cache ca-certificates
RUN adduser -D -u 10001 minikafka
RUN mkdir -p /var/lib/mini-kafka && chown minikafka /var/lib/mini-kafka
WORKDIR /app
COPY --from=builder /mini-kafka-broker .
COPY --from=builder /mini-kafka-producer .
COPY --from=builder /mini-kafka-consumer .
COPY config/broker-single.yaml /etc/mini-kafka/broker-single.yaml
USER minikafka

EXPOSE 9092
CMD ["./mini-kafka-broker", "-config", "/etc/mini-kafka/broker-single.yaml"]