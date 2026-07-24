FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /mini-kafka ./cmd/broker

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /mini-kafka .
COPY config/broker.yaml /etc/mini-kafka/broker.yaml

EXPOSE 9092
CMD ["./mini-kafka", "-config", "/etc/mini-kafka/broker.yaml"]
