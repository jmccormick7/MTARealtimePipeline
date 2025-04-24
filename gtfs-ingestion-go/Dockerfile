FROM golang:1.24.2 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download -x

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o gtfs-producer .

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/gtfs-producer .

ENTRYPOINT ["./gtfs-producer"]
