FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/price-alerter ./cmd/alerter

FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /bin/price-alerter /app/price-alerter
COPY migrations ./migrations

EXPOSE 8080 9090
ENTRYPOINT ["/app/price-alerter"]
