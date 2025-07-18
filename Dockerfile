FROM golang:1.24.5-alpine3.22 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /app/carijo ./main.go

FROM alpine:3.22
WORKDIR /app
COPY --from=builder /app/carijo /usr/local/bin/carijo
CMD ["carijo"]
