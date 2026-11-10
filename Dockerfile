FROM golang:1.23-alpine AS build

WORKDIR /src
RUN apk add --no-cache ca-certificates
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/publisher ./cmd/publisher \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/consumer ./cmd/consumer \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/analytics-consumer ./cmd/analytics-consumer \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=build /out/ ./
COPY migrations/ ./migrations/
EXPOSE 8080
