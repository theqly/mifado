FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/node ./cmd

FROM alpine:3.22

RUN addgroup -S app && adduser -S -G app app
WORKDIR /app

COPY --from=builder --chown=app:app /out/node /app/node
COPY --chown=app:app cluster_ini.txt /app/cluster_ini.txt

USER app
EXPOSE 8080
ENTRYPOINT ["/app/node"]