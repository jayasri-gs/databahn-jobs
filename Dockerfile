# Databahn vetted base images (databahn-ai/container-base-images).
# CGO + librdkafka — replaces golang:1.24-alpine + alpine:3.20.3.

FROM databahn/builder-golang1.25:1 AS build

ENV CGO_ENABLED=1
RUN apk add --no-cache gcc musl-dev librdkafka-dev

WORKDIR /app
COPY go.mod go.sum ./
COPY . .

RUN go build -mod=vendor -tags musl -trimpath -ldflags="-s -w" -o databahn-jobs .

FROM databahn/runner-golang:1

USER root
WORKDIR /home/databahn/service

RUN apk add --no-cache curl librdkafka

ENV SERVICE_NAME=databahn-jobs

COPY --chown=databahn:databahn templates /home/databahn/templates
COPY --chown=databahn:databahn internal/datahealthscore/config.yaml /home/databahn/service/config.yaml
COPY --from=build --chown=databahn:databahn /app/databahn-jobs ./databahn-jobs

RUN chmod +x ./databahn-jobs

USER databahn

# Batch / job runner — no HTTP server; PID 1 is the job process.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD ["sh", "-c", "kill -0 1"]

ENTRYPOINT ["/home/databahn/service/databahn-jobs"]
