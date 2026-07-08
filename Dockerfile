FROM golang:1.25-alpine AS builder
RUN apk add alpine-sdk librdkafka-dev
WORKDIR /go/app
COPY . .
ENV CGO_ENABLED=1
# macOS vendors github.com/microsoft and github.com/Microsoft into one directory; Linux needs exact casing.
RUN set -eux; \
    mkdir -p vendor/github.com/microsoft; \
    if [ -d vendor/github.com/Microsoft/go-mssqldb ]; then mv vendor/github.com/Microsoft/go-mssqldb vendor/github.com/microsoft/; fi
RUN go build -tags musl -o databahn-jobs main.go

FROM alpine:3.20.3 AS runner
WORKDIR /home/databahn/service

ADD templates /home/databahn/templates

RUN apk add librdkafka-dev

ENV SERVICE_NAME="databahn-jobs"

COPY --from=builder /go/app/databahn-jobs .
COPY internal/datahealthscore/config.yaml /home/databahn/service/config.yaml

ENTRYPOINT ["/home/databahn/service/databahn-jobs"]
