FROM golang:1.24-alpine AS builder
RUN apk add alpine-sdk librdkafka-dev
WORKDIR /go/app
COPY . .
ENV CGO_ENABLED=1
RUN go build -tags musl -o databahn-jobs main.go

FROM alpine:3.20.3 as runner
WORKDIR /home/databahn/service

RUN apk add librdkafka-dev

ENV SERVICE_NAME="databahn-jobs"

COPY --from=builder /go/app/databahn-jobs .
COPY internal/datahealthscore/config.yaml /home/databahn/service/config.yaml

ENTRYPOINT /home/databahn/service/databahn-jobs
