FROM golang:1.20-alpine AS builder
RUN apk add alpine-sdk librdkafka-dev
WORKDIR /go/app
COPY . .
RUN GOOS=linux GOARCH=arm64 go build -tags musl -o databahn-jobs main.go

FROM alpine:latest as runner
WORKDIR /root/

RUN apk add librdkafka-dev

ENV SERVICE_NAME="databahn-jobs"

COPY --from=builder /go/app/databahn-jobs .
ENTRYPOINT /root/databahn-jobs