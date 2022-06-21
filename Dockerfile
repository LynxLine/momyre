FROM golang:1.15.11-alpine as dev

RUN apk add git tree

COPY main.go go.mod go.sum /momyre/
COPY app /momyre/app/
RUN tree /momyre

WORKDIR /momyre
RUN go get || echo end1
RUN go build

RUN ls -al

FROM alpine:3.12 as base

COPY --from=dev /momyre/momyre /usr/bin

RUN mkdir -p /momyre
WORKDIR /momyre
RUN momyre --help

ENTRYPOINT ["momyre"]

