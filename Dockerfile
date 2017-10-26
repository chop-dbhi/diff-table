FROM golang:1.9 AS build-env
WORKDIR /go/src/github.com/chop-dbhi/diff-table
RUN make dist-build-linux

FROM alpine:3.6
RUN apk add --update ca-certificates
COPY --from=build-env /go/src/github.com/chop-dbhi/diff-table/dist/linux-amd64/diff-table /
ENTRYPOINT ["/diff-table"]
