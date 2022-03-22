FROM golang:1.17-buster as builder

WORKDIR $GOPATH/src/github.com/karnott/pubsub-to-pubsub/
ADD . $GOPATH/src/github.com/karnott/pubsub-to-pubsub/

RUN go clean
RUN go mod vendor
RUN go build -o /pubsub-to-pubsub main.go

FROM alpine
WORKDIR /app
COPY --from=builder /usr/share/zoneinfo/ /usr/share/zoneinfo/
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /pubsub-to-pubsub /app/
