FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tls-egress .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 egress
COPY --from=build /tls-egress /usr/local/bin/tls-egress
USER 10001
ENTRYPOINT ["/usr/local/bin/tls-egress"]
