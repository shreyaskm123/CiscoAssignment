# Build stage. Context = repo ROOT (replace collector => ../collector, which in
# turn replaces streamer/proto => ../streamer/proto).
FROM golang:1.26-alpine AS build
WORKDIR /workspace
COPY streamer ./streamer
COPY collector ./collector
COPY messageQueue ./messageQueue
COPY apiGateway ./apiGateway
RUN cd /workspace/apiGateway \
    && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/api ./cmd/api

# Runtime stage
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/api /usr/local/bin/api
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/api"]