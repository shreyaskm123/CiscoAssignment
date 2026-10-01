# Build stage. Context = repo ROOT (needed because go.mod replaces resolve to
# sibling modules: streamer/proto, ../collector).
FROM golang:1.26-alpine AS build
WORKDIR /workspace
COPY streamer ./streamer
COPY collector ./collector
COPY messageQueue ./messageQueue
COPY apiGateway ./apiGateway
RUN cd /workspace/streamer \
    && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/streamer ./cmd/streamer

# Runtime stage
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/streamer /usr/local/bin/streamer
COPY streamer/scripts/entrypoint.sh /scripts/entrypoint.sh
RUN chmod +x /scripts/entrypoint.sh
# CSV is mounted here from the configMap.
VOLUME /mnt/data
ENTRYPOINT ["/bin/sh", "/scripts/entrypoint.sh"]