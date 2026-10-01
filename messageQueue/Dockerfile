# Build stage. Context = repo ROOT (replace streamer/proto => ../streamer/proto).
FROM golang:1.26-alpine AS build
WORKDIR /workspace
COPY streamer ./streamer
COPY collector ./collector
COPY messageQueue ./messageQueue
COPY apiGateway ./apiGateway
RUN cd /workspace/messageQueue \
    && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/messagequeue ./cmd/messagequeue

# Runtime stage
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/messagequeue /usr/local/bin/messagequeue
EXPOSE 50051
ENTRYPOINT ["/usr/local/bin/messagequeue"]