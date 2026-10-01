#!/bin/sh
# Loads replica layout env injected by the pod-index init container, then
# starts the streamer with real environment variables set in-process.
if [ -f /etc/streamer/env ]; then
    . /etc/streamer/env
fi
exec /usr/local/bin/streamer