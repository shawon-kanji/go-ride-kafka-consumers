# syntax=docker/dockerfile:1
#
# Legacy worker — currently wired to a no-op Kafka consumer stub (see
# internal/bootstrap). Not deployed by any environment's Helm charts today;
# this Dockerfile exists so the image can be built on demand if the service
# is ever resurrected.

FROM golang:1.25-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends git openssh-client ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p -m 0700 /root/.ssh \
    && ssh-keyscan github.com >> /root/.ssh/known_hosts \
    && git config --global url."git@github.com:".insteadOf "https://github.com/"

ENV GOPRIVATE=github.com/shawon-kanji/*
ENV CGO_ENABLED=0
ENV GOOS=linux
# Root module is intentionally excluded from go.work (see repo CLAUDE.md) —
# must build with GOWORK=off or `go build` here will try to resolve via the
# workspace instead of this module's own go.mod.
ENV GOWORK=off

WORKDIR /src
COPY . .

RUN --mount=type=ssh go build -trimpath -ldflags="-s -w" -o /out/driver-location-worker ./cmd/driver-location-worker

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/driver-location-worker /app

ENTRYPOINT ["/app"]
