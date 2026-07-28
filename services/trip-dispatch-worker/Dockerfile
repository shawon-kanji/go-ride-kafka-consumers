# syntax=docker/dockerfile:1
#
# Builds both binaries this module produces. Only `consumer` is deployed as
# a workload today — `api` mode is currently a no-op stub (see
# internal/bootstrap) — but both ship in the image so flipping the chart's
# api.enabled toggle on later needs no image rebuild.

FROM golang:1.25-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends git openssh-client ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p -m 0700 /root/.ssh \
    && ssh-keyscan github.com >> /root/.ssh/known_hosts \
    && git config --global url."git@github.com:".insteadOf "https://github.com/"

ENV GOPRIVATE=github.com/shawon-kanji/*
ENV CGO_ENABLED=0
ENV GOOS=linux

WORKDIR /src
COPY . .

RUN --mount=type=ssh go build -trimpath -ldflags="-s -w" -o /out/consumer ./services/trip-dispatch-worker/cmd/consumer
RUN --mount=type=ssh go build -trimpath -ldflags="-s -w" -o /out/api ./services/trip-dispatch-worker/cmd/api

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/consumer /app/consumer
COPY --from=builder /out/api /app/api

ENTRYPOINT ["/app/consumer"]
