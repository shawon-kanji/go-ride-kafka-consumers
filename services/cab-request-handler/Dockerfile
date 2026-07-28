# syntax=docker/dockerfile:1

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

RUN --mount=type=ssh go build -trimpath -ldflags="-s -w" -o /out/cab-request-handler ./services/cab-request-handler/cmd/api

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/cab-request-handler /app

ENTRYPOINT ["/app"]
