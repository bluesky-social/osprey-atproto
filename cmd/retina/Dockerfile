# Run this dockerfile from the top level of the tango git repository like:
#
#   podman build -f ./cmd/hepa/Dockerfile -t retina .

### Compile stage
FROM golang:1.25-bookworm AS build

ENV DEBIAN_FRONTEND="noninteractive"
ENV TZ="Etc/UTC"
ENV GODEBUG="netdns=go"
ENV GOOS="linux"
ENV GOARCH="amd64"
ENV CGO_ENABLED="1"
ENV GOEXPERIMENT="loopvar"

WORKDIR /usr/src/retina

COPY go.mod go.sum ./

RUN go mod download && \
  go mod verify

COPY . .

RUN GIT_VERSION=$(git describe --tags --long --always) && \
    go build \
        -v \
        -trimpath \
        -tags timetzdata \
        -o /retina-linux-amd64 \
        ./cmd/retina

FROM debian:bookworm-slim AS pdq-build

ENV DEBIAN_FRONTEND="noninteractive"
ENV TZ="Etc/UTC"

WORKDIR /usr/src/pdq

# Grab Git and Imagemagick for building PDQ Photo Hasher
RUN apt-get update && apt-get install --yes \
  git \
  build-essential \
  imagemagick

# Clone https://github.com/facebook/ThreatExchange
RUN git clone --depth 1 https://github.com/facebook/ThreatExchange

WORKDIR /usr/src/pdq/ThreatExchange/pdq/cpp

# Build the PDQ Photo Hasher
RUN make pdq-photo-hasher

FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND="noninteractive"
ENV TZ="Etc/UTC"
ENV GODEBUG="netdns=go"

RUN apt-get update && apt-get install --yes \
  dumb-init \
  ca-certificates \
  curl wget strace iproute2 net-tools dnsutils netcat-openbsd traceroute mtr iputils-ping \
  runit \
  imagemagick \
  tesseract-ocr

WORKDIR /retina-linux-amd64
COPY --from=build /retina-linux-amd64 /usr/bin/retina
COPY --from=pdq-build /usr/src/pdq/ThreatExchange/pdq/cpp/pdq-photo-hasher /usr/bin/pdq-photo-hasher

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/usr/bin/retina"]

LABEL org.opencontainers.image.source=https://github.com/bluesky-social/retina-linux-amd64
LABEL org.opencontainers.image.description="bsky.app retina"
LABEL org.opencontainers.image.licenses=MIT
