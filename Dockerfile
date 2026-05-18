# syntax=docker/dockerfile:1

ARG GO_VERSION=1.26.2
ARG NODE_VERSION=24-bookworm-slim

FROM node:${NODE_VERSION} AS web-build
WORKDIR /src/web
COPY web/package*.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM golang:${GO_VERSION}-bookworm AS go-build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -buildvcs=false -o /out/nats-service-tests ./cmd/nats-service-tests

FROM debian:bookworm-slim AS runtime
ARG TARGETARCH=amd64
ARG FIRECRACKER_VERSION=latest

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates curl e2fsprogs tar \
	&& rm -rf /var/lib/apt/lists/*

RUN set -eu; \
	case "${TARGETARCH}" in \
		amd64) fc_arch="x86_64" ;; \
		arm64) fc_arch="aarch64" ;; \
		*) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
	esac; \
	releases="https://github.com/firecracker-microvm/firecracker/releases"; \
	version="${FIRECRACKER_VERSION}"; \
	if [ "${version}" = "latest" ]; then \
		version="$(basename "$(curl -fsSLI -o /dev/null -w '%{url_effective}' "${releases}/latest")")"; \
	fi; \
	tmpdir="$(mktemp -d)"; \
	curl -fsSL "${releases}/download/${version}/firecracker-${version}-${fc_arch}.tgz" | tar -xz -C "${tmpdir}"; \
	install -m 0755 "${tmpdir}/release-${version}-${fc_arch}/firecracker-${version}-${fc_arch}" /usr/local/bin/firecracker; \
	rm -rf "${tmpdir}"

WORKDIR /opt/nats-python-runtime
RUN mkdir -p firecracker-assets/python-snapshot web
COPY --from=go-build /out/nats-service-tests /usr/local/bin/nats-service-tests
COPY --from=web-build /src/web/build ./web/build
COPY firecracker-assets/vmlinux-6.1.155 ./firecracker-assets/vmlinux-6.1.155
COPY firecracker-assets/ubuntu-24.04-python-data.ext4 ./firecracker-assets/ubuntu-24.04-python-data.ext4
RUN ln -s vmlinux-6.1.155 ./firecracker-assets/vmlinux.bin \
	&& ln -s ubuntu-24.04-python-data.ext4 ./firecracker-assets/rootfs.ext4

COPY --chmod=0755 scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD []
