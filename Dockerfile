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
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -buildvcs=false -o /out/nats-sandbox-runtime ./cmd/nats-sandbox-runtime

FROM debian:bookworm-slim AS firecracker-assets
ARG TARGETARCH=amd64
ARG FIRECRACKER_VERSION=latest
ARG GUEST_ROOTFS_SIZE=2G

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates curl e2fsprogs grep squashfs-tools \
	&& rm -rf /var/lib/apt/lists/*

COPY scripts/fc-python-init.py /tmp/fc-python-init
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
	ci_version="$(printf '%s' "${version}" | sed -E 's/^([v]?[0-9]+\.[0-9]+)(\.[0-9]+)?$/\1/')"; \
	listing="http://spec.ccfc.min.s3.amazonaws.com"; \
	bucket="https://s3.amazonaws.com/spec.ccfc.min"; \
	kernel="$(curl -fsSL "${listing}/?prefix=firecracker-ci/${ci_version}/${fc_arch}/vmlinux-&list-type=2" | grep -oP "(?<=<Key>)(firecracker-ci/${ci_version}/${fc_arch}/vmlinux-[0-9]+\.[0-9]+\.[0-9]{1,3})(?=</Key>)" | sort -V | tail -1)"; \
	rootfs="$(curl -fsSL "${listing}/?prefix=firecracker-ci/${ci_version}/${fc_arch}/ubuntu-&list-type=2" | grep -oP "(?<=<Key>)(firecracker-ci/${ci_version}/${fc_arch}/ubuntu-[0-9]+\.[0-9]+\.squashfs)(?=</Key>)" | sort -V | tail -1)"; \
	if [ -z "${kernel}" ] || [ -z "${rootfs}" ]; then \
		echo "could not find Firecracker CI kernel/rootfs for ${ci_version}/${fc_arch}" >&2; \
		exit 1; \
	fi; \
	mkdir -p /out /tmp/rootfs; \
	curl -fsSL "${bucket}/${kernel}" -o /out/vmlinux.bin; \
	curl -fsSL "${bucket}/${rootfs}" -o /tmp/rootfs.squashfs; \
	unsquashfs -q -d /tmp/rootfs /tmp/rootfs.squashfs; \
	install -m 0755 /tmp/fc-python-init /tmp/rootfs/usr/local/bin/fc-python-init; \
	truncate -s "${GUEST_ROOTFS_SIZE}" /out/rootfs.ext4; \
	mkfs.ext4 -q -d /tmp/rootfs /out/rootfs.ext4; \
	rm -rf /tmp/rootfs /tmp/rootfs.squashfs

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

WORKDIR /opt/nats-sandbox-runtime
RUN mkdir -p firecracker-assets/python-snapshot web
COPY --from=go-build /out/nats-sandbox-runtime /usr/local/bin/nats-sandbox-runtime
COPY --from=web-build /src/web/build ./web/build
COPY --from=firecracker-assets /out/vmlinux.bin ./firecracker-assets/vmlinux.bin
COPY --from=firecracker-assets /out/rootfs.ext4 ./firecracker-assets/rootfs.ext4

COPY --chmod=0755 scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD []
