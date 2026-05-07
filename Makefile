.PHONY: build firecracker

build:
	go build -buildvcs=false -o bin/nats-service-tests ./cmd/nats-service-tests

firecracker:
	mkdir -p bin
	ARCH="$$(uname -m)"; \
	RELEASE_URL="https://github.com/firecracker-microvm/firecracker/releases"; \
	LATEST="$$(basename "$$(curl -fsSLI -o /dev/null -w '%{url_effective}' "$$RELEASE_URL/latest")")"; \
	TMPDIR="$$(mktemp -d)"; \
	trap 'rm -rf "$$TMPDIR"' EXIT; \
	curl -fsSL "$$RELEASE_URL/download/$$LATEST/firecracker-$$LATEST-$$ARCH.tgz" | tar -xz -C "$$TMPDIR"; \
	cp "$$TMPDIR/release-$$LATEST-$$ARCH/firecracker-$$LATEST-$$ARCH" bin/firecracker; \
	chmod +x bin/firecracker; \
	bin/firecracker --version
