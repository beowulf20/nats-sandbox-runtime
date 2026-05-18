GO ?= tmp/go/bin/go
AIR ?= $(GO) run github.com/air-verse/air@latest
NPM ?= npm
WEB_PORT ?= 3000
RUNTIME_API_LISTEN ?= 127.0.0.1:8080
RUNTIME_API_BUCKET ?= python-runtime-workspaces

.PHONY: build firecracker dev

build:
	$(GO) build -buildvcs=false -o bin/nats-sandbox-runtime ./cmd/nats-sandbox-runtime

dev:
	@command -v $(NPM) >/dev/null 2>&1 || { echo "npm is required for the web dev server"; exit 1; }
	@echo "runtime API: http://$(RUNTIME_API_LISTEN)"
	@echo "web UI:      http://127.0.0.1:$(WEB_PORT)"
	@RUNTIME_API_LISTEN="$(RUNTIME_API_LISTEN)" RUNTIME_API_BUCKET="$(RUNTIME_API_BUCKET)" $(AIR) & \
	api_pid=$$!; \
	(cd web && RUNTIME_API_PROXY="http://$(RUNTIME_API_LISTEN)" BROWSER=none PORT="$(WEB_PORT)" $(NPM) start) & \
	web_pid=$$!; \
	trap 'kill $$api_pid $$web_pid 2>/dev/null || true; wait $$api_pid $$web_pid 2>/dev/null || true' INT TERM EXIT; \
	wait $$api_pid $$web_pid

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
