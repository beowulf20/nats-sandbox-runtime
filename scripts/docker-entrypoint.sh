#!/usr/bin/env sh
set -eu

binary="/usr/local/bin/nats-service-tests"
mode="${NATS_RUNTIME_MODE:-api}"

case "${1:-}" in
	nats-service-tests)
		shift
		exec "${binary}" "$@"
		;;
	runtime|local|help|--help|-h)
		exec "${binary}" "$@"
		;;
	api|python)
		mode="$1"
		shift
		;;
esac

common_args="
	--url ${NATS_URL:-nats://nats:4222}
	--bucket ${NATS_BUCKET:-python-runtime-workspaces}
	--workers ${NATS_RUNTIME_WORKERS:-1}
	--kernel ${NATS_RUNTIME_KERNEL:-/opt/nats-python-runtime/firecracker-assets/vmlinux.bin}
	--rootfs ${NATS_RUNTIME_ROOTFS:-/opt/nats-python-runtime/firecracker-assets/rootfs.ext4}
	--firecracker ${NATS_RUNTIME_FIRECRACKER:-/usr/local/bin/firecracker}
	--memory-mib ${NATS_RUNTIME_MEMORY_MIB:-128}
	--swap-mib ${NATS_RUNTIME_SWAP_MIB:-0}
	--workspace-mib ${NATS_RUNTIME_WORKSPACE_MIB:-16}
	--vcpus ${NATS_RUNTIME_VCPUS:-1}
	--max-vcpus ${NATS_RUNTIME_MAX_VCPUS:-1}
	--exec-timeout ${NATS_RUNTIME_EXEC_TIMEOUT:-5s}
	--truncate-log-mib ${NATS_RUNTIME_TRUNCATE_LOG_MIB:-1}
"

if [ "${mode}" = "api" ]; then
	# shellcheck disable=SC2086
	exec "${binary}" runtime api \
		--listen "${RUNTIME_API_LISTEN:-0.0.0.0:8080}" \
		--web-dir "${RUNTIME_API_WEB_DIR:-/opt/nats-python-runtime/web/build}" \
		${common_args} \
		"$@"
fi

if [ "${mode}" = "python" ]; then
	# shellcheck disable=SC2086
	exec "${binary}" runtime python ${common_args} "$@"
fi

echo "unsupported NATS_RUNTIME_MODE=${mode}; expected api or python" >&2
exit 2
