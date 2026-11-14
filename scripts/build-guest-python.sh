#!/bin/sh
# Runs inside the disposable Ubuntu guest compiler stage.
set -eu
: "${PYTHON_VERSION:?set PYTHON_VERSION}"
: "${PYTHON_SHA256:?set PYTHON_SHA256}"
case "$PYTHON_VERSION" in *[!0-9.]*|'') echo 'invalid Python version' >&2; exit 1;; esac
case "$PYTHON_SHA256" in *[!0-9a-f]*|'') echo 'invalid SHA-256' >&2; exit 1;; esac
[ "${#PYTHON_SHA256}" = 64 ]

mkdir -p /tmp/python-build
cd /tmp/python-build
curl --fail --silent --show-error --location \
 "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz" -o python.tar.xz
printf '%s  python.tar.xz\n' "$PYTHON_SHA256" | sha256sum -c -
tar -xf python.tar.xz
cd "Python-${PYTHON_VERSION}"
# Portable code generation: do not specialize the image for the build host CPU.
# These are C compiler options; Python assertions and docstrings stay enabled.
cflags='-O3 -fno-omit-frame-pointer -fstack-protector-strong -fstack-clash-protection'
case "$(uname -m)" in x86_64) cflags="$cflags -fcf-protection";; esac
./configure --prefix=/opt/python --with-ensurepip=install \
 --enable-optimizations --with-lto \
 CFLAGS="$cflags" \
 LDFLAGS='-Wl,-z,relro,-z,now'
make -j "${PYTHON_BUILD_JOBS:-4}"
make install
/opt/python/bin/python3 -c 'import ssl,sqlite3,ctypes,bz2,lzma,readline,dbm.gnu,decimal,zlib; assert __debug__'
