#!/usr/bin/env bash
# Builds brotli-decoder.wasm, the module the decoder worker loads: brotli
# 1.2.0's reference decoder behind brotli-decoder.c's interface, compiled with
# zig's clang and wasi-libc.
#
#   build-brotli-decoder.sh <brotli-1.2.0.tar.gz> <scratch directory>
#
# The archive is github.com/google/brotli/archive/refs/tags/v1.2.0.tar.gz; the
# script refuses any other bytes. It is unpacked under the scratch directory,
# which must be outside the source tree (a `target/` directory is), and the
# module is written beside this script. The build is the committed module's
# when it ends by matching brotli-decoder.wasm.sha256; a module that differs
# fails here, and brotli-decoder-module.test.ts fails on it too. Changing the
# module means changing that record by hand, in the same commit.
#
# The committed module was built with zig 0.14.0.
set -euo pipefail

BROTLI_SHA256=816c96e8e8f193b40151dad7e8ff37b1221d019dbcb9c35cd3fadbfe6477dfec
ZIG_VERSION=0.14.0

archive=$1
scratch=$2
here=$(cd "$(dirname "$0")" && pwd)

actual=$(shasum -a 256 "$archive" | cut -d ' ' -f 1)
if [ "$actual" != "$BROTLI_SHA256" ]; then
  printf 'the archive is %s, not brotli 1.2.0 (%s)\n' "$actual" "$BROTLI_SHA256" >&2
  exit 1
fi
if [ "$(zig version)" != "$ZIG_VERSION" ]; then
  printf 'zig is %s; the committed module was built with %s\n' "$(zig version)" "$ZIG_VERSION" >&2
  exit 1
fi

mkdir -p "$scratch"
sources=$(mktemp -d "$scratch/brotli-decoder.XXXXXX")
tar -xzf "$archive" -C "$sources"
brotli="$sources/brotli-1.2.0"

# zig gives a WebAssembly module a 16 MiB stack unless told otherwise, which
# would be half as much again as the window of a 100k artifact. The decoder
# does not recurse; 128 KiB is many times what it uses, and zig puts the stack
# below the data, so running out of it traps.
zig cc -target wasm32-wasi -mexec-model=reactor -O3 -DNDEBUG \
  -Wl,--no-entry -Wl,--strip-all -Wl,-z,stack-size=131072 \
  "-I$brotli/c/include" \
  "$here/brotli-decoder.c" \
  "$brotli"/c/common/{constants,context,dictionary,platform,shared_dictionary,transform}.c \
  "$brotli"/c/dec/{bit_reader,decode,huffman,prefix,state,static_init}.c \
  -o "$here/brotli-decoder.wasm"
chmod 644 "$here/brotli-decoder.wasm"
rm -rf "$sources"

printf 'brotli-decoder.wasm %s bytes\n' "$(wc -c <"$here/brotli-decoder.wasm" | tr -d ' ')"
(cd "$here" && shasum -a 256 -c brotli-decoder.wasm.sha256)
