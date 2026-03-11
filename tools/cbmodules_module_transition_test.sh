#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JP_DIR="$ROOT_DIR/../jpmodules"
BUILD_DIR="$ROOT_DIR/.tools_build"
BIN="$BUILD_DIR/cbmodules_module_transition_test"
SRC="$ROOT_DIR/tools/cbmodules_module_transition_test.cpp"

mkdir -p "$BUILD_DIR"

g++ \
  -std=gnu++17 \
  -O2 \
  -Wall \
  -Wextra \
  -I"$JP_DIR/include" \
  "$SRC" \
  "$JP_DIR/src/module_manager.cpp" \
  -o "$BIN"

exec "$BIN" "$@"
