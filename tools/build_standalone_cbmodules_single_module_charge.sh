#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_BIN="$ROOT_DIR/tools/standalone_cbmodules_single_module_charge"

g++ -std=c++17 -O2 -Wall -Wextra -pthread \
  "$ROOT_DIR/tools/standalone_cbmodules_single_module_charge.cpp" \
  "$ROOT_DIR/../jpmodules/src/module_manager.cpp" \
  -I"$ROOT_DIR/../jpmodules/include" \
  -o "$OUT_BIN"

echo "$OUT_BIN"
