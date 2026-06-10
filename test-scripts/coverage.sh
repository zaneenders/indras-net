#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

llvm_cov() {
  if command -v xcrun >/dev/null 2>&1; then xcrun llvm-cov "$@"; else llvm-cov "$@"; fi
}

swift test --enable-code-coverage || true

PROFDATA=$(find .build -name '*.profdata' -print -quit)
BIN=$(find .build \( \
  -name 'indras-netPackageTests' -o \
  -name 'indras_netPackageTests' -o \
  -name 'indras-netPackageTests.xctest' -o \
  -name 'indras_netPackageTests.xctest' -o \
  -name 'IndrasNetTests.so' \
  \) -type f ! -path '*/dSYM/*' -print -quit)

if [[ -z "$PROFDATA" || -z "$BIN" ]]; then
  echo "error: coverage data not found under .build" >&2
  exit 1
fi

llvm_cov report "$BIN" --instr-profile="$PROFDATA" \
  --ignore-filename-regex='(\.build/|Tests/)'
