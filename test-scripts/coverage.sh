#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

llvm_cov() {
  if command -v xcrun >/dev/null 2>&1; then xcrun llvm-cov "$@"; else llvm-cov "$@"; fi
}

swift test --enable-code-coverage

root="$(swift build --build-tests --show-bin-path)"
bin="$(find "$root" -name indras-netPackageTests -type f ! -path '*/dSYM/*' -print -quit)"

llvm_cov report "$bin" --instr-profile="$root/codecov/default.profdata" Sources \
  --ignore-filename-regex='Tests/'
