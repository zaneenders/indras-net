#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

llvm_cov() {
  if command -v xcrun >/dev/null 2>&1; then xcrun llvm-cov "$@"; else llvm-cov "$@"; fi
}

swift test --enable-code-coverage || true

PROFDATA=$(find .build -name '*.profdata' -print -quit)

BINS=()
while IFS= read -r bin; do
  BINS+=("$bin")
done < <(
  find .build \( \
    -path '*/Products/Debug/*.xctest/Contents/MacOS/*' -o \
    -name 'indras-netPackageTests' -o \
    -name 'indras_netPackageTests' -o \
    -name 'IndrasNetTests.so' \
    \) -type f ! -path '*/dSYM/*' | sort -u
)

if [[ -z "$PROFDATA" || ${#BINS[@]} -eq 0 ]]; then
  echo "error: coverage data not found under .build" >&2
  exit 1
fi

llvm_cov report "${BINS[@]}" --instr-profile="$PROFDATA" \
  --ignore-filename-regex='(\.build/|Tests/)'
