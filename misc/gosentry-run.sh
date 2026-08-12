#!/usr/bin/env bash
# Build and run erigon under the gosentry toolchain, which panics on integer
# overflow/underflow at runtime.  https://github.com/trailofbits/gosentry
#
# Two kinds of deliberate wraparound have to be silenced or the node cannot
# start:
#
#   * whole packages whose purpose is modular arithmetic (below) — exempted
#     here, because marking every operation would mean 40 markers in murmur3
#     alone;
#   * single sites inside packages worth instrumenting — marked in the source
#     with `// overflow_false_positive`, so the rest of the package keeps its
#     checks.
#
# Anything that panics after that is a finding.
set -euo pipefail

GOSENTRY=${GOSENTRY:-$HOME/fzz/gosentry}
OUT=${OUT:-./build/bin/erigon-gosentry}
P=github.com/erigontech/erigon

# entire package is modular arithmetic by design
EXEMPT=(
	"$P/common/murmur3"                     # MurmurHash3 mixing
	"$P/common/crypto/blake2b"              # BLAKE2b compression
	"$P/common/bitutil"                     # SWAR select/popcount
	"$P/db/seg/patricia"                    # SWAR edge lookup
	"$P/db/recsplit"                        # RecSplit remix
	"$P/execution/protocol/rules/ethash"    # FNV
)

flags=()
for pkg in "${EXEMPT[@]}"; do
	flags+=("-gcflags=$pkg=-overflowdetect=false")
done

export GOTOOLCHAIN=local CGO_ENABLED=1

case "${1:-build}" in
build)
	# -truncationdetect=true is deliberately NOT set: it fires within the first
	# few executions of nearly every package, so it needs its own triage pass.
	exec "$GOSENTRY/bin/go" build "${flags[@]}" -o "$OUT" ./cmd/erigon
	;;
test)
	shift
	exec "$GOSENTRY/bin/go" test "${flags[@]}" -short -count=1 "${@:-./...}"
	;;
fuzz)
	# the LibAFL harness is linked by cargo, which does not pull in the C++
	# runtime that evmone's modexp needs
	export RUSTFLAGS="-C link-arg=-lstdc++"
	pkg=$2 target=$3 dur=${4:-60s}
	exec "$GOSENTRY/bin/go" test "$pkg" -run='^$' -fuzz="^$target\$" -fuzztime="$dur" \
		--focus-on-new-code=false --catch-races=false --catch-leaks=false "${flags[@]}"
	;;
*)
	echo "usage: $0 {build|test [pkgs...]|fuzz <pkg> <FuzzTarget> [dur]}" >&2
	exit 2
	;;
esac
