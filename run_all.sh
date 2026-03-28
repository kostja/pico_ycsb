#!/bin/bash
#
# Run all three vinyl workloads and extract final reports.
#
# Usage: run_all.sh <tarantool_binary> <prefix> [scale]
#   e.g.: run_all.sh /path/to/src/tarantool baseline
#          run_all.sh /path/to/src/tarantool baseline_s100 100
#
# Results are stored in /var/opt/bench/<prefix>_{a,b,c}/

set -e

TARANTOOL="$1"
PREFIX="$2"
SCALE="${3:-10}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="/var/opt/bench"

if [ -z "$TARANTOOL" ] || [ -z "$PREFIX" ]; then
    echo "Usage: $0 <tarantool_binary> <prefix> [scale]"
    exit 1
fi

if [ ! -x "$TARANTOOL" ]; then
    echo "Error: $TARANTOOL is not executable"
    exit 1
fi

echo "Binary: $($TARANTOOL --version | head -1)"
echo "Prefix: $PREFIX"
echo "Scale: $SCALE"
echo "VINYL_CACHE: ${VINYL_CACHE:-0}"
echo ""

for w in a b c; do
    DIR="${BENCH_DIR}/${PREFIX}_${w}"
    SCRIPT="${SCRIPT_DIR}/vinyl_workload_${w}.lua"
    echo "=== Workload ${w^^} ==="
    rm -rf "$DIR"
    mkdir -p "$DIR"
    (cd "$DIR" && "$TARANTOOL" "$SCRIPT" "" "$SCALE")
    echo ""
    echo "--- Results ---"
    grep -E '(OPS|TPS|Errors|Ranges|Runs:|Write amp|Space amp|Read amp|Compaction I/O|Compaction tasks)' \
        "$DIR/picodata.log" | sed 's/.*I> //'
    echo ""
done

echo "=== All workloads complete ==="
