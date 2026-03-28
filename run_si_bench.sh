#!/bin/bash
#
# Benchmark snapshot isolation with auto and zero conflict windows.
#
# Usage: run_si_bench.sh <tarantool_binary> [scale]
#
# Runs YCSB A, B, C with conflict_window=-1 (auto) and 0 (all slow path).

set -e

TARANTOOL="$1"
SCALE="${2:-10}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="/var/opt/bench"

if [ -z "$TARANTOOL" ]; then
    echo "Usage: $0 <tarantool_binary> [scale]"
    exit 1
fi

echo "Binary: $($TARANTOOL --version | head -1)"
echo "Scale: $SCALE"
echo ""

# Create a Lua snippet that adds SI metrics to any workload.
cat > /tmp/si_metrics_patch.lua << 'PATCH'
-- SI metrics reporter (injected by run_si_bench.sh).
local _si_reporter_started = false
if not _si_reporter_started then
    _si_reporter_started = true
    require('fiber').create(function()
        local log = require('log')
        local fiber = require('fiber')
        while true do
            fiber.sleep(10)
            local vs = box.stat.vinyl()
            local tx = vs.tx or {}
            log.info('bench: SI window=%d miss=%d conflict=%d',
                     tx.conflict_window or 0,
                     tx.conflict_window_miss or 0,
                     tx.conflict or 0)
            log.info('bench: memory tx=%d level0=%d',
                     vs.memory.tx or 0, vs.memory.level0 or 0)
        end
    end)
end
PATCH

for window in auto zero; do
    for w in a b c; do
        PREFIX="si_${window}_${w}"
        DIR="${BENCH_DIR}/${PREFIX}"
        SCRIPT="${SCRIPT_DIR}/vinyl_workload_${w}.lua"

        echo "=== Workload ${w^^}, window=${window} ==="
        rm -rf "$DIR"
        mkdir -p "$DIR"

        # Build patched script: original + SI metrics + window setting.
        PATCHED="${DIR}/workload.lua"
        cp "$SCRIPT" "$PATCHED"

        # Inject SI metrics fiber and window setting after load_data().
        # Find the line "load_data()" and append after it.
        if [ "$window" = "zero" ]; then
            sed -i '/^load_data()/a \
local _ffi = require("ffi")\
_ffi.cdef("void vinyl_set_conflict_window(int64_t w)")\
_ffi.C.vinyl_set_conflict_window(0)\
require("log").info("bench: conflict_window forced to 0")\
dofile("/tmp/si_metrics_patch.lua")' "$PATCHED"
        else
            sed -i '/^load_data()/a \
require("log").info("bench: conflict_window = auto")\
dofile("/tmp/si_metrics_patch.lua")' "$PATCHED"
        fi

        (cd "$DIR" && "$TARANTOOL" "$PATCHED" "" "$SCALE")
        echo ""
        echo "--- Results ---"
        grep -E '(OPS|TPS|Errors|bench: SI|bench: memory)' \
            "$DIR/picodata.log" | sed 's/.*I> //' | tail -30
        echo ""
    done
done

echo "=== All SI benchmark runs complete ==="
