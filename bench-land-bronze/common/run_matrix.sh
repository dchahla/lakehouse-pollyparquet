#!/usr/bin/env bash
# Run every implementation N times against the same seeded data, all in Docker so the environment
# is identical. Each run writes its own result JSON; compare.py takes the median and discards the
# warm-up run. Peak RSS is self-reported by each program (getrusage / VmHWM), so no host-side
# timing wrapper is needed.
#
#   ./common/run_matrix.sh                       # all impls, default sources, 5 runs
#   RUNS=10 SOURCES=crm_customers ./common/run_matrix.sh
#
# Run from the bench-land-bronze/ directory. Assumes `make seed` has been run in the parent repo.
set -euo pipefail

RUNS="${RUNS:-5}"
SOURCES="${SOURCES:-crm_customers,web_events}"
IMPLS="${IMPLS:-java go rust}"

cd "$(dirname "$0")/.."           # bench-land-bronze/
ROOT="$(pwd)"

# One results dir per matrix run, timestamped, so old numbers aren't overwritten or mixed in.
STAMP="$(date +%Y%m%d-%H%M%S)"
OUTDIR="results/matrix-$STAMP"
mkdir -p "$OUTDIR"

echo "matrix run $STAMP: $RUNS runs each of [$IMPLS] over sources=$SOURCES"
echo "results -> $OUTDIR/"
echo

for impl in $IMPLS; do
    image="land-bronze-$impl"
    echo "=== $impl: building $image ==="
    docker build -f "docker/Dockerfile.$impl" -t "$image" . >/dev/null

    for run in $(seq 1 "$RUNS"); do
        # Run 1 is the warm-up (cold caches); compare.py drops the first per impl. We keep it on
        # disk anyway so the discard is visible rather than hidden.
        out="$OUTDIR/${impl}-run${run}.json"
        echo "--- $impl run $run/$RUNS ---"
        docker run --rm \
            -v "$ROOT/../:/work" \
            "$image" \
            --bronze-dir /work/data/bronze \
            --sources "$SOURCES" \
            --out "/work/bench-land-bronze/$out"
    done
    echo
done

echo "done. compare with:"
echo "  python3 common/compare.py $OUTDIR"
