#!/usr/bin/env python3
"""Read benchmark result JSON files and print a median comparison table.

    python3 common/compare.py                       # reads results/*.json
    python3 common/compare.py results/matrix-20260822-1200

With more than one run per impl, the earliest run (by filename) is dropped as a cold-cache
warm-up, and the median of the rest is reported. Peak RSS is self-reported by each program.
"""
import json
import sys
from collections import defaultdict
from pathlib import Path
from statistics import median


def load_runs(results_dir):
    """Return impl -> [run_data, ...], ordered by filename so run1 sorts first."""
    results_dir = Path(results_dir)
    if not results_dir.exists():
        print(f"{results_dir}/ not found. Run the benchmark first.", file=sys.stderr)
        sys.exit(1)

    groups = defaultdict(list)
    for f in sorted(results_dir.glob("*.json")):
        try:
            with open(f) as fp:
                data = json.load(fp)
        except Exception as e:
            print(f"error reading {f}: {e}", file=sys.stderr)
            continue
        groups[data.get("impl", "unknown")].append(data)
    return groups


def timed_runs(data_list):
    """Drop the warm-up (first) run when there's more than one; keep the rest."""
    return data_list[1:] if len(data_list) > 1 else data_list


def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "results"
    groups = load_runs(results_dir)
    if not groups:
        print("no results found.", file=sys.stderr)
        sys.exit(1)

    print(f"\n=== Benchmark Results ({results_dir}) ===\n")
    header = (f"{'impl':<8} {'wall_ms':>9} {'rows/sec':>11} {'cpu_ms':>9} "
              f"{'cpu/wall':>9} {'rss_mb':>8} {'rows':>8} {'kept':>6}")
    print(header)
    print("-" * len(header))

    summaries = []
    for impl, data_list in groups.items():
        kept = timed_runs(data_list)
        excl = median(d["totals"]["wall_ms_excl_startup"] for d in kept)
        incl = median(d["totals"]["wall_ms_incl_startup"] for d in kept)
        rss = median(d.get("resources", {}).get("peak_rss_mb", 0) for d in kept)
        # CPU is user+sys; the ratio is against the whole-process clock, since CPU counts startup too.
        cpu = median(d.get("resources", {}).get("user_cpu_ms", 0)
                     + d.get("resources", {}).get("sys_cpu_ms", 0) for d in kept)
        rows = kept[0]["totals"]["rows"]
        # Throughput off the work clock, so startup doesn't dilute it. Guard the divide-by-zero
        # a sub-millisecond run would cause.
        rows_per_sec = rows / (excl / 1000.0) if excl > 0 else 0
        cpu_per_wall = cpu / incl if incl > 0 else 0
        summaries.append((impl, incl, rows_per_sec, cpu, cpu_per_wall, rss, rows,
                          len(kept), len(data_list)))

    # Fastest wall time first.
    summaries.sort(key=lambda s: s[1])
    for impl, wall, rps, cpu, cpw, rss, rows, kept, total in summaries:
        print(f"{impl:<8} {wall:>9.0f} {rps:>11,.0f} {cpu:>9.0f} {cpw:>9.2f} "
              f"{rss:>8.1f} {rows:>8} {f'{kept}/{total}':>6}")

    print("\nmedian of kept runs (warm-up dropped when >1). wall_ms = incl_startup.")
    print("rows/sec = rows / work-clock. cpu_ms = user+sys. cpu/wall ~1 = one core pinned,")
    print("<1 = mostly waiting (I/O or GC stalls), >1 = using multiple cores.")
    print("all resources self-reported: getrusage on go/rust, /proc on java.\n")


if __name__ == "__main__":
    main()
