#!/usr/bin/env python3
"""
Run Raft tests N times each and report results.

Usage: ./run_tests.py [OPTIONS] [PARTS...]

  PARTS          Test parts to run (default: 3A 3B 3C 3D)
                 e.g. 3A, 3B, 3C, 3D, or any combination
  -n N           Number of runs per part (default: 10)
  -p, --parallel Run all repetitions in parallel (default: sequential)
  -j JOBS        Max parallel jobs when --parallel is used (default: CPU count)
  -h, --help     Show this help message

Examples:
  ./run_tests.py                     # run 3A 3B 3C 3D, 10 times each
  ./run_tests.py 3A 3C               # run only 3A and 3C
  ./run_tests.py -n 5 3B 3D          # run 3B and 3D, 5 times each
  ./run_tests.py -p -j 4 3C          # run 3C 10 times, 4 in parallel
"""

import argparse
import os
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

# ANSI color codes
RED    = "\033[31m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
CYAN   = "\033[36m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

USE_COLOR = sys.stdout.isatty()

def color(text: str, *codes: str) -> str:
    if not USE_COLOR:
        return text
    return "".join(codes) + text + RESET


@dataclass
class RunResult:
    part: str
    run: int
    passed: bool
    log_file: Optional[str] = None


def run_once(part: str, run: int, total: int) -> RunResult:
    """Run 'go test -race -run <part>' once, return result."""
    log_fd, log_path = tempfile.mkstemp(prefix=f"raft_{part}_", suffix=".log")
    try:
        with os.fdopen(log_fd, "w") as log_f:
            result = subprocess.run(
                ["go", "test", "-v", "-race", "-run", part, "."],
                stdout=log_f,
                stderr=subprocess.STDOUT,
                cwd=os.path.dirname(os.path.abspath(__file__)),
            )
        passed = result.returncode == 0
        return RunResult(part=part, run=run, passed=passed, log_file=log_path)
    except Exception:
        try:
            os.unlink(log_path)
        except OSError:
            pass
        raise


def run_part_sequential(part: str, runs: int) -> list[RunResult]:
    results = []
    print(color(f"{'='*44}", CYAN))
    print(color(f"  Running {part} tests ({runs} times)", BOLD))
    print(color(f"{'='*44}", CYAN))
    for i in range(1, runs + 1):
        print(f"  [{color(part, BOLD)}] Run {i:2d}/{runs} ... ", end="", flush=True)
        r = run_once(part, i, runs)
        if r.passed:
            print(color("PASS", GREEN, BOLD))
        else:
            print(color(f"FAIL  (log: {r.log_file})", RED, BOLD))
        results.append(r)
    print()
    return results


def run_part_parallel(part: str, runs: int, max_workers: int) -> list[RunResult]:
    results: list[RunResult] = [None] * runs  # type: ignore
    print(color(f"{'='*44}", CYAN))
    print(color(f"  Running {part} tests ({runs} times, parallel j={max_workers})", BOLD))
    print(color(f"{'='*44}", CYAN))

    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(run_once, part, i, runs): i for i in range(1, runs + 1)}
        for future in as_completed(futures):
            r = future.result()
            results[r.run - 1] = r
            completed += 1
            status = color("PASS", GREEN, BOLD) if r.passed else color(f"FAIL  (log: {r.log_file})", RED, BOLD)
            print(f"  [{color(part, BOLD)}] Run {r.run:2d}/{runs} ... {status}")

    print()
    return results


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "parts",
        nargs="*",
        choices=["3A", "3B", "3C", "3D"],
        metavar="PART",
        help="Test parts to run (3A, 3B, 3C, 3D). Default: all four.",
    )
    parser.add_argument(
        "-n",
        type=int,
        default=10,
        metavar="N",
        dest="runs",
        help="Number of runs per part (default: 10)",
    )
    parser.add_argument(
        "-p", "--parallel",
        action="store_true",
        help="Run repetitions in parallel",
    )
    cpu_count = os.cpu_count() or 1
    parser.add_argument(
        "-j",
        type=int,
        default=cpu_count,
        metavar="JOBS",
        dest="jobs",
        help=f"Max parallel jobs, capped at CPU count (default: {cpu_count})",
    )
    args = parser.parse_args()

    parts: list[str] = args.parts if args.parts else ["3A", "3B", "3C", "3D"]
    runs: int = args.runs
    jobs: int = min(args.jobs, cpu_count, runs)

    all_results: list[RunResult] = []
    for part in parts:
        if args.parallel:
            results = run_part_parallel(part, runs, jobs)
        else:
            results = run_part_sequential(part, runs)
        all_results.extend(results)

    # Summary
    print(color(f"{'='*44}", CYAN))
    print(color("  Summary", BOLD))
    print(color(f"{'='*44}", CYAN))

    all_ok = True
    for part in parts:
        part_results = [r for r in all_results if r.part == part]
        passed = sum(1 for r in part_results if r.passed)
        failed = runs - passed
        if failed:
            failed_runs = [str(r.run) for r in part_results if not r.passed]
            print(f"  {color(part, BOLD)}: {color(f'{passed}/{runs} passed', RED)}, "
                  f"{failed} FAILED (runs: {', '.join(failed_runs)})")
            all_ok = False
        else:
            print(f"  {color(part, BOLD)}: {color(f'{passed}/{runs} passed', GREEN)}")

    total = len(all_results)
    total_passed = sum(1 for r in all_results if r.passed)
    total_failed = total - total_passed
    print()
    print(f"  Total: {color(str(total_passed), GREEN)} passed, "
          f"{color(str(total_failed), RED if total_failed else GREEN)} failed "
          f"out of {total} runs")

    if all_ok:
        print()
        print(color("  All tests passed!", GREEN, BOLD))
        return 0
    else:
        print()
        print(color("  Some tests FAILED. Check the log files above for details.", RED, BOLD))
        return 1


if __name__ == "__main__":
    sys.exit(main())
