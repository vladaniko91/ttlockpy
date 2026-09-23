#!/usr/bin/env python3
"""Repeatedly unlock then lock a TTLock, once per fixed-length cycle.

Runs indefinitely until interrupted (Ctrl+C). Each cycle opens a single
BLE connection, does unlock then lock on it, and disconnects — reusing
the connection for both operations avoids the reconnect cooldown the
lock needs right after finishing an operation.

Usage:
  ./venv/bin/python cycle_test.py --lock lock.json [--period 60] [--min-gap 5] [--log cycle_log.csv]
"""

import argparse
import asyncio
import csv
import sys
import time
from datetime import datetime, timezone

from ttlock import TTLock


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", default="lock.json", help="Path to lock credentials file")
    parser.add_argument("--period", type=float, default=60.0, help="Seconds between cycle starts")
    parser.add_argument("--min-gap", type=float, default=5.0,
                         help="Minimum seconds to wait after disconnecting before the next "
                              "cycle reconnects, even if the cycle already overran --period. "
                              "The lock needs a moment to recover after a BLE session; hammering "
                              "it with zero gap between back-to-back cycles is what actually "
                              "causes cascading failures under a short period.")
    parser.add_argument("--log", default="cycle_log.csv", help="CSV file to append cycle results to")
    return parser.parse_args()


def log_row(log_path: str, row: dict) -> None:
    is_new = False
    try:
        with open(log_path, "x", newline=""):
            is_new = True
    except FileExistsError:
        pass
    with open(log_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if is_new:
            writer.writeheader()
        writer.writerow(row)


async def run_cycle(lock: TTLock) -> dict:
    t0 = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        async with lock:
            await lock.unlock()
            await lock.lock()
        return {
            "started_at": started_at,
            "duration_s": round(time.monotonic() - t0, 1),
            "result": "success",
            "battery": lock.battery,
            "error": "",
        }
    except Exception as exc:
        message = str(exc) or f"{type(exc).__name__} (no further details)"
        return {
            "started_at": started_at,
            "duration_s": round(time.monotonic() - t0, 1),
            "result": "failure",
            "battery": "",
            "error": message,
        }


async def main() -> None:
    args = parse_args()
    lock = TTLock.from_file(args.lock)

    cycle_num = 0
    successes = 0
    failures = 0

    print(f"Cycling unlock/lock on {lock.data.address} every {args.period:.0f}s. "
          f"Logging to {args.log}. Ctrl+C to stop.")

    try:
        while True:
            cycle_num += 1
            cycle_start = time.monotonic()
            print(f"[cycle {cycle_num}] starting…", flush=True)

            result = await run_cycle(lock)
            result = {"cycle": cycle_num, **result}
            log_row(args.log, result)

            if result["result"] == "success":
                successes += 1
                print(f"[cycle {cycle_num}] OK in {result['duration_s']}s "
                      f"(battery {result['battery']}%)", flush=True)
                lock.save(args.lock)
            else:
                failures += 1
                print(f"[cycle {cycle_num}] FAILED in {result['duration_s']}s: "
                      f"{result['error']}", flush=True)

            elapsed = time.monotonic() - cycle_start
            remaining = args.period - elapsed
            gap = max(remaining, args.min_gap)
            await asyncio.sleep(gap)
    except (KeyboardInterrupt, asyncio.CancelledError):
        # asyncio.run() delivers Ctrl+C as a CancelledError into the running
        # task, not KeyboardInterrupt directly (Python 3.11+); it raises the
        # real KeyboardInterrupt afterward at the top level, past this point.
        print(f"\nStopped after {cycle_num} cycles: {successes} succeeded, {failures} failed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
