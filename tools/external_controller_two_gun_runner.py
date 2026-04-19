#!/usr/bin/env python3

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
BATCH_SCRIPT = ROOT / "tools" / "external_controller_reset_batch_test.py"

DEFAULT_GUN_CONFIG = {
    "1": {
        "label": "gun1",
        "plc_id": 1,
        "home_addr": 1,
        "home_group": 1,
        "port": "/dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_B8:F8:62:4B:C5:08-if00",
    },
    "2": {
        "label": "gun2",
        "plc_id": 2,
        "home_addr": 2,
        "home_group": 2,
        "port": "/dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_B8:F8:62:4B:C5:20-if00",
    },
}


def ts() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def build_arg_parser() -> argparse.ArgumentParser:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(
        description="Run controller-mode batch tests against gun 1, gun 2, or both guns sequentially."
    )
    parser.add_argument("--guns", nargs="+", choices=["1", "2"], default=["1", "2"])
    parser.add_argument("--sessions", type=int, default=1)
    parser.add_argument("--gap-s", type=float, default=10.0)
    parser.add_argument("--current-demand-hold-s", type=float, default=300.0)
    parser.add_argument("--session-start-timeout-s", type=float, default=300.0)
    parser.add_argument("--child-timeout-s", type=float, default=0.0)
    parser.add_argument("--controller-id", type=int, default=7)
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument("--esp-reset-pulse-s", type=float, default=0.2)
    parser.add_argument("--esp-reset-wait-s", type=float, default=14.0)
    parser.add_argument(
        "--base-prefix",
        default=f"/home/jpi/Desktop/EVSE/plc_firmware/logs/external_controller_two_gun_{stamp}",
    )
    parser.add_argument("--port-gun1", default=DEFAULT_GUN_CONFIG["1"]["port"])
    parser.add_argument("--port-gun2", default=DEFAULT_GUN_CONFIG["2"]["port"])
    parser.add_argument("--extra-arg", action="append", default=[])
    return parser


def run_batch_for_gun(args: argparse.Namespace, gun: str) -> dict:
    cfg = dict(DEFAULT_GUN_CONFIG[gun])
    cfg["port"] = args.port_gun1 if gun == "1" else args.port_gun2
    gun_prefix = f"{args.base_prefix}_{cfg['label']}"
    cmd = [
        sys.executable,
        str(BATCH_SCRIPT),
        "--plc-port",
        str(cfg["port"]),
        "--baud",
        str(args.baud),
        "--plc-id",
        str(cfg["plc_id"]),
        "--controller-id",
        str(args.controller_id),
        "--sessions",
        str(args.sessions),
        "--gap-s",
        str(args.gap_s),
        "--current-demand-hold-s",
        str(args.current_demand_hold_s),
        "--session-start-timeout-s",
        str(args.session_start_timeout_s),
        "--esp-reset-pulse-s",
        str(args.esp_reset_pulse_s),
        "--esp-reset-wait-s",
        str(args.esp_reset_wait_s),
        "--base-prefix",
        gun_prefix,
        "--extra-arg",
        "--home-addr",
        "--extra-arg",
        str(cfg["home_addr"]),
        "--extra-arg",
        "--home-group",
        "--extra-arg",
        str(cfg["home_group"]),
    ]
    if args.child_timeout_s > 0.0:
        cmd.extend(["--child-timeout-s", str(args.child_timeout_s)])
    for extra in args.extra_arg:
        cmd.extend(["--extra-arg", extra])

    print(f"[RUN] {cfg['label']} start={ts()} port={cfg['port']} plc_id={cfg['plc_id']}", flush=True)
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    runner_out = Path(f"{gun_prefix}_runner.out")
    ensure_parent(runner_out)
    runner_out.write_text(proc.stdout + proc.stderr, encoding="utf-8")

    aggregate_json = Path(f"{gun_prefix}_aggregate.json")
    aggregate_payload = {}
    if aggregate_json.exists():
        try:
            aggregate_payload = json.loads(aggregate_json.read_text(encoding="utf-8"))
        except Exception:
            aggregate_payload = {}

    return {
        "gun": gun,
        "label": cfg["label"],
        "port": cfg["port"],
        "plc_id": cfg["plc_id"],
        "home_addr": cfg["home_addr"],
        "home_group": cfg["home_group"],
        "rc": proc.returncode,
        "runner_file": str(runner_out),
        "aggregate_json": str(aggregate_json),
        "passes": aggregate_payload.get("passes"),
        "fails": aggregate_payload.get("fails"),
    }


def main() -> int:
    args = build_arg_parser().parse_args()
    results = [run_batch_for_gun(args, gun) for gun in args.guns]
    aggregate = {
        "generated_at": ts(),
        "base_prefix": args.base_prefix,
        "controller_id": args.controller_id,
        "sessions_per_gun": args.sessions,
        "results": results,
    }
    summary_path = Path(f"{args.base_prefix}_aggregate.json")
    ensure_parent(summary_path)
    summary_path.write_text(json.dumps(aggregate, indent=2), encoding="utf-8")

    failed = [row for row in results if row["rc"] != 0 or (row.get("fails") not in (None, 0))]
    if failed:
        print(json.dumps(aggregate, indent=2), flush=True)
        return 1
    print(json.dumps(aggregate, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
