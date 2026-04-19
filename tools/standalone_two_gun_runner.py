#!/usr/bin/env python3

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
MONITOR_SCRIPT = ROOT / "tools" / "standalone_plc_live_monitor.py"

DEFAULT_GUN_CONFIG = {
    "1": {
        "label": "gun1",
        "port": "/dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_B8:F8:62:4B:C5:08-if00",
    },
    "2": {
        "label": "gun2",
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
        description="Run standalone PLC live-monitor sessions against gun 1, gun 2, or both guns sequentially."
    )
    parser.add_argument("--guns", nargs="+", choices=["1", "2"], default=["1", "2"])
    parser.add_argument("--sessions", type=int, default=1)
    parser.add_argument("--gap-s", type=float, default=10.0)
    parser.add_argument("--hold-s", type=float, default=300.0)
    parser.add_argument("--start-timeout-s", type=float, default=180.0)
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument("--esp-reset-before-start", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--esp-reset-pulse-s", type=float, default=0.2)
    parser.add_argument("--esp-reset-wait-s", type=float, default=14.0)
    parser.add_argument(
        "--base-prefix",
        default=f"/home/jpi/Desktop/EVSE/plc_firmware/logs/standalone_two_gun_{stamp}",
    )
    parser.add_argument("--port-gun1", default=DEFAULT_GUN_CONFIG["1"]["port"])
    parser.add_argument("--port-gun2", default=DEFAULT_GUN_CONFIG["2"]["port"])
    parser.add_argument("--extra-arg", action="append", default=[])
    return parser


def build_monitor_cmd(args: argparse.Namespace, gun_cfg: dict, session_prefix: str) -> list[str]:
    log_file = f"{session_prefix}.log"
    summary_file = f"{session_prefix}_summary.json"
    cmd = [
        sys.executable,
        str(MONITOR_SCRIPT),
        "--port",
        str(gun_cfg["port"]),
        "--baud",
        str(args.baud),
        "--hold-s",
        str(args.hold_s),
        "--start-timeout-s",
        str(args.start_timeout_s),
        "--esp-reset-pulse-s",
        str(args.esp_reset_pulse_s),
        "--esp-reset-wait-s",
        str(args.esp_reset_wait_s),
        "--log-file",
        log_file,
        "--summary-file",
        summary_file,
    ]
    if args.esp_reset_before_start:
        cmd.append("--esp-reset-before-start")
    for extra in args.extra_arg:
        cmd.append(extra)
    return cmd


def run_session(cmd: list[str], runner_out: Path) -> int:
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    ensure_parent(runner_out)
    runner_out.write_text(proc.stdout + proc.stderr, encoding="utf-8")
    return proc.returncode


def run_batch_for_gun(args: argparse.Namespace, gun: str) -> dict:
    cfg = dict(DEFAULT_GUN_CONFIG[gun])
    cfg["port"] = args.port_gun1 if gun == "1" else args.port_gun2
    results = []
    passes = 0
    fails = 0
    for session_idx in range(1, args.sessions + 1):
        session_prefix = f"{args.base_prefix}_{cfg['label']}_session{session_idx:02d}"
        cmd = build_monitor_cmd(args, cfg, session_prefix)
        runner_out = Path(f"{session_prefix}_runner.out")
        print(
            f"[RUN] {cfg['label']} session={session_idx}/{args.sessions} start={ts()} port={cfg['port']}",
            flush=True,
        )
        rc = run_session(cmd, runner_out)
        summary_path = Path(f"{session_prefix}_summary.json")
        payload = {}
        if summary_path.exists():
            try:
                payload = json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
        if rc == 0 and payload.get("result") == "pass":
            passes += 1
        else:
            fails += 1
        results.append(
            {
                "session": session_idx,
                "rc": rc,
                "runner_file": str(runner_out),
                "summary_file": str(summary_path),
                "result": payload.get("result"),
                "diagnosis": payload.get("diagnosis"),
                "failure": payload.get("failure"),
            }
        )
        if session_idx < args.sessions and args.gap_s > 0.0:
            time.sleep(args.gap_s)

    return {
        "gun": gun,
        "label": cfg["label"],
        "port": cfg["port"],
        "passes": passes,
        "fails": fails,
        "results": results,
    }


def main() -> int:
    args = build_arg_parser().parse_args()
    results = [run_batch_for_gun(args, gun) for gun in args.guns]
    aggregate = {
        "generated_at": ts(),
        "base_prefix": args.base_prefix,
        "sessions_per_gun": args.sessions,
        "hold_s": args.hold_s,
        "results": results,
    }
    summary_path = Path(f"{args.base_prefix}_aggregate.json")
    ensure_parent(summary_path)
    summary_path.write_text(json.dumps(aggregate, indent=2), encoding="utf-8")
    print(json.dumps(aggregate, indent=2), flush=True)
    return 1 if any(row["fails"] > 0 for row in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
