#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path
import subprocess
import sys
import time

try:
    import serial  # type: ignore
except Exception as exc:
    print(f"[ERR] pyserial import failed: {exc}", file=sys.stderr)
    sys.exit(2)


DEFAULT_RESET_PULSE_S = 0.2
DEFAULT_RESET_WAIT_S = 14.0
ROOT = Path(__file__).resolve().parents[1]
SINGLE_SESSION_SCRIPT = ROOT / "tools" / "external_controller_single_module_charge_test.py"


def ts() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def ensure_parent(path: str) -> None:
    parent = Path(path).parent
    parent.mkdir(parents=True, exist_ok=True)


def reset_plc_over_serial(port: str, baud: int, reset_log_file: str, pulse_s: float, wait_s: float) -> None:
    ensure_parent(reset_log_file)
    kwargs = {
        "port": port,
        "baudrate": baud,
        "timeout": 0.2,
        "write_timeout": 0.5,
    }
    chunks = [
        f"{ts()} [HOST] reset port={port} baud={baud} pulse_s={max(0.05, pulse_s):.3f} wait_s={max(1.0, wait_s):.1f}\n"
    ]
    ser = None
    try:
        try:
            ser = serial.Serial(exclusive=True, **kwargs)
        except TypeError:
            ser = serial.Serial(**kwargs)
        ser.setDTR(False)
        ser.setRTS(True)
        time.sleep(max(0.05, pulse_s))
        ser.setDTR(True)
        ser.setRTS(False)
        deadline = time.time() + max(1.0, wait_s)
        while time.time() < deadline:
            try:
                raw = ser.read(4096)
            except Exception as exc:
                chunks.append(f"{ts()} [HOST] reset read failed: {exc}\n")
                break
            if raw:
                chunks.append(raw.decode("utf-8", errors="ignore"))
    finally:
        if ser is not None:
            try:
                ser.close()
            except Exception:
                pass
    Path(reset_log_file).write_text("".join(chunks), encoding="utf-8")


def load_summary(summary_file: str) -> dict:
    path = Path(summary_file)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_failure_summary(summary_file: str, failure: str) -> None:
    ensure_parent(summary_file)
    payload = {
        "result": "fail",
        "phase": "runner_wrapper",
        "events": [f"{ts()} phase=runner_wrapper"],
        "notes": [],
        "failures": [failure],
        "current_demand_started_at": None,
        "current_demand_ready_since": None,
        "vehicle_stop_requested": False,
        "vehicle_stop_reason": "",
        "plc": {},
        "module": {},
    }
    Path(summary_file).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_aggregate(base_prefix: str, rows: list[dict]) -> tuple[str, str]:
    aggregate_json = f"{base_prefix}_aggregate.json"
    aggregate_csv = f"{base_prefix}_aggregate.csv"
    ensure_parent(aggregate_json)
    payload = {
        "base_prefix": base_prefix,
        "generated_at": ts(),
        "sessions": rows,
        "passes": sum(1 for row in rows if row.get("result") == "pass"),
        "fails": sum(1 for row in rows if row.get("result") != "pass"),
    }
    Path(aggregate_json).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    fieldnames = [
        "session",
        "rc",
        "result",
        "failure",
        "slac_matched",
        "hlc_ready",
        "cp_phase",
        "current_demand_ready_since",
        "summary_file",
        "runner_file",
        "plc_log",
        "can_log",
        "reset_log",
    ]
    with open(aggregate_csv, "w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})
    return aggregate_json, aggregate_csv


def build_arg_parser() -> argparse.ArgumentParser:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(description="Batch external-controller charge test with PLC ESP reset before each session")
    parser.add_argument("--plc-port", required=True)
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument("--plc-id", type=int, default=None)
    parser.add_argument("--controller-id", type=int, default=1)
    parser.add_argument("--sessions", type=int, default=10)
    parser.add_argument("--gap-s", type=float, default=60.0)
    parser.add_argument("--current-demand-hold-s", type=float, default=120.0)
    parser.add_argument("--session-start-timeout-s", type=float, default=300.0)
    parser.add_argument("--child-timeout-s", type=float, default=0.0)
    parser.add_argument("--allow-zero-load-ready", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--esp-reset-pulse-s", type=float, default=DEFAULT_RESET_PULSE_S)
    parser.add_argument("--esp-reset-wait-s", type=float, default=DEFAULT_RESET_WAIT_S)
    parser.add_argument(
        "--base-prefix",
        default=f"/home/jpi/Desktop/EVSE/plc_firmware/logs/external_controller_reset_batch_{stamp}",
    )
    parser.add_argument(
        "--extra-arg",
        action="append",
        default=[],
        help="extra argument to forward to external_controller_single_module_charge_test.py; repeat as needed",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    if args.child_timeout_s <= 0.0:
        args.child_timeout_s = args.session_start_timeout_s + args.current_demand_hold_s + 180.0
    rows: list[dict] = []
    passes = 0
    fails = 0

    print(f"RUN_BASE={args.base_prefix}", flush=True)
    for session_idx in range(1, args.sessions + 1):
        session = f"{session_idx:02d}"
        plc_log = f"{args.base_prefix}_session{session}.log"
        can_log = f"{args.base_prefix}_session{session}_can.log"
        summary_file = f"{args.base_prefix}_session{session}_summary.json"
        runner_file = f"{args.base_prefix}_session{session}_runner.out"
        reset_log = f"{args.base_prefix}_session{session}_reset.log"

        print(f"[RESET] session={session} start={ts()}", flush=True)
        reset_plc_over_serial(
            args.plc_port,
            args.baud,
            reset_log,
            args.esp_reset_pulse_s,
            args.esp_reset_wait_s,
        )

        cmd = [
            sys.executable,
            str(SINGLE_SESSION_SCRIPT),
            "--plc-port",
            args.plc_port,
            "--baud",
            str(args.baud),
            "--controller-id",
            str(args.controller_id),
            "--current-demand-hold-s",
            str(args.current_demand_hold_s),
            "--session-start-timeout-s",
            str(args.session_start_timeout_s),
            "--plc-log",
            plc_log,
            "--can-log",
            can_log,
            "--summary-file",
            summary_file,
            "--no-esp-reset-before-start",
        ]
        if args.plc_id is not None:
            cmd.extend(["--plc-id", str(args.plc_id)])
        cmd.append("--allow-zero-load-ready" if args.allow_zero_load_ready else "--no-allow-zero-load-ready")
        for extra in args.extra_arg:
            cmd.append(extra)

        print(f"[RUN] session={session} start={ts()}", flush=True)
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        timed_out = False
        failure = ""
        try:
            stdout, _ = proc.communicate(timeout=args.child_timeout_s)
        except subprocess.TimeoutExpired:
            timed_out = True
            failure = f"session runner timeout after {args.child_timeout_s:.1f}s"
            proc.kill()
            stdout, _ = proc.communicate()
        Path(runner_file).write_text(stdout or "", encoding="utf-8")
        if timed_out and not Path(summary_file).exists():
            write_failure_summary(summary_file, failure)

        summary = load_summary(summary_file)
        result = summary.get("result", "missing")
        failure_list = summary.get("failures") or []
        failure = failure_list[-1] if failure_list else failure
        row = {
            "session": session,
            "rc": proc.returncode if not timed_out else 124,
            "result": result,
            "failure": failure,
            "slac_matched": summary.get("plc", {}).get("slac_matched"),
            "hlc_ready": summary.get("plc", {}).get("hlc_ready"),
            "cp_phase": summary.get("plc", {}).get("cp_phase"),
            "current_demand_ready_since": summary.get("current_demand_ready_since"),
            "summary_file": summary_file,
            "runner_file": runner_file,
            "plc_log": plc_log,
            "can_log": can_log,
            "reset_log": reset_log,
        }
        rows.append(row)

        if (not timed_out) and proc.returncode == 0 and result == "pass":
            passes += 1
        else:
            fails += 1
        done_rc = proc.returncode if not timed_out else 124
        print(f"[DONE] session={session} rc={done_rc} result={result} end={ts()}", flush=True)
        if session_idx < args.sessions:
            print(f"[GAP] after_session={session} sleep_s={int(args.gap_s)} start={ts()}", flush=True)
            time.sleep(max(0.0, args.gap_s))

    aggregate_json, aggregate_csv = write_aggregate(args.base_prefix, rows)
    print(f"[SUMMARY] base={args.base_prefix} passes={passes} fails={fails} end={ts()}", flush=True)
    print(f"[ARTIFACT] aggregate_json={aggregate_json}", flush=True)
    print(f"[ARTIFACT] aggregate_csv={aggregate_csv}", flush=True)
    return 0 if fails == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
