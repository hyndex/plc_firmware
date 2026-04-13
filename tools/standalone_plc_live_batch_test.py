#!/usr/bin/env python3

import argparse
import csv
import datetime as dt
import json
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
SINGLE_SESSION_SCRIPT = ROOT / "tools" / "standalone_plc_live_monitor.py"


def ts() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


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
        "failure": failure,
        "failure_stage": "runner_wrapper",
        "furthest_stage": "runner_wrapper",
        "timing_s": {},
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
        "failure_stage",
        "furthest_stage",
        "cfg_mode_s",
        "cp_b1_s",
        "cp_c_s",
        "sdp_request_s",
        "evcc_connected_s",
        "precharge_runtime_s",
        "power_delivery_runtime_s",
        "current_demand_first_s",
        "current_demand_hold_start_s",
        "current_demand_hold_complete_s",
        "peak_present_v_hold",
        "last_req_v",
        "last_req_i",
        "last_applied_v",
        "last_applied_i",
        "last_present_v",
        "last_present_i",
        "max_applied_v_error",
        "max_applied_i_error",
        "max_present_v_error_hold",
        "summary_file",
        "runner_file",
        "log_file",
    ]
    with open(aggregate_csv, "w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})
    return aggregate_json, aggregate_csv


def build_arg_parser() -> argparse.ArgumentParser:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(description="Batch real standalone-mode PLC live monitor with ESP reset before each session")
    parser.add_argument("--port", required=True)
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument("--sessions", type=int, default=5)
    parser.add_argument("--gap-s", type=float, default=10.0)
    parser.add_argument("--hold-s", type=float, default=300.0)
    parser.add_argument("--start-timeout-s", type=float, default=180.0)
    parser.add_argument("--child-timeout-s", type=float, default=0.0)
    parser.add_argument("--esp-reset-pulse-s", type=float, default=0.2)
    parser.add_argument("--esp-reset-wait-s", type=float, default=14.0)
    parser.add_argument("--min-present-v", type=float, default=10.0)
    parser.add_argument("--min-current-a", type=float, default=1.0)
    parser.add_argument("--max-applied-v-error", type=float, default=1.0)
    parser.add_argument("--max-applied-i-error", type=float, default=0.6)
    parser.add_argument("--max-present-v-error", type=float, default=2.0)
    parser.add_argument("--no-load-bench", action="store_true")
    parser.add_argument(
        "--base-prefix",
        default=f"/home/jpi/Desktop/EVSE/plc_firmware/logs/standalone_plc_live_batch_{stamp}",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    if args.child_timeout_s <= 0.0:
        args.child_timeout_s = args.start_timeout_s + args.hold_s + args.esp_reset_wait_s + 180.0

    rows: list[dict] = []
    passes = 0
    fails = 0
    print(f"RUN_BASE={args.base_prefix}", flush=True)
    for session_idx in range(1, args.sessions + 1):
        session = f"{session_idx:02d}"
        log_file = f"{args.base_prefix}_session{session}.log"
        summary_file = f"{args.base_prefix}_session{session}_summary.json"
        runner_file = f"{args.base_prefix}_session{session}_runner.out"
        cmd = [
            sys.executable,
            str(SINGLE_SESSION_SCRIPT),
            "--port",
            args.port,
            "--baud",
            str(args.baud),
            "--start-timeout-s",
            str(args.start_timeout_s),
            "--hold-s",
            str(args.hold_s),
            "--min-present-v",
            str(args.min_present_v),
            "--min-current-a",
            str(args.min_current_a),
            "--max-applied-v-error",
            str(args.max_applied_v_error),
            "--max-applied-i-error",
            str(args.max_applied_i_error),
            "--max-present-v-error",
            str(args.max_present_v_error),
            "--esp-reset-before-start",
            "--esp-reset-pulse-s",
            str(args.esp_reset_pulse_s),
            "--esp-reset-wait-s",
            str(args.esp_reset_wait_s),
            "--log-file",
            log_file,
            "--summary-file",
            summary_file,
        ]
        if args.no_load_bench:
            cmd.append("--no-load-bench")

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
        failure = summary.get("failure", failure)
        timing = summary.get("timing_s") or {}
        row = {
            "session": session,
            "rc": proc.returncode if not timed_out else 124,
            "result": result,
            "failure": failure,
            "failure_stage": summary.get("failure_stage", ""),
            "furthest_stage": summary.get("furthest_stage", ""),
            "cfg_mode_s": timing.get("cfg_mode"),
            "cp_b1_s": timing.get("cp_b1"),
            "cp_c_s": timing.get("cp_c"),
            "sdp_request_s": timing.get("sdp_request"),
            "evcc_connected_s": timing.get("evcc_connected"),
            "precharge_runtime_s": timing.get("precharge_runtime"),
            "power_delivery_runtime_s": timing.get("power_delivery_runtime"),
            "current_demand_first_s": timing.get("current_demand_first"),
            "current_demand_hold_start_s": timing.get("current_demand_hold_start"),
            "current_demand_hold_complete_s": timing.get("current_demand_hold_complete"),
            "peak_present_v_hold": summary.get("peak_present_v_hold"),
            "last_req_v": summary.get("last_req_v"),
            "last_req_i": summary.get("last_req_i"),
            "last_applied_v": summary.get("last_applied_v"),
            "last_applied_i": summary.get("last_applied_i"),
            "last_present_v": summary.get("last_present_v"),
            "last_present_i": summary.get("last_present_i"),
            "max_applied_v_error": summary.get("max_applied_v_error"),
            "max_applied_i_error": summary.get("max_applied_i_error"),
            "max_present_v_error_hold": summary.get("max_present_v_error_hold"),
            "summary_file": summary_file,
            "runner_file": runner_file,
            "log_file": log_file,
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
