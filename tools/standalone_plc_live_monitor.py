#!/usr/bin/env python3
"""
Monitor a real firmware standalone-mode HLC session on a live PLC.

This validates the actual PLC mode=0 path, where the firmware owns SLAC/HLC,
applies EV-requested power targets with configured headroom, and reports live
module telemetry over serial logs.
"""

from __future__ import annotations

import argparse
from collections import deque
import datetime as dt
import json
import math
import os
from pathlib import Path
import re
import sys
import time
from typing import Optional

try:
    import serial  # type: ignore
except Exception as exc:
    print(f"[ERR] pyserial import failed: {exc}", file=sys.stderr)
    sys.exit(2)


DEFAULT_RESET_PULSE_S = 0.2
DEFAULT_RESET_WAIT_S = 14.0

CFG_RE = re.compile(
    r"\[CFG\] mode=(\d+)\(([^)]+)\).*?headroom_v=([0-9.+-]+)\s+headroom_i=([0-9.+-]+)"
    r"(?:\s+hlc_(?:follow_ev|precharge_exact)=([01]))?"
)
CP_TRANSITION_RE = re.compile(r"\[CP\] state .* -> ([A-F](?:[12])?)")


def ts() -> str:
    return dt.datetime.now().isoformat(timespec="milliseconds")


def ensure_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def safe_float(value: object, fallback: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return fallback
    if not math.isfinite(parsed):
        return fallback
    return parsed


class Monitor:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.log_fp = None
        self.ser = None
        self.summary = {
            "result": "fail",
            "port": args.port,
            "mode": "",
            "headroom_expected_v": None,
            "headroom_expected_a": None,
            "hlc_follow_ev": None,
            "start_timeout_s": args.start_timeout_s,
            "hold_s": args.hold_s,
            "min_current_a": args.min_current_a,
            "no_load_bench": bool(args.no_load_bench),
            "started_current_demand": False,
            "hold_started": False,
            "hold_completed": False,
            "hold_start_t_s": None,
            "hold_end_t_s": None,
            "peak_present_v": 0.0,
            "peak_present_v_hold": 0.0,
            "min_present_v_hold": None,
            "peak_present_i": 0.0,
            "peak_present_i_hold": 0.0,
            "peak_applied_v": 0.0,
            "peak_applied_i": 0.0,
            "last_req_v": 0.0,
            "last_req_i": 0.0,
            "last_applied_v": 0.0,
            "last_applied_i": 0.0,
            "last_present_v": 0.0,
            "last_present_i": 0.0,
            "max_applied_v_error": 0.0,
            "max_applied_i_error": 0.0,
            "max_present_v_error_any": 0.0,
            "max_present_v_error_hold": 0.0,
            "notes": [],
            "failure": "",
            "failure_stage": "",
            "furthest_stage": "",
            "timing_s": {},
            "log_file": args.log_file,
            "summary_file": args.summary_file,
        }
        self.monitor_started_monotonic = 0.0
        self.hold_started_monotonic = None
        self.last_hold_ok_monotonic = None
        self.metric_ts: dict[str, float] = {}
        self.loaded_samples: deque[tuple[float, float, float]] = deque()

    def _note_once(self, text: str) -> None:
        if text not in self.summary["notes"]:
            self.summary["notes"].append(text)

    def _log(self, line: str) -> None:
        assert self.log_fp is not None
        self.log_fp.write(line + "\n")

    def _mark_metric(self, name: str, now: Optional[float] = None) -> None:
        if name in self.metric_ts:
            return
        self.metric_ts[name] = time.monotonic() if now is None else now

    def _timing_payload(self) -> dict[str, float]:
        if self.monitor_started_monotonic <= 0.0:
            return {}
        return {
            name: round(max(0.0, when - self.monitor_started_monotonic), 3)
            for name, when in sorted(self.metric_ts.items())
        }

    def _prune_loaded_samples(self, now: float) -> None:
        window_s = max(0.0, self.args.loaded_settle_window_s)
        if window_s <= 0.0:
            self.loaded_samples.clear()
            return
        cutoff = now - window_s
        while self.loaded_samples and self.loaded_samples[0][0] < cutoff:
            self.loaded_samples.popleft()

    def _loaded_voltage_settled(self, now: float) -> bool:
        if self.args.no_load_bench:
            return True
        window_s = max(0.0, self.args.loaded_settle_window_s)
        if window_s <= 0.0:
            return True
        self._prune_loaded_samples(now)
        if len(self.loaded_samples) < max(2, self.args.loaded_settle_min_samples):
            return False
        required_coverage_s = max(0.5, window_s * self.args.loaded_settle_coverage_ratio)
        if (self.loaded_samples[-1][0] - self.loaded_samples[0][0]) < required_coverage_s:
            return False
        voltages = [sample[1] for sample in self.loaded_samples]
        currents = [sample[2] for sample in self.loaded_samples]
        voltage_span = max(voltages) - min(voltages)
        current_span = max(currents) - min(currents)
        return (
            voltage_span <= self.args.loaded_settle_delta_v
            and current_span <= self.args.loaded_settle_delta_a
        )

    def _furthest_stage(self) -> str:
        stage_order = [
            ("current_demand_hold_complete", "current_demand_hold_complete"),
            ("current_demand_hold_start", "current_demand_hold_start"),
            ("current_demand_first", "current_demand_first"),
            ("power_delivery_runtime", "power_delivery_runtime"),
            ("precharge_runtime", "precharge_runtime"),
            ("evcc_connected", "evcc_connected"),
            ("sdp_request", "sdp_request"),
            ("cp_c", "cp_c"),
            ("cp_b1", "cp_b1"),
            ("cfg_mode", "cfg_mode"),
        ]
        for metric_name, label in stage_order:
            if metric_name in self.metric_ts:
                return label
        return "startup"

    def _open_serial(self) -> None:
        self.ser = serial.Serial(
            port=self.args.port,
            baudrate=self.args.baud,
            timeout=0.05,
            write_timeout=0.5,
            inter_byte_timeout=0.01,
            exclusive=True,
        )

    def _close_serial(self) -> None:
        if self.ser is not None:
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None

    def _reset_if_requested(self) -> None:
        if not self.args.esp_reset_before_start:
            return
        assert self.ser is not None
        self._log(
            f"{ts()} [HOST] reset port={self.args.port} pulse_s={self.args.esp_reset_pulse_s:.3f} "
            f"wait_s={self.args.esp_reset_wait_s:.1f}"
        )
        self.ser.setDTR(False)
        self.ser.setRTS(True)
        time.sleep(max(0.05, self.args.esp_reset_pulse_s))
        self.ser.setDTR(True)
        self.ser.setRTS(False)
        boot_deadline = time.monotonic() + max(1.0, self.args.esp_reset_wait_s)
        rx_buffer = ""
        while time.monotonic() < boot_deadline:
            raw = self.ser.read(4096)
            if not raw:
                continue
            rx_buffer += raw.decode("utf-8", errors="replace")
            while "\n" in rx_buffer:
                line, rx_buffer = rx_buffer.split("\n", 1)
                line = line.rstrip("\r")
                now = time.monotonic()
                self._log(f"{ts()} {line}")
                self._parse_cfg(line)
                self._parse_runtime_events(line, now)
                self._parse_hlc_json(line, now)

    def _parse_cfg(self, line: str) -> None:
        match = CFG_RE.search(line)
        if not match:
            return
        mode_name = match.group(2)
        self.summary["mode"] = mode_name
        self.summary["headroom_expected_v"] = safe_float(match.group(3))
        self.summary["headroom_expected_a"] = safe_float(match.group(4))
        follow_ev = match.group(5)
        self.summary["hlc_follow_ev"] = None if follow_ev is None else bool(int(follow_ev))
        self._mark_metric("cfg_mode")

    def _parse_runtime_events(self, line: str, now: float) -> None:
        cp_match = CP_TRANSITION_RE.search(line)
        if cp_match:
            phase = cp_match.group(1)
            if phase == "B1":
                self._mark_metric("cp_b1", now)
            elif phase == "B2":
                self._mark_metric("cp_b2", now)
            elif phase == "C":
                self._mark_metric("cp_c", now)
            elif phase == "D":
                self._mark_metric("cp_d", now)
        if "[SDP] req" in line:
            self._mark_metric("sdp_request", now)
            self._note_once("standalone firmware log does not emit an explicit SLAC matched event; SDP request is used as the SLAC-match proxy")
        if "EVCC connected, processing session" in line:
            self._mark_metric("evcc_connected", now)
        if '"msg":"PreChargeRuntime"' in line:
            self._mark_metric("precharge_runtime", now)
        if '"msg":"PowerDeliveryRuntime"' in line:
            self._mark_metric("power_delivery_runtime", now)

    def _parse_hlc_json(self, line: str, now: float) -> None:
        if "[HLC] {" not in line:
            return
        payload_start = line.find("{")
        if payload_start < 0:
            return
        try:
            payload = json.loads(line[payload_start:])
        except Exception:
            return

        msg = str(payload.get("msg", ""))
        if msg != "CurrentDemandRuntime":
            return

        self.summary["started_current_demand"] = True
        self._mark_metric("current_demand_first", now)
        req_v = safe_float(payload.get("reqV"))
        req_i = safe_float(payload.get("reqI"))
        applied_v = safe_float(payload.get("appliedV"))
        applied_i = safe_float(payload.get("appliedI"))
        present_v = safe_float(payload.get("presentV"))
        present_i = safe_float(payload.get("presentI"))
        ready = bool(int(payload.get("ready", 0)))
        allow = bool(int(payload.get("allow", 0)))
        delivery = bool(int(payload.get("delivery", 0)))
        target_applied = bool(int(payload.get("targetApplied", 0)))
        relay1 = bool(int(payload.get("relay1", 0)))
        precharge_done = bool(int(payload.get("prechargeDone", 0)))

        self.summary["last_req_v"] = req_v
        self.summary["last_req_i"] = req_i
        self.summary["last_applied_v"] = applied_v
        self.summary["last_applied_i"] = applied_i
        self.summary["last_present_v"] = present_v
        self.summary["last_present_i"] = present_i
        self.summary["peak_present_v"] = max(self.summary["peak_present_v"], present_v)
        self.summary["peak_present_i"] = max(self.summary["peak_present_i"], present_i)
        self.summary["peak_applied_v"] = max(self.summary["peak_applied_v"], applied_v)
        self.summary["peak_applied_i"] = max(self.summary["peak_applied_i"], applied_i)
        self.loaded_samples.append((now, present_v, present_i))
        self._prune_loaded_samples(now)
        present_v_error = abs(present_v - applied_v)
        self.summary["max_present_v_error_any"] = max(
            self.summary["max_present_v_error_any"],
            present_v_error,
        )

        expected_headroom_v = self.summary.get("headroom_expected_v")
        expected_headroom_a = self.summary.get("headroom_expected_a")
        if expected_headroom_v is not None and delivery and target_applied:
            # Active standalone CurrentDemand reports the physically applied
            # module targets, and the firmware now keeps configured headroom
            # in this loop even when hlc_follow_ev is enabled.
            expected_applied_v = max(0.0, req_v - safe_float(expected_headroom_v))
            self.summary["max_applied_v_error"] = max(
                self.summary["max_applied_v_error"],
                abs(applied_v - expected_applied_v),
            )
        if expected_headroom_a is not None and delivery and target_applied:
            expected_applied_i = max(0.0, req_i - safe_float(expected_headroom_a))
            self.summary["max_applied_i_error"] = max(
                self.summary["max_applied_i_error"],
                abs(applied_i - expected_applied_i),
            )

        voltage_aligned = (not self.args.no_load_bench) or present_v_error <= self.args.max_present_v_error
        current_floor_a = self.args.min_current_a
        if not self.args.no_load_bench:
            current_floor_a = max(current_floor_a, applied_i * self.args.min_current_ratio)
        loaded_voltage_settled = self._loaded_voltage_settled(now)
        hold_ok = (
            allow
            and delivery
            and target_applied
            and ready
            and relay1
            and precharge_done
            and present_v >= self.args.min_present_v
            and voltage_aligned
            and loaded_voltage_settled
            and (
                self.args.no_load_bench
                or present_i >= current_floor_a
            )
        )
        if hold_ok:
            self.last_hold_ok_monotonic = now
            self.summary["peak_present_v_hold"] = max(self.summary["peak_present_v_hold"], present_v)
            self.summary["peak_present_i_hold"] = max(self.summary["peak_present_i_hold"], present_i)
            min_present_v_hold = self.summary["min_present_v_hold"]
            if min_present_v_hold is None or present_v < min_present_v_hold:
                self.summary["min_present_v_hold"] = present_v
            self.summary["max_present_v_error_hold"] = max(
                self.summary["max_present_v_error_hold"],
                present_v_error,
            )
            if self.hold_started_monotonic is None:
                self.hold_started_monotonic = now
                self.summary["hold_started"] = True
                self.summary["hold_start_t_s"] = round(now - self.monitor_started_monotonic, 3)
                self._mark_metric("current_demand_hold_start", now)
                if self.args.no_load_bench:
                    self._note_once(
                        f"hold started after settle at req={req_v:.1f}V/{req_i:.2f}A "
                        f"applied={applied_v:.1f}V/{applied_i:.2f}A "
                        f"present={present_v:.1f}V/{present_i:.2f}A "
                        f"(dv={present_v_error:.2f}V)"
                    )
                else:
                    self._note_once(
                        f"loaded hold started at req={req_v:.1f}V/{req_i:.2f}A "
                        f"applied={applied_v:.1f}V/{applied_i:.2f}A "
                        f"present={present_v:.1f}V/{present_i:.2f}A "
                        f"(settle_window={self.args.loaded_settle_window_s:.1f}s)"
                    )
            held_s = now - self.hold_started_monotonic
            if held_s >= self.args.hold_s:
                self.summary["hold_completed"] = True
                self.summary["hold_end_t_s"] = round(now - self.monitor_started_monotonic, 3)
                self._mark_metric("current_demand_hold_complete", now)
        elif self.hold_started_monotonic is not None:
            if self.last_hold_ok_monotonic is not None and (now - self.last_hold_ok_monotonic) > self.args.hold_grace_s:
                self.summary["failure"] = (
                    f"hold interrupted at req={req_v:.1f}V/{req_i:.2f}A "
                    f"applied={applied_v:.1f}V/{applied_i:.2f}A "
                    f"present={present_v:.1f}V/{present_i:.2f}A"
                )
                raise RuntimeError(self.summary["failure"])

    def _validate_alignment(self) -> None:
        if not self.summary["started_current_demand"]:
            raise RuntimeError("never reached CurrentDemandRuntime")
        if not self.summary["hold_started"]:
            raise RuntimeError("CurrentDemand never reached stable ready hold")
        if not self.summary["hold_completed"]:
            raise RuntimeError(f"CurrentDemand hold did not complete {self.args.hold_s:.1f}s")
        if self.summary["mode"] and self.summary["mode"] != "standalone":
            raise RuntimeError(f"PLC not in standalone mode: {self.summary['mode'] or 'unknown'}")
        if not self.summary["mode"]:
            self._note_once("cfg banner not observed during reset window; mode inferred from flashed standalone image and runtime behavior")
        if self.summary["headroom_expected_v"] is not None and self.summary["max_applied_v_error"] > self.args.max_applied_v_error:
            raise RuntimeError(
                f"applied voltage deviated from requested-headroom by {self.summary['max_applied_v_error']:.2f}V"
            )
        if self.summary["headroom_expected_a"] is not None and self.summary["max_applied_i_error"] > self.args.max_applied_i_error:
            raise RuntimeError(
                f"applied current deviated from requested-headroom by {self.summary['max_applied_i_error']:.2f}A"
            )
        if self.args.no_load_bench and self.summary["max_present_v_error_hold"] > self.args.max_present_v_error:
            raise RuntimeError(
                f"present voltage deviated from applied target by {self.summary['max_present_v_error_hold']:.2f}V during hold"
            )

    def run(self) -> int:
        ensure_parent(self.args.log_file)
        ensure_parent(self.args.summary_file)
        self.log_fp = open(self.args.log_file, "w", encoding="utf-8", buffering=1)
        try:
            self._open_serial()
            self._log(
                f"{ts()} [HOST] monitor start port={self.args.port} baud={self.args.baud} "
                f"start_timeout_s={self.args.start_timeout_s} hold_s={self.args.hold_s}"
            )
            self._reset_if_requested()
            self.monitor_started_monotonic = time.monotonic()

            deadline = self.monitor_started_monotonic + self.args.start_timeout_s + self.args.hold_s + 60.0
            rx_buffer = ""
            while time.monotonic() < deadline:
                raw = self.ser.read(4096) if self.ser is not None else b""
                if not raw:
                    if self.summary["hold_completed"]:
                        break
                    continue
                rx_buffer += raw.decode("utf-8", errors="replace")
                while "\n" in rx_buffer:
                    line, rx_buffer = rx_buffer.split("\n", 1)
                    line = line.rstrip("\r")
                    now = time.monotonic()
                    self._log(f"{ts()} {line}")
                    self._parse_cfg(line)
                    self._parse_runtime_events(line, now)
                    self._parse_hlc_json(line, now)
                    if self.summary["hold_completed"]:
                        break
                if self.summary["hold_completed"]:
                    break

            self._validate_alignment()
            self.summary["result"] = "pass"
            return 0
        except Exception as exc:
            if not self.summary["failure"]:
                self.summary["failure"] = str(exc)
            self.summary["result"] = "fail"
            return 1
        finally:
            try:
                self.summary["timing_s"] = self._timing_payload()
                self.summary["furthest_stage"] = self._furthest_stage()
                if self.summary["result"] != "pass":
                    self.summary["failure_stage"] = self.summary["furthest_stage"]
                Path(self.args.summary_file).write_text(json.dumps(self.summary, indent=2), encoding="utf-8")
            finally:
                self._close_serial()
                if self.log_fp is not None:
                    self.log_fp.close()
                    self.log_fp = None


def build_arg_parser() -> argparse.ArgumentParser:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(description="Monitor a live firmware standalone-mode PLC session")
    parser.add_argument("--port", required=True)
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument("--start-timeout-s", type=float, default=180.0)
    parser.add_argument("--hold-s", type=float, default=300.0)
    parser.add_argument("--hold-grace-s", type=float, default=0.75)
    parser.add_argument("--min-present-v", type=float, default=10.0)
    parser.add_argument("--min-current-a", type=float, default=1.0)
    parser.add_argument("--min-current-ratio", type=float, default=0.8)
    parser.add_argument("--loaded-settle-window-s", type=float, default=1.5)
    parser.add_argument("--loaded-settle-delta-v", type=float, default=5.0)
    parser.add_argument("--loaded-settle-delta-a", type=float, default=1.0)
    parser.add_argument("--loaded-settle-min-samples", type=int, default=4)
    parser.add_argument("--loaded-settle-coverage-ratio", type=float, default=0.75)
    parser.add_argument("--no-load-bench", action="store_true")
    parser.add_argument("--max-applied-v-error", type=float, default=1.0)
    parser.add_argument("--max-applied-i-error", type=float, default=0.6)
    parser.add_argument("--max-present-v-error", type=float, default=2.0)
    parser.add_argument("--esp-reset-before-start", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--esp-reset-pulse-s", type=float, default=DEFAULT_RESET_PULSE_S)
    parser.add_argument("--esp-reset-wait-s", type=float, default=DEFAULT_RESET_WAIT_S)
    parser.add_argument(
        "--log-file",
        default=f"/home/jpi/Desktop/EVSE/plc_firmware/logs/standalone_plc_live_{stamp}.log",
    )
    parser.add_argument(
        "--summary-file",
        default=f"/home/jpi/Desktop/EVSE/plc_firmware/logs/standalone_plc_live_{stamp}_summary.json",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    monitor = Monitor(args)
    rc = monitor.run()
    print(f"LOG_FILE={args.log_file}")
    print(f"SUMMARY_FILE={args.summary_file}")
    if rc == 0:
        print("[PASS] standalone PLC live monitor")
    else:
        print(f"[FAIL] {monitor.summary['failure']}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
