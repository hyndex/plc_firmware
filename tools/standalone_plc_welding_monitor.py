#!/usr/bin/env python3
"""
Monitor the real standalone-mode PLC stop path through welding detection.

This complements standalone_plc_live_monitor.py, which intentionally exits
after a sustained CurrentDemand hold. Welding detection happens later in the
protocol sequence, after PowerDelivery(stop), so it needs a separate monitor.
"""

from __future__ import annotations

import argparse
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

CFG_RE = re.compile(r"\[CFG\] mode=(\d+)\(([^)]+)\)")


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


def safe_bool_int(value: object) -> bool:
    try:
        return bool(int(value))
    except (TypeError, ValueError):
        return False


class Monitor:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.ser: Optional[serial.Serial] = None
        self.log_fp = None
        self.monitor_started_monotonic = 0.0
        self.hold_started_monotonic: Optional[float] = None
        self.hold_ok_last_monotonic: Optional[float] = None
        self.stop_command_sent = False
        self.metric_ts: dict[str, float] = {}
        self.summary = {
            "result": "fail",
            "port": args.port,
            "mode": "",
            "hold_s": args.hold_s,
            "post_hold_timeout_s": args.post_hold_timeout_s,
            "started_current_demand": False,
            "hold_completed": False,
            "stop_ramp_seen": False,
            "host_stop_cmd_sent": False,
            "power_delivery_stop_seen": False,
            "welding_req_seen": False,
            "welding_runtime_seen": False,
            "session_stop_req_seen": False,
            "session_stop_runtime_seen": False,
            "client_done_seen": False,
            "unexpected_disconnect_seen": False,
            "current_demand": {
                "last_req_v": 0.0,
                "last_req_i": 0.0,
                "last_present_v": 0.0,
                "last_present_i": 0.0,
                "peak_present_v": 0.0,
                "peak_present_i": 0.0,
                "hold_start_t_s": None,
                "hold_complete_t_s": None,
            },
            "welding": {
                "sample_count": 0,
                "shutdown_all_true": True,
                "relay1_all_open": True,
                "sources": [],
                "first_present_v": None,
                "last_present_v": None,
                "max_present_v": 0.0,
                "last_source": "",
                "last_age_ms": None,
                "last_shutdown": None,
                "last_relay1": None,
                "last_upstream_v": None,
                "last_module_target_v": None,
                "last_module_target_i": None,
            },
            "timing_s": {},
            "notes": [],
            "failure": "",
            "log_file": args.log_file,
            "summary_file": args.summary_file,
        }

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
        time.sleep(max(1.0, self.args.esp_reset_wait_s))

    def _parse_cfg(self, line: str) -> None:
        match = CFG_RE.search(line)
        if not match:
            return
        self.summary["mode"] = match.group(2)
        self._mark_metric("cfg_mode")

    def _parse_hlc_req(self, payload: dict[str, object], now: float) -> None:
        msg_type = str(payload.get("type", ""))
        if msg_type == "PowerDeliveryReq":
            ready_to_charge = payload.get("readyToCharge")
            charge_progress = str(payload.get("chargeProgress", ""))
            is_stop = False
            if ready_to_charge is not None:
                is_stop = not safe_bool_int(ready_to_charge)
            elif charge_progress:
                is_stop = charge_progress.lower() not in {"0", "start"}
            if is_stop:
                self.summary["power_delivery_stop_seen"] = True
                self._mark_metric("power_delivery_stop_req", now)
        elif msg_type == "WeldingDetectionReq":
            self.summary["welding_req_seen"] = True
            self._mark_metric("welding_req", now)
        elif msg_type == "SessionStopReq":
            self.summary["session_stop_req_seen"] = True
            self._mark_metric("session_stop_req", now)

    def _parse_current_demand(self, payload: dict[str, object], now: float) -> None:
        if not self.summary["started_current_demand"]:
            self.summary["power_delivery_stop_seen"] = False
            self.summary["welding_req_seen"] = False
            self.summary["welding_runtime_seen"] = False
            self.summary["session_stop_req_seen"] = False
            self.summary["session_stop_runtime_seen"] = False
            self.summary["client_done_seen"] = False
            self.summary["unexpected_disconnect_seen"] = False
            self.metric_ts.pop("power_delivery_stop_req", None)
            self.metric_ts.pop("welding_req", None)
            self.metric_ts.pop("welding_runtime", None)
            self.metric_ts.pop("session_stop_req", None)
            self.metric_ts.pop("session_stop_runtime", None)
            self.metric_ts.pop("client_done", None)
            self.metric_ts.pop("unexpected_disconnect", None)
        self.summary["started_current_demand"] = True
        self._mark_metric("current_demand_first", now)
        req_v = safe_float(payload.get("reqV"))
        req_i = safe_float(payload.get("reqI"))
        present_v = safe_float(payload.get("presentV"))
        present_i = safe_float(payload.get("presentI"))
        allow = safe_bool_int(payload.get("allow"))
        delivery = safe_bool_int(payload.get("delivery"))
        target_applied = safe_bool_int(payload.get("targetApplied"))
        ready = safe_bool_int(payload.get("ready"))
        relay1 = safe_bool_int(payload.get("relay1"))
        precharge_done = safe_bool_int(payload.get("prechargeDone"))
        hold_phase = safe_bool_int(payload.get("hold"))

        cd = self.summary["current_demand"]
        cd["last_req_v"] = req_v
        cd["last_req_i"] = req_i
        cd["last_present_v"] = present_v
        cd["last_present_i"] = present_i
        cd["peak_present_v"] = max(cd["peak_present_v"], present_v)
        cd["peak_present_i"] = max(cd["peak_present_i"], present_i)

        if hold_phase or req_i <= self.args.stop_ramp_current_a:
            if not self.summary["stop_ramp_seen"]:
                self.summary["stop_ramp_seen"] = True
                self._mark_metric("current_demand_stop_ramp", now)
                self._note_once(
                    f"stop ramp started at req={req_v:.1f}V/{req_i:.2f}A "
                    f"present={present_v:.1f}V/{present_i:.2f}A "
                    f"(hold={1 if hold_phase else 0})"
                )

        hold_ok = (
            allow
            and delivery
            and target_applied
            and ready
            and relay1
            and precharge_done
            and present_v >= self.args.min_present_v
            and present_i >= self.args.min_present_i
        )
        if hold_ok:
            self.hold_ok_last_monotonic = now
            if self.hold_started_monotonic is None:
                self.hold_started_monotonic = now
                cd["hold_start_t_s"] = round(now - self.monitor_started_monotonic, 3)
                self._mark_metric("current_demand_hold_start", now)
            if (now - self.hold_started_monotonic) >= self.args.hold_s:
                self.summary["hold_completed"] = True
                if cd["hold_complete_t_s"] is None:
                    cd["hold_complete_t_s"] = round(now - self.monitor_started_monotonic, 3)
                    self._mark_metric("current_demand_hold_complete", now)
        elif self.hold_started_monotonic is not None:
            if (
                not self.summary["stop_ramp_seen"]
                and self.hold_ok_last_monotonic is not None
                and (now - self.hold_ok_last_monotonic) > self.args.hold_grace_s
            ):
                raise RuntimeError("CurrentDemand hold broke before stop-phase validation")

    def _parse_welding_runtime(self, payload: dict[str, object], now: float) -> None:
        self.summary["welding_runtime_seen"] = True
        self._mark_metric("welding_runtime", now)
        welding = self.summary["welding"]
        welding["sample_count"] += 1
        shutdown = safe_bool_int(payload.get("shutdown"))
        relay1 = safe_bool_int(payload.get("relay1"))
        present_v = safe_float(payload.get("presentV"))
        source = str(payload.get("presentVSource", ""))
        age_ms = payload.get("presentVAgeMs")
        upstream_v = safe_float(payload.get("upstreamV", payload.get("measuredV")))
        module_target_v = safe_float(payload.get("moduleTargetV"))
        module_target_i = safe_float(payload.get("moduleTargetI"))
        if welding["first_present_v"] is None:
            welding["first_present_v"] = present_v
        welding["last_present_v"] = present_v
        welding["max_present_v"] = max(welding["max_present_v"], present_v)
        welding["last_source"] = source
        welding["last_age_ms"] = int(age_ms) if age_ms is not None else None
        welding["last_shutdown"] = shutdown
        welding["last_relay1"] = relay1
        welding["last_upstream_v"] = upstream_v
        welding["last_module_target_v"] = module_target_v
        welding["last_module_target_i"] = module_target_i
        if source and source not in welding["sources"]:
            welding["sources"].append(source)
        welding["shutdown_all_true"] = bool(welding["shutdown_all_true"] and shutdown)
        welding["relay1_all_open"] = bool(welding["relay1_all_open"] and (not relay1))

    def _parse_line(self, line: str, now: float) -> None:
        self._parse_cfg(line)
        if "unexpected disconnect:" in line:
            self.summary["unexpected_disconnect_seen"] = True
            self._mark_metric("unexpected_disconnect", now)
        if "client session done rc=" in line:
            self.summary["client_done_seen"] = True
            self._mark_metric("client_done", now)
        if "[HLC][REQ] {" in line:
            start = line.find("{")
            if start >= 0:
                try:
                    payload = json.loads(line[start:])
                except Exception:
                    payload = None
                if isinstance(payload, dict):
                    self._parse_hlc_req(payload, now)
        if "[HLC] {" not in line:
            return
        start = line.find("{")
        if start < 0:
            return
        try:
            payload = json.loads(line[start:])
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        msg = str(payload.get("msg", ""))
        if msg == "CurrentDemandRuntime":
            self._parse_current_demand(payload, now)
        elif msg == "WeldingDetectionRuntime":
            self._parse_welding_runtime(payload, now)
        elif msg == "SessionStopRuntime":
            self.summary["session_stop_runtime_seen"] = True
            self._mark_metric("session_stop_runtime", now)

    def _maybe_send_stop_after_hold(self, now: float) -> None:
        if not self.args.send_stop_after_hold:
            return
        if self.stop_command_sent or not self.summary["hold_completed"]:
            return
        if self.ser is None:
            return
        self.ser.write(b"CTRL STOP soft 3000\n")
        self.ser.flush()
        self.stop_command_sent = True
        self.summary["host_stop_cmd_sent"] = True
        self._mark_metric("host_stop_cmd", now)
        self._note_once("sent CTRL STOP soft 3000 after CurrentDemand hold to force welding-detection path")

    def _validate(self) -> None:
        if self.summary["mode"] != "standalone":
            raise RuntimeError(f"PLC not in standalone mode: {self.summary['mode'] or 'unknown'}")
        if not self.summary["started_current_demand"]:
            raise RuntimeError("never reached CurrentDemandRuntime before stop-phase validation")
        if self.hold_started_monotonic is None:
            raise RuntimeError("never reached a stable ready CurrentDemand sample before stop-phase validation")
        if not self.summary["hold_completed"] and not self.summary["stop_ramp_seen"]:
            raise RuntimeError(f"CurrentDemand hold did not complete {self.args.hold_s:.1f}s before stop-phase validation")
        if not self.summary["hold_completed"] and self.summary["stop_ramp_seen"]:
            self._note_once("accepted early stop-ramp session before hold timer completion")
        if not self.summary["power_delivery_stop_seen"]:
            raise RuntimeError("did not observe PowerDelivery(stop) after CurrentDemand hold")
        if not self.summary["welding_req_seen"]:
            raise RuntimeError("did not observe WeldingDetectionReq after PowerDelivery(stop)")
        if not self.summary["welding_runtime_seen"]:
            raise RuntimeError("did not observe WeldingDetectionRuntime response")
        welding = self.summary["welding"]
        if not welding["shutdown_all_true"]:
            raise RuntimeError("WeldingDetectionRuntime reported shutdown=0 during stop-phase")
        if not welding["relay1_all_open"]:
            raise RuntimeError("WeldingDetectionRuntime reported relay1 still closed during stop-phase")
        if not self.summary["session_stop_req_seen"]:
            raise RuntimeError("did not observe SessionStopReq after welding detection")
        if not self.summary["client_done_seen"]:
            raise RuntimeError("session did not complete after welding detection")
        if welding["last_source"] == "meter_cache":
            age_ms = welding["last_age_ms"]
            if age_ms is None or age_ms > self.args.max_welding_age_ms:
                raise RuntimeError("welding detection relied on stale meter cache voltage")
        if welding["last_source"] == "none":
            raise RuntimeError("welding detection had no usable voltage source")

    def run(self) -> int:
        ensure_parent(self.args.log_file)
        ensure_parent(self.args.summary_file)
        self.log_fp = open(self.args.log_file, "w", encoding="utf-8", buffering=1)
        try:
            self._open_serial()
            self._log(
                f"{ts()} [HOST] welding monitor start port={self.args.port} baud={self.args.baud} "
                f"hold_s={self.args.hold_s} post_hold_timeout_s={self.args.post_hold_timeout_s}"
            )
            self._reset_if_requested()
            self.monitor_started_monotonic = time.monotonic()
            deadline = self.monitor_started_monotonic + self.args.start_timeout_s + self.args.hold_s + self.args.post_hold_timeout_s + 120.0
            post_hold_deadline: Optional[float] = None
            rx_buffer = ""
            while time.monotonic() < deadline:
                raw = self.ser.read(4096) if self.ser is not None else b""
                if not raw:
                    if self.summary["hold_completed"] and post_hold_deadline is None:
                        post_hold_deadline = time.monotonic() + self.args.post_hold_timeout_s
                    if post_hold_deadline is not None and time.monotonic() >= post_hold_deadline:
                        break
                    if self.summary["client_done_seen"] and self.summary["welding_runtime_seen"]:
                        break
                    continue
                rx_buffer += raw.decode("utf-8", errors="replace")
                while "\n" in rx_buffer:
                    line, rx_buffer = rx_buffer.split("\n", 1)
                    line = line.rstrip("\r")
                    now = time.monotonic()
                    self._log(f"{ts()} {line}")
                    self._parse_line(line, now)
                    if (self.summary["hold_completed"] or self.summary["stop_ramp_seen"]) and post_hold_deadline is None:
                        post_hold_deadline = now + self.args.post_hold_timeout_s
                    self._maybe_send_stop_after_hold(now)
                    if self.summary["client_done_seen"] and self.summary["welding_runtime_seen"]:
                        break
                if self.summary["client_done_seen"] and self.summary["welding_runtime_seen"]:
                    break
                if post_hold_deadline is not None and time.monotonic() >= post_hold_deadline:
                    break

            self._validate()
            self.summary["result"] = "pass"
            return 0
        except Exception as exc:
            self.summary["failure"] = str(exc)
            self.summary["result"] = "fail"
            return 1
        finally:
            try:
                self.summary["timing_s"] = self._timing_payload()
                Path(self.args.summary_file).write_text(json.dumps(self.summary, indent=2), encoding="utf-8")
            finally:
                self._close_serial()
                if self.log_fp is not None:
                    self.log_fp.close()
                    self.log_fp = None


def build_arg_parser() -> argparse.ArgumentParser:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(description="Monitor standalone PLC welding detection on a live bench")
    parser.add_argument("--port", required=True)
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument("--start-timeout-s", type=float, default=180.0)
    parser.add_argument("--hold-s", type=float, default=20.0)
    parser.add_argument("--hold-grace-s", type=float, default=0.75)
    parser.add_argument("--stop-ramp-current-a", type=float, default=3.5)
    parser.add_argument("--post-hold-timeout-s", type=float, default=120.0)
    parser.add_argument("--min-present-v", type=float, default=50.0)
    parser.add_argument("--min-present-i", type=float, default=10.0)
    parser.add_argument("--max-welding-age-ms", type=int, default=1500)
    parser.add_argument("--send-stop-after-hold", action="store_true")
    parser.add_argument("--esp-reset-before-start", action="store_true")
    parser.add_argument("--esp-reset-pulse-s", type=float, default=DEFAULT_RESET_PULSE_S)
    parser.add_argument("--esp-reset-wait-s", type=float, default=DEFAULT_RESET_WAIT_S)
    default_log = f"/home/jpi/Desktop/EVSE/plc_firmware/logs/standalone_plc_welding_{stamp}.log"
    default_summary = f"/home/jpi/Desktop/EVSE/plc_firmware/logs/standalone_plc_welding_{stamp}_summary.json"
    parser.add_argument("--log-file", default=default_log)
    parser.add_argument("--summary-file", default=default_summary)
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    return Monitor(args).run()


if __name__ == "__main__":
    sys.exit(main())
