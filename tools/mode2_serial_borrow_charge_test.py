#!/usr/bin/env python3
"""
Serial-only controller-managed borrow/release charging simulator.

Behavior:
  1. Bootstrap borrower PLC in mode 2 over UART.
  2. Bootstrap lender PLC in controller-backed idle mode over UART.
  3. Start charging on borrower with its home module only.
  4. After the configured local-only window, add the lender module into the
     borrower allowlist while the lender bridge stays open.
  5. Wait until the donor module is energized and voltage-matched in isolation,
     then close lender Relay2.
  6. Keep charging with the shared module set for the configured shared window.
  7. For release, reduce to the home-module budget, open lender Relay2 once the
     donor current is quiet, then drop the donor allocation.
  8. Hold a short post-release window and stop cleanly.

The borrower PLC is the only HLC endpoint. The lender PLC stays idle and only
provides its module plus the lender-side bridge relay (Relay2 by default).

This script is legacy-only. It assumes the PLC owns module allocation/power,
which is no longer true in mode 2.
"""

from __future__ import annotations

import argparse
from collections import deque
import datetime as dt
import json
import os
import random
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

try:
    import serial  # type: ignore
except Exception as exc:
    print(f"[ERR] pyserial import failed: {exc}", file=sys.stderr)
    sys.exit(2)


LEGACY_MODE2_ERROR = (
    "tools/mode2_serial_borrow_charge_test.py is obsolete for the current "
    "mode-2 UART-router architecture. It still routes module allocation/power "
    "through the PLC instead of controlling modules directly over CAN."
)


CURRENT_DEMAND_RE = re.compile(r'"msg"\s*:\s*"CurrentDemand"')
HLC_RX_REQ_RE = re.compile(r"RX (PreCharge|PowerDelivery|CurrentDemand)Req\b")
CTRL_STATUS_RE = re.compile(r"\[SERCTRL\] STATUS .*cp=([A-Z0-9]+)\s+duty=(\d+)")
STATUS_ALLOC_RE = re.compile(r"alloc_sz=(\d+)")
STATUS_STOP_DONE_RE = re.compile(r"stop_done=(\d+)")
SESSION_SETUP_EVCCID_RE = re.compile(r'"msg"\s*:\s*"SessionSetup".*?"evccId"\s*:\s*"([0-9A-Fa-f]+)"')
EV_MAC_RE = re.compile(r"(?:EV[_ ]?MAC|ev_mac|evmac)[^0-9A-Fa-f]*([0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2}){5})")
SOC_RE = re.compile(r'"soc"\s*:\s*(-?\d+)')
WH_RE = re.compile(r'"wh"\s*:\s*(-?\d+)')
CP_TRANSITION_RE = re.compile(r"\[CP\] state .* -> ([A-F](?:[12])?)")
MODRT_GRP_RE = re.compile(
    r"\[MODRT\]\[GRP\].*en=(\d+).*assigned=(\d+).*active=(\d+).*reqV=([0-9.+-]+).*reqI=([0-9.+-]+).*cmbV=([0-9.+-]+).*cmbI=([0-9.+-]+).*availI=([0-9.+-]+)"
)
MODRT_MOD_RE = re.compile(
    r"\[MODRT\]\[MOD\].*id=([^\s]+).*lease=(\d+).*off=(\d+).*fault=(\d+).*online=(\d+).*fresh=(\d+).*V=([0-9.+-]+).*I=([0-9.+-]+)"
)
RELAY_CHANGE_RE = re.compile(r"\[RELAY\] Relay(\d+) -> (OPEN|CLOSED)")


def _safe_float(value: object) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp_non_negative(value: Optional[float], fallback: float = 0.0) -> float:
    if value is None:
        return fallback
    return max(0.0, value)


def _steady_ts() -> str:
    return dt.datetime.now().isoformat(timespec="milliseconds")


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _extract_json_payload(line: str) -> Optional[dict[str, object]]:
    start = line.find("{")
    end = line.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        payload = json.loads(line[start : end + 1])
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


@dataclass
class ModuleSpec:
    module_id: str
    can_group: int
    can_address: int
    limit_kw: float


@dataclass
class ModuleRuntime:
    module_id: str
    lease_active: int = 0
    off: int = 1
    fault: int = 0
    online: int = 0
    fresh: int = 0
    voltage_v: float = 0.0
    current_a: float = 0.0


@dataclass
class PlcState:
    name: str
    cp_connected: bool = False
    cp_phase: str = "U"
    cp_duty_pct: Optional[int] = None
    b1_since_ts: Optional[float] = None
    slac_matched: bool = False
    hlc_connected: bool = False
    precharge_seen: bool = False
    power_delivery_seen: bool = False
    current_demand_count: int = 0
    first_current_demand_ts: Optional[float] = None
    last_current_demand_ts: Optional[float] = None
    charging_window_start: Optional[float] = None
    session_done_count: int = 0
    last_session_done_ts: Optional[float] = None
    identity_seen: bool = False
    identity_source: str = ""
    identity_value: str = ""
    relay_states: dict[int, bool] = field(default_factory=dict)
    ctrl_alloc_sz: Optional[int] = None
    ctrl_stop_done: Optional[bool] = None
    group_enable: Optional[int] = None
    group_assigned_modules: Optional[int] = None
    group_active_modules: Optional[int] = None
    group_request_v: Optional[float] = None
    group_request_i: Optional[float] = None
    group_voltage_v: Optional[float] = None
    group_current_a: Optional[float] = None
    group_avail_i: Optional[float] = None
    modules: dict[str, ModuleRuntime] = field(default_factory=dict)
    last_hlc_stage: str = ""
    last_ev_target_v: Optional[float] = None
    last_ev_target_i: Optional[float] = None
    last_ev_present_v: Optional[float] = None
    last_ev_present_i: Optional[float] = None
    last_ev_status: str = ""
    last_ev_request_ts: Optional[float] = None
    last_live_voltage_v: Optional[float] = None
    last_live_voltage_ts: Optional[float] = None
    precharge_ready_seen: bool = False
    current_demand_ready_seen: bool = False
    precharge_ready_count: int = 0
    current_demand_valid_voltage_count: int = 0
    current_demand_ready_count: int = 0
    last_soc: Optional[int] = None
    last_meter_wh: Optional[int] = None
    cp_a_count: int = 0
    cp_f_count: int = 0
    errors: list[str] = field(default_factory=list)


class SerialLink:
    def __init__(
        self,
        port: str,
        baud: int,
        log_file: str,
        parse_line: Callable[[str], None],
    ) -> None:
        self.port = port
        self.baud = baud
        self.log_file = log_file
        self.parse_line = parse_line
        self.ser: Optional[serial.Serial] = None
        self.log_fp = None
        self.stop_event = threading.Event()
        self.reader_thread: Optional[threading.Thread] = None
        self.ser_lock = threading.RLock()
        self.write_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.last_write_ts = 0.0
        self.reader_error: Optional[str] = None

    def _open_serial(self) -> serial.Serial:
        kwargs = {
            "port": self.port,
            "baudrate": self.baud,
            "timeout": 0.05,
            "write_timeout": 0.5,
        }
        try:
            return serial.Serial(exclusive=True, **kwargs)
        except TypeError:
            return serial.Serial(**kwargs)

    def open(self) -> None:
        _ensure_parent_dir(self.log_file)
        self.stop_event.clear()
        self.reader_error = None
        self.log_fp = open(self.log_file, "w", encoding="utf-8", buffering=1)
        with self.ser_lock:
            self.ser = self._open_serial()
            self.ser.reset_input_buffer()
            self.ser.reset_output_buffer()
        self.reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self.reader_thread.start()

    def close(self) -> None:
        self.stop_event.set()
        if self.reader_thread and self.reader_thread.is_alive():
            self.reader_thread.join(timeout=2.0)
        with self.ser_lock:
            if self.ser:
                try:
                    self.ser.close()
                except Exception:
                    pass
                self.ser = None
        if self.log_fp:
            self.log_fp.close()
            self.log_fp = None

    def _log(self, line: str) -> None:
        if self.log_fp:
            with self.log_lock:
                self.log_fp.write(line + "\n")

    def command(self, cmd: str) -> None:
        with self.write_lock:
            now = time.time()
            wait_s = 0.04 - (now - self.last_write_ts)
            if wait_s > 0:
                time.sleep(wait_s)
            payload = (cmd.strip() + "\n").encode("utf-8", errors="ignore")
            try:
                with self.ser_lock:
                    if not self.ser:
                        raise serial.SerialException("serial port not open")
                    self.ser.write(payload)
                    self.ser.flush()
                self.last_write_ts = time.time()
                self._log(f"{_steady_ts()} > {cmd}")
            except Exception as exc:
                self._log(f"{_steady_ts()} [HOST] write failed: {exc}")
                raise

    def _reader_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                with self.ser_lock:
                    raw = self.ser.readline() if self.ser else b""
            except Exception as exc:
                self.reader_error = str(exc)
                self._log(f"{_steady_ts()} [HOST] read failed: {exc}")
                break
            if not raw:
                continue
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            self._log(f"{_steady_ts()} {line}")
            self.parse_line(line)


class SerialBorrowChargeRunner:
    def __init__(
        self,
        borrower_port: str,
        lender_port: str,
        baud: int,
        borrower_plc_id: int,
        lender_plc_id: int,
        controller_id: int,
        borrower_module: ModuleSpec,
        lender_module: ModuleSpec,
        borrower_log: str,
        lender_log: str,
        summary_file: str,
        borrow_after_s: int,
        shared_duration_s: int,
        post_release_s: int,
        max_total_s: int,
        heartbeat_ms: int,
        auth_ttl_ms: int,
        arm_ms: int,
        assignment_ttl_ms: int,
        relay_settle_s: float,
        release_settle_s: float,
        borrower_power_relay: int,
        lender_bridge_relay: int,
    ) -> None:
        self.borrower_plc_id = borrower_plc_id
        self.lender_plc_id = lender_plc_id
        self.controller_id = controller_id
        self.borrower_module = borrower_module
        self.lender_module = lender_module
        self.summary_file = summary_file
        self.borrower_power_relay = max(1, borrower_power_relay)
        self.lender_bridge_relay = max(1, lender_bridge_relay)
        self.borrow_after_s = max(10, borrow_after_s)
        self.shared_duration_s = max(10, shared_duration_s)
        self.post_release_s = max(0, post_release_s)
        self.max_total_s = max(90, max_total_s)
        self.heartbeat_ms = max(200, heartbeat_ms)
        self.heartbeat_timeout_ms = max(3000, self.heartbeat_ms * 8)
        # Shared USB/UART links carry both controller commands and verbose PLC
        # logs. Keep auth TTL safely above the heartbeat timeout so temporary
        # serial congestion does not expire auth and drop Relay1 mid-session.
        self.auth_ttl_ms = max(6000, auth_ttl_ms, self.heartbeat_timeout_ms + 2000)
        self.arm_ms = max(2000, arm_ms)
        self.assignment_ttl_ms = max(5000, assignment_ttl_ms)
        self.relay_settle_s = max(0.5, relay_settle_s)
        self.release_settle_s = max(0.5, release_settle_s)
        self.voltage_tolerance_v = 15.0

        self.borrower_state = PlcState(name="borrower")
        self.lender_state = PlcState(name="lender")
        self.borrower_lock = threading.RLock()
        self.lender_lock = threading.RLock()

        self.borrower_link = SerialLink(borrower_port, baud, borrower_log, self._parse_borrower_line)
        self.lender_link = SerialLink(lender_port, baud, lender_log, self._parse_lender_line)

        self.stop_sent = False
        self.stop_verify_deadline: Optional[float] = None
        self.auth_granted = False
        self.remote_start_sent = False
        self.phase = "bootstrap"
        self.share_request_at: Optional[float] = None
        self.share_allocation_sent = False
        self.release_request_at: Optional[float] = None
        self.lender_open_sent = False
        self.post_release_started_at: Optional[float] = None
        self.started_at = time.time()
        self.last_borrower_hb = 0.0
        self.last_lender_hb = 0.0
        self.last_auth = 0.0
        self.last_borrower_status = 0.0
        self.last_lender_status = 0.0
        self.last_lender_idle = 0.0
        self.last_arm = 0.0
        self.last_start = 0.0
        self.last_managed_cmd = 0.0
        self.last_progress = 0.0
        self.borrower_allocation_name = ""
        self.cp_monitor_enabled = False
        self.shared_verify_deadline: Optional[float] = None
        self.last_start_recovery_at = time.time()
        self.start_recovery_count = 0
        self.service_stop = threading.Event()
        self.service_thread: Optional[threading.Thread] = None
        self.service_period_s = 0.05
        self.managed_refresh_s = 0.25
        self.advertised_budget_kw = self.borrower_module.limit_kw
        self.transition_current_limit_a: Optional[float] = None
        self.borrow_match_tolerance_v = 20.0
        self.release_current_threshold_a = 1.0
        self.share_prepare_deadline: Optional[float] = None
        self.share_closed_at: Optional[float] = None
        self.release_open_requested_at: Optional[float] = None
        self.release_shrink_deadline: Optional[float] = None
        self.post_release_relax_at: Optional[float] = None
        self.last_relay_refresh = 0.0
        self.relay_refresh_s = 0.8
        self.borrower_relay_closed = False
        self.borrower_cmd_queue: deque[str] = deque()
        self.borrower_cmd_queue_lock = threading.Lock()
        self.borrow_allocation_queued = False
        self.release_allocation_queued = False
        self.bootstrap_wait_status_ts = 0.0
        self.bootstrap_wait_hb_ts = 0.0

    def _with_state(self, lender: bool) -> tuple[PlcState, threading.RLock]:
        if lender:
            return self.lender_state, self.lender_lock
        return self.borrower_state, self.borrower_lock

    def _relay_state(self, lender: bool, relay_idx: int) -> Optional[bool]:
        state, lock = self._with_state(lender)
        with lock:
            return state.relay_states.get(relay_idx)

    def _relay_is_closed(self, lender: bool, relay_idx: int) -> bool:
        return self._relay_state(lender, relay_idx) is True

    def _relay_is_open(self, lender: bool, relay_idx: int) -> bool:
        return self._relay_state(lender, relay_idx) is False

    def _reset_session_state(self, state: PlcState, preserve_identity: bool) -> None:
        state.slac_matched = False
        state.hlc_connected = False
        state.precharge_seen = False
        state.power_delivery_seen = False
        state.current_demand_count = 0
        state.first_current_demand_ts = None
        state.last_current_demand_ts = None
        state.charging_window_start = None
        state.last_hlc_stage = ""
        state.last_ev_target_v = None
        state.last_ev_target_i = None
        state.last_ev_present_v = None
        state.last_ev_present_i = None
        state.last_ev_status = ""
        state.last_ev_request_ts = None
        state.precharge_ready_seen = False
        state.current_demand_ready_seen = False
        state.precharge_ready_count = 0
        state.current_demand_valid_voltage_count = 0
        state.current_demand_ready_count = 0
        if not preserve_identity:
            state.identity_seen = False
            state.identity_source = ""
            state.identity_value = ""

    def _parse_common_line(self, state: PlcState, lock: threading.RLock, line: str) -> None:
        now = time.time()
        with lock:
            was_cp_connected = state.cp_connected

            m_cp = CP_TRANSITION_RE.search(line)
            if m_cp:
                target = m_cp.group(1)
                state.cp_phase = target
                state.cp_connected = target in ("B", "B1", "B2", "C", "D")
                if self.cp_monitor_enabled:
                    if target == "A":
                        state.cp_a_count += 1
                    elif target == "F":
                        state.cp_f_count += 1
                if target == "B1":
                    state.b1_since_ts = now
                elif target != "B1":
                    state.b1_since_ts = None

            m_status = CTRL_STATUS_RE.search(line)
            if m_status:
                state.cp_phase = m_status.group(1)
                try:
                    state.cp_duty_pct = int(m_status.group(2))
                except ValueError:
                    state.cp_duty_pct = None
                m_alloc = STATUS_ALLOC_RE.search(line)
                if m_alloc:
                    try:
                        state.ctrl_alloc_sz = int(m_alloc.group(1))
                    except ValueError:
                        state.ctrl_alloc_sz = None
                m_stop_done = STATUS_STOP_DONE_RE.search(line)
                if m_stop_done:
                    try:
                        state.ctrl_stop_done = int(m_stop_done.group(1)) != 0
                    except ValueError:
                        state.ctrl_stop_done = None
                if state.cp_phase == "B1":
                    if state.b1_since_ts is None:
                        state.b1_since_ts = now
                    state.cp_connected = True
                elif state.cp_phase in ("B2", "C", "D"):
                    state.cp_connected = True
                else:
                    state.cp_connected = False
                    state.b1_since_ts = None

            m_relay = RELAY_CHANGE_RE.search(line)
            if m_relay:
                relay_idx = int(m_relay.group(1))
                closed = m_relay.group(2) == "CLOSED"
                state.relay_states[relay_idx] = closed

            if "client session done rc=" in line:
                state.session_done_count += 1
                state.last_session_done_ts = now
                self._reset_session_state(state, preserve_identity=True)

            if was_cp_connected and not state.cp_connected:
                self._reset_session_state(state, preserve_identity=False)

            if "Relay1 -> OPEN (CtrlPowerTimeout)" in line:
                state.errors.append("CtrlPowerTimeout")
            elif "allowlist apply failed" in line:
                state.errors.append("AllowlistApplyFailed")

            m_grp = MODRT_GRP_RE.search(line)
            if m_grp:
                try:
                    state.group_enable = int(m_grp.group(1))
                    state.group_assigned_modules = int(m_grp.group(2))
                    state.group_active_modules = int(m_grp.group(3))
                    state.group_request_v = float(m_grp.group(4))
                    state.group_request_i = float(m_grp.group(5))
                    state.group_voltage_v = float(m_grp.group(6))
                    state.group_current_a = float(m_grp.group(7))
                    state.group_avail_i = float(m_grp.group(8))
                    if state.group_voltage_v > 5.0:
                        state.last_live_voltage_v = state.group_voltage_v
                        state.last_live_voltage_ts = now
                except ValueError:
                    pass

            m_mod = MODRT_MOD_RE.search(line)
            if m_mod:
                try:
                    module_id = m_mod.group(1)
                    runtime = state.modules.get(module_id)
                    if runtime is None:
                        runtime = ModuleRuntime(module_id=module_id)
                        state.modules[module_id] = runtime
                    runtime.lease_active = int(m_mod.group(2))
                    runtime.off = int(m_mod.group(3))
                    runtime.fault = int(m_mod.group(4))
                    runtime.online = int(m_mod.group(5))
                    runtime.fresh = int(m_mod.group(6))
                    runtime.voltage_v = float(m_mod.group(7))
                    runtime.current_a = float(m_mod.group(8))
                    if runtime.voltage_v > 5.0:
                        state.last_live_voltage_v = runtime.voltage_v
                        state.last_live_voltage_ts = now
                except ValueError:
                    pass

            ms = WH_RE.search(line)
            if ms:
                try:
                    state.last_meter_wh = int(ms.group(1))
                except ValueError:
                    pass
            ss = SOC_RE.search(line)
            if ss:
                try:
                    state.last_soc = int(ss.group(1))
                except ValueError:
                    pass

    def _parse_borrower_line(self, line: str) -> None:
        self._parse_common_line(self.borrower_state, self.borrower_lock, line)
        now = time.time()
        payload = _extract_json_payload(line)

        with self.borrower_lock:
            state = self.borrower_state
            current_demand_seen = False

            if "[SLAC] MATCHED" in line:
                state.slac_matched = True
                state.cp_connected = True
            if "[HLC] EVCC connected" in line:
                state.hlc_connected = True
                state.cp_connected = True
            if "PreChargeReq received" in line:
                state.precharge_seen = True

            m_hlc_rx = HLC_RX_REQ_RE.search(line)
            if m_hlc_rx:
                stage_name = m_hlc_rx.group(1).lower()
                if stage_name == "precharge":
                    state.last_hlc_stage = "precharge"
                    state.precharge_seen = True
                elif stage_name == "powerdelivery":
                    state.last_hlc_stage = "power_delivery"
                    state.power_delivery_seen = True
                elif stage_name == "currentdemand":
                    state.last_hlc_stage = "current_demand"
                    current_demand_seen = True
                state.last_ev_request_ts = now

            if CURRENT_DEMAND_RE.search(line):
                current_demand_seen = True
                state.last_hlc_stage = "current_demand"

            if not state.identity_seen:
                m_mac = EV_MAC_RE.search(line)
                if m_mac:
                    state.identity_seen = True
                    state.identity_source = "EV_MAC"
                    state.identity_value = m_mac.group(1).upper()
                elif payload and str(payload.get("msg", "")).strip().lower() == "sessionsetup":
                    evcc_id = str(payload.get("evccId", "")).strip()
                    if evcc_id:
                        state.identity_seen = True
                        state.identity_source = "EVCCID"
                        state.identity_value = evcc_id.upper()
                else:
                    m_evcc = SESSION_SETUP_EVCCID_RE.search(line)
                    if m_evcc:
                        state.identity_seen = True
                        state.identity_source = "EVCCID"
                        state.identity_value = m_evcc.group(1).upper()

            if isinstance(payload, dict):
                msg = str(payload.get("msg", "")).strip().lower()
                req = payload.get("req") if isinstance(payload.get("req"), dict) else {}
                res = payload.get("res") if isinstance(payload.get("res"), dict) else {}

                if "precharge" in msg:
                    state.last_hlc_stage = "precharge"
                    state.precharge_seen = True
                elif "powerdelivery" in msg:
                    state.last_hlc_stage = "power_delivery"
                    state.power_delivery_seen = True
                elif "currentdemand" in msg:
                    state.last_hlc_stage = "current_demand"
                    current_demand_seen = True

                if state.last_hlc_stage:
                    target_v = _safe_float(req.get("targetV", req.get("targetVoltage")))
                    target_i = _safe_float(req.get("targetI", req.get("targetCurrent")))
                    present_v = _safe_float(res.get("v", res.get("presentV")))
                    present_i = _safe_float(res.get("i", res.get("presentI")))
                    status = str(res.get("status", res.get("evseStatus", ""))).strip()
                    if target_v is not None:
                        state.last_ev_target_v = target_v
                    if target_i is not None:
                        state.last_ev_target_i = target_i
                    if present_v is not None:
                        state.last_ev_present_v = present_v
                    if present_i is not None:
                        state.last_ev_present_i = present_i
                    if status:
                        state.last_ev_status = status
                    state.last_ev_request_ts = now
                    expected_v = _clamp_non_negative(target_v, 0.0)
                    measured_v = _clamp_non_negative(present_v, 0.0)
                    voltage_ok = self._voltage_converged(expected_v, measured_v) if expected_v > 1.0 else False
                    if state.last_hlc_stage == "precharge" and voltage_ok:
                        state.precharge_ready_count += 1
                        if status == "EVSE_Ready":
                            state.precharge_ready_seen = True
                    if state.last_hlc_stage == "current_demand" and voltage_ok:
                        state.current_demand_valid_voltage_count += 1
                        if status == "EVSE_Ready":
                            state.current_demand_ready_seen = True
                            state.current_demand_ready_count += 1

            if current_demand_seen:
                state.current_demand_count += 1
                state.last_current_demand_ts = now
                state.cp_connected = True
                if state.first_current_demand_ts is None:
                    state.first_current_demand_ts = now
                if state.charging_window_start is None:
                    state.charging_window_start = now

    def _parse_lender_line(self, line: str) -> None:
        self._parse_common_line(self.lender_state, self.lender_lock, line)

    def _cmd(self, lender: bool, command: str) -> None:
        if lender:
            self.lender_link.command(command)
        else:
            self.borrower_link.command(command)

    def _start_service_thread(self) -> None:
        if self.service_thread and self.service_thread.is_alive():
            return
        self.service_stop.clear()
        self.service_thread = threading.Thread(target=self._service_loop, daemon=True)
        self.service_thread.start()

    def _stop_service_thread(self) -> None:
        self.service_stop.set()
        if self.service_thread and self.service_thread.is_alive():
            self.service_thread.join(timeout=2.0)
        self.service_thread = None

    def _record_runtime_error(self, lender: bool, message: str) -> None:
        state, lock = self._with_state(lender)
        with lock:
            state.errors.append(message)

    def _service_loop(self) -> None:
        while not self.service_stop.is_set():
            now = time.time()
            try:
                if self.stop_sent:
                    self._tick_stop(now)
                else:
                    self._tick_borrower_contract(now)
                    self._tick_lender_idle(now)
                    self._tick_managed_power(now)
                    self._tick_borrower_queue()
            except Exception as exc:
                self._record_runtime_error(False, f"service loop failed: {exc}")
                break
            self.service_stop.wait(self.service_period_s)

    def _module_with_limit(self, module: ModuleSpec, limit_kw: float) -> ModuleSpec:
        return ModuleSpec(
            module_id=module.module_id,
            can_group=module.can_group,
            can_address=module.can_address,
            limit_kw=max(0.5, limit_kw),
        )

    def _donor_runtime(self) -> Optional[ModuleRuntime]:
        with self.borrower_lock:
            runtime = self.borrower_state.modules.get(self.lender_module.module_id)
        if runtime is not None:
            return runtime
        with self.lender_lock:
            return self.lender_state.modules.get(self.lender_module.module_id)

    def _donor_voltage_matched(self) -> bool:
        runtime = self._donor_runtime()
        if not runtime or runtime.fault or not runtime.online:
            return False
        with self.borrower_lock:
            target_v = _clamp_non_negative(self.borrower_state.last_ev_target_v, 0.0)
        if target_v <= 10.0:
            return False
        return abs(runtime.voltage_v - target_v) <= self.borrow_match_tolerance_v

    def _donor_current_low(self) -> bool:
        runtime = self._donor_runtime()
        if runtime is None:
            return False
        return abs(runtime.current_a) <= self.release_current_threshold_a

    def _measured_voltage(self) -> float:
        with self.borrower_lock:
            group_v = self.borrower_state.group_voltage_v
            live_v = self.borrower_state.last_live_voltage_v
            present_v = self.borrower_state.last_ev_present_v
            target_v = self.borrower_state.last_ev_target_v
        if present_v is not None and present_v > 5.0:
            return present_v
        if group_v is not None and group_v > 5.0:
            return group_v
        if live_v is not None and live_v > 5.0:
            return live_v
        return _clamp_non_negative(target_v, 0.0)

    def _measured_current(self) -> float:
        with self.borrower_lock:
            group_i = self.borrower_state.group_current_a
            present_i = self.borrower_state.last_ev_present_i
        if present_i is not None and present_i >= 0.0:
            return present_i
        return _clamp_non_negative(group_i, 0.0)

    def _home_voltage_stable(self) -> bool:
        with self.borrower_lock:
            target_v = _clamp_non_negative(self.borrower_state.last_ev_target_v, 0.0)
        measured_v = self._measured_voltage()
        if target_v <= 10.0:
            return False
        return self._voltage_converged(target_v, measured_v)

    def _home_path_ready(self) -> bool:
        return (
            self._borrower_assigned_modules() <= 1
            and self._home_module_ready()
            and self._home_voltage_stable()
        )

    def _queue_borrower_commands(self, commands: list[str]) -> None:
        with self.borrower_cmd_queue_lock:
            for command in commands:
                self.borrower_cmd_queue.append(command)

    def _tick_borrower_queue(self) -> None:
        command = ""
        with self.borrower_cmd_queue_lock:
            if self.borrower_cmd_queue:
                command = self.borrower_cmd_queue.popleft()
        if command:
            self._cmd(False, command)

    def _alloc_sync(self, modules: list[ModuleSpec], total_limit_kw: float) -> None:
        if not modules:
            self._cmd(False, "CTRL ALLOC ABORT")
            self._cmd(False, "CTRL STATUS")
            self.borrower_allocation_name = "none"
            return
        txn = random.randint(1, 255)
        self._cmd(False, f"CTRL ALLOC BEGIN {txn} {len(modules)} {self.assignment_ttl_ms} {total_limit_kw:.1f}")
        for module in modules:
            self._cmd(
                False,
                f"CTRL ALLOC DATA {txn} {module.can_group} {module.can_address} {module.limit_kw:.1f}",
            )
        self._cmd(False, f"CTRL ALLOC COMMIT {txn}")
        self._cmd(False, "CTRL STATUS")
        self.borrower_allocation_name = ",".join(module.module_id for module in modules)

    def _alloc_async(self, modules: list[ModuleSpec], total_limit_kw: float) -> None:
        if not modules:
            self._queue_borrower_commands(["CTRL ALLOC ABORT", "CTRL STATUS"])
            self.borrower_allocation_name = "none"
            return
        txn = random.randint(1, 255)
        commands = [
            f"CTRL ALLOC BEGIN {txn} {len(modules)} {self.assignment_ttl_ms} {total_limit_kw:.1f}",
        ]
        for module in modules:
            commands.append(
                f"CTRL ALLOC DATA {txn} {module.can_group} {module.can_address} {module.limit_kw:.1f}"
            )
        commands.append(f"CTRL ALLOC COMMIT {txn}")
        commands.append("CTRL STATUS")
        self._queue_borrower_commands(commands)
        self.borrower_allocation_name = ",".join(module.module_id for module in modules)

    def _set_lender_bridge_relay(self, closed: bool) -> None:
        self._cmd(
            True,
            f"CTRL RELAY {self.lender_bridge_relay} {self.lender_bridge_relay if closed else 0} 0",
        )
        self._cmd(True, "CTRL STATUS")

    def _bootstrap_keepalive(self, now: float) -> None:
        if now - self.bootstrap_wait_hb_ts >= max(0.8, self.heartbeat_ms / 1000.0):
            self._cmd(False, f"CTRL HB {self.heartbeat_timeout_ms}")
            self._cmd(True, f"CTRL HB {self.heartbeat_timeout_ms}")
            self.bootstrap_wait_hb_ts = now
        if now - self.bootstrap_wait_status_ts >= 1.0:
            self._cmd(False, "CTRL STATUS")
            self._cmd(True, "CTRL STATUS")
            self.bootstrap_wait_status_ts = now

    def _wait_until(self, timeout_s: float, predicate: Callable[[], bool], description: str) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if predicate():
                return
            self._bootstrap_keepalive(time.time())
            time.sleep(0.2)
        raise RuntimeError(description)

    def _borrower_module_runtime(self, module_id: str) -> Optional[ModuleRuntime]:
        with self.borrower_lock:
            return self.borrower_state.modules.get(module_id)

    def _home_module_ready(self) -> bool:
        runtime = self._borrower_module_runtime(self.borrower_module.module_id)
        return bool(runtime and runtime.online and runtime.fresh and not runtime.fault)

    def _borrower_assigned_modules(self) -> int:
        with self.borrower_lock:
            if self.borrower_state.group_assigned_modules is not None:
                return self.borrower_state.group_assigned_modules
            return self.borrower_state.ctrl_alloc_sz or 0

    def _bootstrap_local_allocation(self) -> None:
        deadline = time.time() + 25.0
        attempts = 0
        last_error = "borrower local module allocation never committed"
        while time.time() < deadline:
            attempts += 1
            self._alloc_sync([self.borrower_module], self.borrower_module.limit_kw)
            try:
                self._wait_until(
                    3.0,
                    lambda: self._borrower_assigned_modules() >= 1,
                    "borrower local module allocation never committed",
                )
                return
            except RuntimeError as exc:
                last_error = str(exc)
                self._cmd(False, "CTRL ALLOC ABORT")
                self._bootstrap_keepalive(time.time())
                time.sleep(0.5)
        raise RuntimeError(f"{last_error} after {attempts} attempts")

    def _bootstrap(self) -> None:
        borrower_cmds = [
            "CTRL STOP hard 3000",
            "CTRL RESET",
            f"CTRL MODE 2 {self.borrower_plc_id} {self.controller_id}",
            f"CTRL HB {self.heartbeat_timeout_ms}",
            "CTRL STOP clear 3000",
            f"CTRL AUTH deny {self.auth_ttl_ms}",
            "CTRL SLAC disarm 3000",
            "CTRL ALLOC ABORT",
            "CTRL POWER 0 0 0",
            "CTRL FEEDBACK 1 0 0 0",
            f"CTRL RELAY {self.borrower_power_relay} 0 1500",
            "CTRL STATUS",
        ]
        lender_cmds = [
            "CTRL STOP hard 3000",
            "CTRL RESET",
            f"CTRL MODE 1 {self.lender_plc_id} {self.controller_id}",
            f"CTRL HB {self.heartbeat_timeout_ms}",
            "CTRL STOP clear 3000",
            f"CTRL AUTH deny {self.auth_ttl_ms}",
            "CTRL SLAC disarm 3000",
            "CTRL ALLOC ABORT",
            f"CTRL RELAY {self.lender_bridge_relay} 0 0",
            "CTRL STATUS",
        ]
        for cmd in borrower_cmds:
            self._cmd(False, cmd)
            time.sleep(0.08)
        for cmd in lender_cmds:
            self._cmd(True, cmd)
            time.sleep(0.08)
        time.sleep(1.0)
        self._bootstrap_local_allocation()
        self._cmd(False, "CTRL POWER 0 0 0")
        self._cmd(False, "CTRL FEEDBACK 1 0 0 0")
        self.last_managed_cmd = time.time()
        self.phase = "local"
        self.advertised_budget_kw = self.borrower_module.limit_kw
        self.transition_current_limit_a = None
        self.cp_monitor_enabled = True
        self.last_start_recovery_at = time.time()
        self.borrower_relay_closed = False

    def _tick_borrower_contract(self, now: float) -> None:
        if (now - self.last_borrower_hb) * 1000.0 >= self.heartbeat_ms:
            self._cmd(False, f"CTRL HB {self.heartbeat_timeout_ms}")
            self.last_borrower_hb = now

        with self.borrower_lock:
            slac_matched = self.borrower_state.slac_matched
            hlc_connected = self.borrower_state.hlc_connected
            cp_phase = self.borrower_state.cp_phase
            b1_since_ts = self.borrower_state.b1_since_ts
            identity_seen = self.borrower_state.identity_seen

        transition_phase = self.phase not in ("bootstrap", "local", "shared")
        status_interval_s = 2.0 if (not hlc_connected or transition_phase) else 10.0
        if now - self.last_borrower_status >= status_interval_s:
            self._cmd(False, "CTRL STATUS")
            self.last_borrower_status = now

        waiting_for_slac = (not slac_matched) and (not hlc_connected)
        at_b1 = cp_phase == "B1"
        if waiting_for_slac and at_b1 and now - self.last_arm >= 4.0:
            self._cmd(False, f"CTRL SLAC arm {self.arm_ms}")
            self.last_arm = now

        if waiting_for_slac and at_b1 and (not self.remote_start_sent) and b1_since_ts is not None and (now - b1_since_ts) >= 0.6:
            self._cmd(False, f"CTRL SLAC start {self.arm_ms}")
            self.last_start = now
            self.remote_start_sent = True
            print("[CTRL] borrower remote start -> B1 to B2", flush=True)
        elif waiting_for_slac and at_b1 and now - self.last_start >= 4.0:
            self._cmd(False, f"CTRL SLAC start {self.arm_ms}")
            self.last_start = now
        elif not at_b1:
            self.remote_start_sent = False

        if now - self.last_auth >= 1.0:
            if identity_seen:
                self._cmd(False, f"CTRL AUTH grant {self.auth_ttl_ms}")
                self.auth_granted = True
            else:
                self._cmd(False, f"CTRL AUTH pending {self.auth_ttl_ms}")
            self.last_auth = now

    def _tick_lender_idle(self, now: float) -> None:
        if (now - self.last_lender_hb) * 1000.0 >= self.heartbeat_ms:
            self._cmd(True, f"CTRL HB {self.heartbeat_timeout_ms}")
            self.last_lender_hb = now
        if now - self.last_lender_status >= 5.0:
            self._cmd(True, "CTRL STATUS")
            self.last_lender_status = now
        if now - self.last_lender_idle >= 2.0:
            self._cmd(True, f"CTRL AUTH deny {self.auth_ttl_ms}")
            self._cmd(True, "CTRL SLAC disarm 3000")
            self.last_lender_idle = now

    def _effective_stage(self) -> str:
        with self.borrower_lock:
            stage = self.borrower_state.last_hlc_stage
            power_delivery_seen = self.borrower_state.power_delivery_seen
            current_demand_count = self.borrower_state.current_demand_count
            precharge_seen = self.borrower_state.precharge_seen
        if stage:
            return stage
        if current_demand_count > 0:
            return "current_demand"
        if power_delivery_seen:
            return "power_delivery"
        if precharge_seen:
            return "precharge"
        return ""

    def _power_budget_kw(self) -> float:
        return max(0.5, self.advertised_budget_kw)

    def _voltage_converged(self, target_v: float, measured_v: float) -> bool:
        if target_v <= 1.0:
            return False
        return abs(measured_v - target_v) <= self.voltage_tolerance_v

    def _available_current_limit(self, target_v: float) -> float:
        if target_v <= 10.0:
            return 999.0
        limit_a = max(0.0, (self._power_budget_kw() * 1000.0) / target_v)
        if self.transition_current_limit_a is not None:
            limit_a = min(limit_a, max(0.0, self.transition_current_limit_a))
        return limit_a

    def _drive_power_off(self, now: float) -> None:
        self._cmd(False, "CTRL POWER 0 0 0")
        self._cmd(False, "CTRL FEEDBACK 1 0 0 0")
        if self.borrower_relay_closed or (now - self.last_relay_refresh) >= self.relay_refresh_s:
            self._cmd(False, f"CTRL RELAY {self.borrower_power_relay} 0 1500")
            self.borrower_relay_closed = False
            self.last_relay_refresh = now
        self.last_managed_cmd = now

    def _tick_managed_power(self, now: float) -> None:
        if now - self.last_managed_cmd < self.managed_refresh_s:
            return

        stage = self._effective_stage()
        with self.borrower_lock:
            target_v = self.borrower_state.last_ev_target_v
            target_i = self.borrower_state.last_ev_target_i
            last_req_ts = self.borrower_state.last_ev_request_ts
            cp_connected = self.borrower_state.cp_connected

        recent_request = last_req_ts is not None and (now - last_req_ts) <= 8.0
        if (not cp_connected) or (not stage) or (not recent_request):
            self._drive_power_off(now)
            return

        effective_v = _clamp_non_negative(target_v, 400.0)
        requested_i = _clamp_non_negative(target_i, 0.0)
        capped_i = min(requested_i, self._available_current_limit(effective_v)) if requested_i > 0.0 else 0.0
        measured_v = self._measured_voltage()
        measured_i = self._measured_current()
        feedback_v = measured_v if measured_v > 5.0 else effective_v
        voltage_ok = self._voltage_converged(effective_v, feedback_v)
        current_limited = requested_i > (capped_i + 0.1)
        power_limited = (effective_v * requested_i) > ((effective_v * capped_i) + 50.0)

        if stage == "precharge":
            precharge_i = min(2.0, capped_i if capped_i > 0.05 else 2.0)
            self._cmd(False, f"CTRL POWER 1 {effective_v:.1f} {precharge_i:.1f}")
            self._cmd(
                False,
                f"CTRL FEEDBACK 1 {1 if voltage_ok else 0} {feedback_v:.1f} 0.0 0 0 0",
            )
            if self.borrower_relay_closed or (now - self.last_relay_refresh) >= self.relay_refresh_s:
                self._cmd(False, f"CTRL RELAY {self.borrower_power_relay} 0 1500")
                self.borrower_relay_closed = False
                self.last_relay_refresh = now
        elif stage in ("power_delivery", "current_demand"):
            self._cmd(False, f"CTRL POWER 1 {effective_v:.1f} {capped_i:.1f}")
            self._cmd(
                False,
                f"CTRL FEEDBACK 1 {1 if voltage_ok else 0} {feedback_v:.1f} {measured_i:.1f} "
                f"{1 if current_limited else 0} 0 {1 if power_limited else 0}",
            )
            if (not self.borrower_relay_closed) or (now - self.last_relay_refresh) >= self.relay_refresh_s:
                self._cmd(
                    False,
                    f"CTRL RELAY {self.borrower_power_relay} {self.borrower_power_relay} 1500",
                )
                self.borrower_relay_closed = True
                self.last_relay_refresh = now
        self.last_managed_cmd = now

    def _start_borrow(self, now: float) -> None:
        print("[PHASE] borrow requested: allocate donor first, then close lender bridge", flush=True)
        self.phase = "share_alloc_pending"
        self.share_request_at = now
        self.share_closed_at = None
        self.shared_verify_deadline = None
        self.share_prepare_deadline = now + 40.0
        self.advertised_budget_kw = self.borrower_module.limit_kw
        self.transition_current_limit_a = None
        self.borrow_allocation_queued = True
        self._cmd(True, "CTRL ALLOC ABORT")
        self._set_lender_bridge_relay(False)
        self._alloc_async(
            [self.borrower_module, self.lender_module],
            self.borrower_module.limit_kw + self.lender_module.limit_kw,
        )

    def _shared_path_ready(self, active_modules: int) -> bool:
        if not self._home_voltage_stable():
            return False
        if self._donor_voltage_matched():
            return True
        return active_modules >= 2

    def _finish_borrow_if_ready(self, now: float) -> None:
        timed_out = self.share_prepare_deadline is not None and now >= self.share_prepare_deadline
        if self.phase == "share_alloc_pending":
            with self.borrower_lock:
                assigned = self.borrower_state.group_assigned_modules or 0
            if assigned >= 2 and self._home_voltage_stable() and self._donor_voltage_matched():
                self._set_lender_bridge_relay(True)
                self.phase = "share_bridge_prepare"
                print("[PHASE] donor matched in isolation; closing lender bridge", flush=True)
                return
            if timed_out:
                self._record_runtime_error(False, "ShareAllocationTimeout")
            return
        if self.phase != "share_bridge_prepare":
            return
        lender_ready = self._relay_is_closed(True, self.lender_bridge_relay)
        with self.borrower_lock:
            assigned = self.borrower_state.group_assigned_modules or 0
            active = self.borrower_state.group_active_modules or 0
        stable = self._shared_path_ready(active)
        if lender_ready and assigned >= 2 and stable:
            self.phase = "shared"
            self.advertised_budget_kw = self.borrower_module.limit_kw + self.lender_module.limit_kw
            self.transition_current_limit_a = None
            print("[PHASE] shared allocation live: matched donor is now connected", flush=True)
            return
        if timed_out:
            self._record_runtime_error(True, "ShareBridgeCloseTimeout")

    def _recover_start_path(self, now: float) -> None:
        self.start_recovery_count += 1
        self.last_start_recovery_at = now
        self.remote_start_sent = False
        self.last_arm = 0.0
        self.last_start = 0.0
        print(
            f"[RECOVERY] no HLC progress yet; rearming borrower SLAC (count={self.start_recovery_count})",
            flush=True,
        )
        with self.borrower_lock:
            self._reset_session_state(self.borrower_state, preserve_identity=True)
            self.borrower_state.cp_connected = True
        self._cmd(False, "CTRL SLAC disarm 3000")
        self._cmd(False, f"CTRL AUTH pending {self.auth_ttl_ms}")
        self._cmd(False, "CTRL STATUS")

    def _start_release(self, now: float) -> None:
        print("[PHASE] release requested: drop to home budget, then open lender bridge", flush=True)
        self.phase = "release_prepare"
        self.release_request_at = now
        self.lender_open_sent = False
        self.release_open_requested_at = None
        self.release_shrink_deadline = now + max(10.0, self.release_settle_s + 8.0)
        self.post_release_relax_at = None
        self.advertised_budget_kw = self.borrower_module.limit_kw
        self.transition_current_limit_a = None
        self.release_allocation_queued = False

    def _finish_release_if_ready(self, now: float) -> None:
        if self.phase == "release_prepare" and self.release_request_at is not None:
            donor_quiet = self._donor_current_low()
            if donor_quiet or (now - self.release_request_at) >= self.release_settle_s:
                self._set_lender_bridge_relay(False)
                self.lender_open_sent = True
                self.release_open_requested_at = now
                self.phase = "release_bridge_open_pending"
                print("[PHASE] donor current quiet; opening lender bridge", flush=True)
            return
        if self.phase == "release_bridge_open_pending":
            relay_open = self._relay_is_open(True, self.lender_bridge_relay)
            relay_settled = self.release_open_requested_at is not None and (now - self.release_open_requested_at) >= 0.5
            if relay_open and relay_settled and not self.release_allocation_queued:
                self._alloc_async([self.borrower_module], self.borrower_module.limit_kw)
                self.release_allocation_queued = True
                self.phase = "release_shrink_pending"
                print("[PHASE] lender bridge open; shrinking borrower back to home module", flush=True)
                return
            if self.release_shrink_deadline is not None and now >= (self.release_shrink_deadline + 5.0):
                self._record_runtime_error(False, "ReleaseBridgeOpenTimeout")
            return
        if self.phase != "release_shrink_pending":
            return
        relay_open = self._relay_is_open(True, self.lender_bridge_relay)
        if relay_open and self._home_path_ready():
            self.phase = "post_release"
            self.post_release_started_at = now
            self.post_release_relax_at = now + 1.0
            print("[PHASE] borrower stable on home module after release", flush=True)
            return
        if self.release_shrink_deadline is not None and now >= self.release_shrink_deadline:
            self._record_runtime_error(False, "ReleaseShrinkTimeout")

    def _request_stop(self) -> None:
        self.stop_sent = True
        self.phase = "stopping"
        self.stop_verify_deadline = time.time() + 45.0
        self.borrower_allocation_name = "none"
        with self.borrower_cmd_queue_lock:
            self.borrower_cmd_queue.clear()
        self._cmd(False, "CTRL STOP hard 3000")
        self._cmd(False, f"CTRL AUTH deny {self.auth_ttl_ms}")
        self._cmd(False, "CTRL SLAC disarm 3000")
        self._cmd(False, "CTRL ALLOC ABORT")
        self._cmd(False, "CTRL POWER 0 0 0")
        self._cmd(False, "CTRL FEEDBACK 1 0 0 0")
        self._cmd(False, f"CTRL RELAY {self.borrower_power_relay} 0 1500")
        self._cmd(False, "CTRL STATUS")
        self._cmd(True, "CTRL ALLOC ABORT")
        self._cmd(True, f"CTRL AUTH deny {self.auth_ttl_ms}")
        self._cmd(True, "CTRL SLAC disarm 3000")
        self._set_lender_bridge_relay(False)

    def _tick_stop(self, now: float) -> None:
        if (now - self.last_borrower_hb) * 1000.0 >= self.heartbeat_ms:
            self._cmd(False, f"CTRL HB {self.heartbeat_timeout_ms}")
            self.last_borrower_hb = now
        if (now - self.last_lender_hb) * 1000.0 >= self.heartbeat_ms:
            self._cmd(True, f"CTRL HB {self.heartbeat_timeout_ms}")
            self.last_lender_hb = now
        if now - self.last_borrower_status >= 1.0:
            self._cmd(False, "CTRL STATUS")
            self.last_borrower_status = now
        if now - self.last_lender_status >= 1.0:
            self._cmd(True, "CTRL STATUS")
            self.last_lender_status = now
        if now - self.last_auth >= 1.5:
            self._cmd(False, f"CTRL AUTH deny {self.auth_ttl_ms}")
            self._cmd(True, f"CTRL AUTH deny {self.auth_ttl_ms}")
            self.last_auth = now
        if now - self.last_arm >= 1.5:
            self._cmd(False, "CTRL SLAC disarm 3000")
            self._cmd(True, "CTRL SLAC disarm 3000")
            self.last_arm = now
        if now - self.last_lender_idle >= 2.0:
            self._cmd(False, "CTRL ALLOC ABORT")
            self._cmd(True, "CTRL ALLOC ABORT")
            self._set_lender_bridge_relay(False)
            self.last_lender_idle = now
        if now - self.last_managed_cmd >= self.managed_refresh_s:
            self._drive_power_off(now)

    def _stop_verification_ok(self) -> bool:
        with self.borrower_lock:
            borrower_power_closed = self.borrower_state.relay_states.get(self.borrower_power_relay)
            ctrl_alloc_sz = self.borrower_state.ctrl_alloc_sz
            ctrl_stop_done = self.borrower_state.ctrl_stop_done
            group_disabled = (self.borrower_state.group_enable == 0) if self.borrower_state.group_enable is not None else False
            no_assigned = (self.borrower_state.group_assigned_modules == 0) if self.borrower_state.group_assigned_modules is not None else False
            no_active = (self.borrower_state.group_active_modules == 0) if self.borrower_state.group_active_modules is not None else False
        with self.lender_lock:
            lender_bridge_closed = self.lender_state.relay_states.get(self.lender_bridge_relay)
        borrower_power_open = borrower_power_closed is False
        lender_bridge_open = lender_bridge_closed is False
        ctrl_ok = (ctrl_alloc_sz == 0) and (ctrl_stop_done is True)
        modrt_ok = group_disabled and no_assigned and no_active
        return borrower_power_open and lender_bridge_open and (ctrl_ok or modrt_ok)

    def _write_summary(self, result: str) -> None:
        with self.borrower_lock:
            borrower_modules = {key: vars(value) for key, value in self.borrower_state.modules.items()}
            borrower = vars(self.borrower_state).copy()
            borrower["modules"] = borrower_modules
        with self.lender_lock:
            lender_modules = {key: vars(value) for key, value in self.lender_state.modules.items()}
            lender = vars(self.lender_state).copy()
            lender["modules"] = lender_modules
        payload = {
            "result": result,
            "phase": self.phase,
            "allocation": self.borrower_allocation_name,
            "borrower_power_relay": self.borrower_power_relay,
            "lender_bridge_relay": self.lender_bridge_relay,
            "borrower": borrower,
            "lender": lender,
        }
        _ensure_parent_dir(self.summary_file)
        with open(self.summary_file, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, indent=2)

    def _check_link_health(self) -> Optional[str]:
        if self.borrower_link.reader_error:
            return f"borrower serial reader failed: {self.borrower_link.reader_error}"
        if self.lender_link.reader_error:
            return f"lender serial reader failed: {self.lender_link.reader_error}"
        return None

    def run(self) -> int:
        result = "fail"
        try:
            self.borrower_link.open()
            self.lender_link.open()
            print(f"BORROWER_LOG={self.borrower_link.log_file}", flush=True)
            print(f"LENDER_LOG={self.lender_link.log_file}", flush=True)
            print(f"SUMMARY_FILE={self.summary_file}", flush=True)

            self._bootstrap()
            self._start_service_thread()
            t0 = time.time()
            while True:
                now = time.time()
                if (now - t0 > self.max_total_s) and (not self.stop_sent):
                    print("[FAIL] max test time exceeded", flush=True)
                    break

                link_error = self._check_link_health()
                if link_error:
                    print(f"[FAIL] {link_error}", flush=True)
                    self._record_runtime_error(False, link_error)
                    break

                with self.borrower_lock:
                    borrower_errors = list(self.borrower_state.errors)
                    charge_started_at = self.borrower_state.charging_window_start
                    charge_elapsed = (now - charge_started_at) if charge_started_at is not None else 0.0
                    cp_a = self.borrower_state.cp_a_count
                    cp_f = self.borrower_state.cp_f_count
                    hlc_connected = self.borrower_state.hlc_connected
                    cp_connected = self.borrower_state.cp_connected
                    requested_v = self.borrower_state.last_ev_target_v
                    requested_i = self.borrower_state.last_ev_target_i
                    present_v = self.borrower_state.last_ev_present_v
                    status = self.borrower_state.last_ev_status
                    cd_count = self.borrower_state.current_demand_count
                    cp_phase = self.borrower_state.cp_phase
                    duty = self.borrower_state.cp_duty_pct
                    assigned = self.borrower_state.group_assigned_modules
                    active = self.borrower_state.group_active_modules
                with self.lender_lock:
                    lender_errors = list(self.lender_state.errors)
                    relay_state = self.lender_state.relay_states.get(self.lender_bridge_relay)
                    relay_closed_val = -1 if relay_state is None else int(relay_state)

                if borrower_errors or lender_errors:
                    print(f"[FAIL] serial/runtime errors borrower={borrower_errors} lender={lender_errors}", flush=True)
                    break
                if cp_a > 0 and not self.stop_sent:
                    print("[FAIL] borrower CP transitioned to state A before stop sequence completed", flush=True)
                    break
                if cp_f > 0 and not self.stop_sent:
                    print("[FAIL] borrower CP transitioned to state F", flush=True)
                    break

                if (
                    not self.stop_sent
                    and charge_started_at is None
                    and (not hlc_connected)
                    and cp_connected
                    and cd_count == 0
                    and (now - self.last_start_recovery_at) >= 45.0
                ):
                    self._recover_start_path(now)

                if not self.stop_sent:
                    if self.phase == "local" and charge_started_at is not None and charge_elapsed >= self.borrow_after_s:
                        self._start_borrow(now)
                    elif self.phase in ("share_bridge_prepare", "share_alloc_pending"):
                        self._finish_borrow_if_ready(now)
                    elif self.phase == "shared" and charge_started_at is not None and charge_elapsed >= (self.borrow_after_s + self.shared_duration_s):
                        self._start_release(now)
                    elif self.phase in ("release_prepare", "release_shrink_pending", "release_bridge_open_pending"):
                        self._finish_release_if_ready(now)
                    elif self.phase == "post_release" and self.post_release_started_at is not None and (now - self.post_release_started_at) >= self.post_release_s:
                        print("[PHASE] post-release window complete; stopping charge", flush=True)
                        self._request_stop()

                    if (
                        self.phase == "post_release"
                        and self.transition_current_limit_a is not None
                        and self.post_release_relax_at is not None
                        and now >= self.post_release_relax_at
                        and self._home_voltage_stable()
                    ):
                        self.transition_current_limit_a = None
                        self.post_release_relax_at = None
                        print(
                            "[PHASE] post-release local module stable; restoring home-module current budget",
                            flush=True,
                        )

                if now - self.last_progress >= 5.0:
                    print(
                        f"[PROGRESS] phase={self.phase} cpPhase={cp_phase} duty={duty if duty is not None else -1} "
                        f"charge_s={charge_elapsed:.1f} relayClosed={relay_closed_val} assigned={assigned if assigned is not None else -1} "
                        f"active={active if active is not None else -1} reqV={requested_v if requested_v is not None else -1:.1f} "
                        f"reqI={requested_i if requested_i is not None else -1:.1f} presentV={present_v if present_v is not None else -1:.1f} "
                        f"status={status or '-'} cd={cd_count} cpA={cp_a} cpF={cp_f} budgetKW={self._power_budget_kw():.1f} "
                        f"q={len(self.borrower_cmd_queue)}",
                        flush=True,
                    )
                    self.last_progress = now

                if self.stop_sent:
                    if self._stop_verification_ok():
                        result = "pass"
                        print("[PASS] stop verification OK: borrower idle, lender bridge open", flush=True)
                        break
                    if self.stop_verify_deadline is not None and now > self.stop_verify_deadline:
                        print("[FAIL] stop verification timeout", flush=True)
                        break

                time.sleep(0.05)
        except Exception as exc:
            print(f"[FAIL] unhandled exception: {exc}", flush=True)
        finally:
            self._stop_service_thread()
            try:
                self.borrower_link.close()
            finally:
                self.lender_link.close()
            try:
                self._write_summary(result)
            except Exception as exc:
                print(f"[WARN] failed to write summary: {exc}", flush=True)
            print(f"SUMMARY_FILE_DONE={self.summary_file}", flush=True)
        return 0 if result == "pass" else 1


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Serial-only controller-managed borrow/release charging simulator")
    ap.add_argument("--borrower-port", default="/dev/ttyACM0")
    ap.add_argument("--lender-port", default="/dev/ttyACM1")
    ap.add_argument("--baud", type=int, default=115200)
    ap.add_argument("--borrower-plc-id", type=int, default=1)
    ap.add_argument("--lender-plc-id", type=int, default=2)
    ap.add_argument("--controller-id", type=int, default=1)
    ap.add_argument("--borrower-module-id", default="PLC1_MXR_01")
    ap.add_argument("--borrower-module-group", type=int, default=1)
    ap.add_argument("--borrower-module-addr", type=int, default=1)
    ap.add_argument("--borrower-module-kw", type=float, default=15.0)
    ap.add_argument("--lender-module-id", default="LEASED_MXR_03")
    ap.add_argument("--lender-module-group", type=int, default=1)
    ap.add_argument("--lender-module-addr", type=int, default=3)
    ap.add_argument("--lender-module-kw", type=float, default=15.0)
    ap.add_argument("--borrow-after-s", type=int, default=180)
    ap.add_argument("--shared-duration-s", type=int, default=300)
    ap.add_argument("--post-release-s", type=int, default=60)
    ap.add_argument("--max-total-min", type=int, default=20)
    ap.add_argument("--heartbeat-ms", type=int, default=800)
    ap.add_argument("--auth-ttl-ms", type=int, default=3000)
    ap.add_argument("--arm-ms", type=int, default=12000)
    ap.add_argument("--assignment-ttl-ms", type=int, default=900000)
    ap.add_argument("--relay-settle-s", type=float, default=1.5)
    ap.add_argument("--release-settle-s", type=float, default=2.0)
    ap.add_argument(
        "--borrower-power-relay",
        type=int,
        default=1,
        help="borrower-side output relay mask used for managed power switching (default: 1 for Relay1)",
    )
    ap.add_argument(
        "--lender-bridge-relay",
        type=int,
        default=2,
        help="lender-side bridge relay; borrow/release flow closes and opens Relay2 by default",
    )
    ap.add_argument("--borrower-log", default="")
    ap.add_argument("--lender-log", default="")
    ap.add_argument("--summary-file", default="")
    return ap.parse_args()


def main() -> int:
    print(f"[ERR] {LEGACY_MODE2_ERROR}", file=sys.stderr)
    return 2
    args = parse_args()
    if args.borrower_port == args.lender_port:
        print("[ERR] --borrower-port and --lender-port must differ", file=sys.stderr)
        return 2

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    borrower_log = args.borrower_log or f"/home/jpi/Desktop/EVSE/plc_firmware/logs/serial_borrow_charge_{ts}_borrower.log"
    lender_log = args.lender_log or f"/home/jpi/Desktop/EVSE/plc_firmware/logs/serial_borrow_charge_{ts}_lender.log"
    summary_file = args.summary_file or f"/home/jpi/Desktop/EVSE/plc_firmware/logs/serial_borrow_charge_{ts}_summary.json"

    runner = SerialBorrowChargeRunner(
        borrower_port=args.borrower_port,
        lender_port=args.lender_port,
        baud=args.baud,
        borrower_plc_id=args.borrower_plc_id,
        lender_plc_id=args.lender_plc_id,
        controller_id=args.controller_id,
        borrower_module=ModuleSpec(
            module_id=args.borrower_module_id,
            can_group=args.borrower_module_group,
            can_address=args.borrower_module_addr,
            limit_kw=args.borrower_module_kw,
        ),
        lender_module=ModuleSpec(
            module_id=args.lender_module_id,
            can_group=args.lender_module_group,
            can_address=args.lender_module_addr,
            limit_kw=args.lender_module_kw,
        ),
        borrower_log=borrower_log,
        lender_log=lender_log,
        summary_file=summary_file,
        borrow_after_s=args.borrow_after_s,
        shared_duration_s=args.shared_duration_s,
        post_release_s=args.post_release_s,
        max_total_s=max(90, args.max_total_min * 60),
        heartbeat_ms=args.heartbeat_ms,
        auth_ttl_ms=args.auth_ttl_ms,
        arm_ms=args.arm_ms,
        assignment_ttl_ms=args.assignment_ttl_ms,
        relay_settle_s=args.relay_settle_s,
        release_settle_s=args.release_settle_s,
        borrower_power_relay=args.borrower_power_relay,
        lender_bridge_relay=args.lender_bridge_relay,
    )
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
