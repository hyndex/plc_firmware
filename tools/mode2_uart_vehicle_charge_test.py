#!/usr/bin/env python3
"""
Live vehicle end-to-end UART-router charging test with direct Maxwell CAN.

Important mode note:
  - The requested "controller-controlled / UART-only" architecture is the
    current firmware `mode=2` (`controller_uart_router`), not legacy `mode=3`.
  - This harness intentionally boots both PLCs into `mode=2` and fails if the
    PLC reports `can_stack=1` or `module_mgr=1`.

Architecture under test:
  - Controller -> borrower PLC over UART for CP / SLAC / HLC / feedback / Relay1
  - Controller -> donor PLC over UART for donor bridge relay only
  - Controller -> Maxwell modules over CAN directly

Sequence:
  1. Put borrower and donor PLCs into `mode=2/controller_uart_router`.
  2. Verify both PLCs report `can_stack=0` and `module_mgr=0`.
  3. Discover module `1/group1` and donor module `3/group2` on external CAN.
  4. Soft-reset both modules to known-good defaults.
  5. Drive borrower session over UART: HB, AUTH, SLAC, FEEDBACK, STATUS.
  6. Follow EV-driven `PreCharge -> PowerDelivery -> CurrentDemand`.
  7. Start with home module only.
  8. After 40 s of CurrentDemand, precharge donor in isolation, close bridge,
     and share current across the two modules without bus-voltage collapse.
  9. After 40 s shared, ramp back to home-only, open donor bridge, and stop
     donor output cleanly.
 10. Hold home-only for the final 40 s, then stop the session and verify
     relays/modules are off.
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
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Deque, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

try:
    from mode2_serial_module_transition_test import (
        ACK_CMD_RELAY,
        ACK_CMD_STOP,
        ACK_OK,
        Ack,
        DirectMaxwellCan,
        PhaseMetrics,
        PlcStatus,
        SerialLink,
        ensure_parent,
        module_current_value,
        module_is_off,
        module_status_text,
        module_voltage_value,
        mxr,
    )
except Exception as exc:
    print(f"[ERR] failed importing transition-test helpers: {exc}", file=sys.stderr)
    sys.exit(2)

ACK_CMD_HEARTBEAT = 0x10
ACK_CMD_AUTH = 0x11
ACK_CMD_SLAC = 0x12
ACK_CMD_FEEDBACK = 0x1A


STATUS_RE = re.compile(
    r"\[SERCTRL\] STATUS mode=(\d+)\(([^)]+)\)\s+plc_id=(\d+)\s+connector_id=(\d+)\s+"
    r"controller_id=(\d+)\s+module_addr=0x([0-9A-Fa-f]+)\s+local_group=(\d+)\s+module_id=([^\s]+)\s+"
    r"can_stack=(\d)\s+module_mgr=(\d)\s+cp=([A-Z0-9]+)\s+duty=(\d+)\s+hb=(\d+)\s+auth=(\d+)\s+"
    r"allow_slac=(\d+)\s+allow_energy=(\d+)\s+armed=(\d+)\s+start=(\d+)\s+"
    r"relay1=(\d+)\s+relay2=(\d+)\s+relay3=(\d+)\s+alloc_sz=(\d+)"
)
ACK_RE = re.compile(
    r"\[SERCTRL\] ACK cmd=0x([0-9A-Fa-f]+)\s+seq=(\d+)\s+status=(\d+)\s+detail0=(\d+)\s+detail1=(\d+)"
)
RELAY_EVT_RE = re.compile(r"\[RELAY\] Relay(\d+) -> (OPEN|CLOSED)")
CP_TRANSITION_RE = re.compile(r"\[CP\] state .* -> ([A-F](?:[12])?)")
CP_EVT_RE = re.compile(
    r"\[SERCTRL\] EVT CP cp=([A-Z0-9]+)\s+duty=(\d+)\s+connected=(\d)\s+hb=(\d)\s+auth=(\d+)\s+"
    r"allow_slac=(\d)\s+allow_energy=(\d)\s+armed=(\d)\s+start=(\d)"
)
SLAC_EVT_RE = re.compile(r"\[SERCTRL\] EVT SLAC session=(\d)\s+matched=(\d)\s+fsm=(\d+)\s+failures=(\d+)")
HLC_EVT_RE = re.compile(
    r"\[SERCTRL\] EVT HLC ready=(\d)\s+active=(\d)\s+auth=(\d+)\s+hb=(\d)\s+precharge_seen=(\d)\s+"
    r"precharge_ready=(\d)\s+fb_valid=(\d)\s+fb_ready=(\d)\s+fb_v=([0-9.+-]+)\s+fb_i=([0-9.+-]+)"
)
SESSION_EVT_RE = re.compile(
    r"\[SERCTRL\] EVT SESSION session=(\d)\s+matched=(\d)\s+relay1=(\d)\s+relay2=(\d)\s+relay3=(\d)\s+"
    r"stop_active=(\d)\s+stop_hard=(\d)\s+stop_done=(\d)\s+meter_wh=(\d+)\s+soc=(\d+)"
)
BMS_EVT_RE = re.compile(
    r"\[SERCTRL\] EVT BMS stage=(\d+)\(([^)]+)\)\s+valid=(\d)\s+delivery_ready=(\d)\s+"
    r"target_v=([0-9.+-]+)\s+target_i=([0-9.+-]+)\s+fb_valid=(\d)\s+fb_ready=(\d)\s+"
    r"present_v=([0-9.+-]+)\s+present_i=([0-9.+-]+)\s+curr_lim=(\d)\s+volt_lim=(\d)\s+pwr_lim=(\d)"
)
IDENTITY_EVT_RE = re.compile(
    r"\[SERCTRL\] IDENTITY kind=([A-Z0-9_]+)\s+plc_id=(\d+)\s+connector_id=(\d+)\s+value=([0-9A-Fa-f]+)"
)
HLC_REQ_RE = re.compile(r"RX (PowerDeliveryReq|WeldingDetectionReq|SessionStopReq) \(proto=(\d+)\)")


def ts() -> str:
    return dt.datetime.now().isoformat(timespec="milliseconds")


def safe_float(value: object, fallback: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return fallback
    if not math.isfinite(parsed):
        return fallback
    return parsed


def clamp_non_negative(value: object, fallback: float = 0.0) -> float:
    return max(0.0, safe_float(value, fallback))


def normalize_stage_name(raw: str) -> str:
    text = str(raw or "").strip().lower().replace(" ", "").replace("_", "")
    if text == "precharge":
        return "precharge"
    if text == "powerdelivery":
        return "power_delivery"
    if text == "currentdemand":
        return "current_demand"
    return text


@dataclass
class PlcLiveState:
    name: str
    last_status: Optional[PlcStatus] = None
    relay_states: dict[int, bool] = field(default_factory=dict)
    mode_id: int = -1
    mode_name: str = ""
    can_stack: int = -1
    module_mgr: int = -1
    cp_phase: str = "U"
    cp_connected: bool = False
    cp_duty_pct: Optional[int] = None
    b1_since_ts: Optional[float] = None
    slac_session: bool = False
    slac_matched: bool = False
    slac_failures: int = 0
    hlc_ready: bool = False
    hlc_active: bool = False
    precharge_seen: bool = False
    precharge_ready: bool = False
    session_started: bool = False
    stop_active: bool = False
    stop_hard: bool = False
    stop_done: bool = False
    meter_wh: int = 0
    soc: int = 0
    bms_stage_id: int = 0
    bms_stage_name: str = ""
    bms_valid: bool = False
    delivery_ready: bool = False
    target_v: float = 0.0
    target_i: float = 0.0
    fb_valid: bool = False
    fb_ready: bool = False
    fb_v: float = 0.0
    fb_i: float = 0.0
    fb_curr_lim: bool = False
    fb_volt_lim: bool = False
    fb_pwr_lim: bool = False
    last_bms_ts: Optional[float] = None
    first_current_demand_ts: Optional[float] = None
    identity_seen: bool = False
    identity_source: str = ""
    identity_value: str = ""
    power_delivery_req_count: int = 0
    welding_req_count: int = 0
    session_stop_req_count: int = 0
    last_power_delivery_req_ts: Optional[float] = None
    last_welding_req_ts: Optional[float] = None
    last_session_stop_req_ts: Optional[float] = None
    errors: list[str] = field(default_factory=list)


@dataclass
class ControlPlan:
    enabled: bool = False
    voltage_v: float = 0.0
    current_a: float = 0.0


@dataclass
class SummaryModule:
    address: Optional[int]
    group: Optional[int]
    rated_current_a: Optional[float]
    status: Optional[str]
    voltage_v: Optional[float]
    current_a: Optional[float]


class VehicleInitiatedStop(RuntimeError):
    pass


class VehicleChargeRunner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.borrower = PlcLiveState("borrower")
        self.donor = PlcLiveState("donor")
        self.borrower_lock = threading.RLock()
        self.donor_lock = threading.RLock()

        self.status_queues: dict[str, Deque[PlcStatus]] = {"borrower": deque(), "donor": deque()}
        self.ack_queues: dict[str, Deque[Ack]] = {"borrower": deque(), "donor": deque()}

        self.borrower_link = SerialLink(args.borrower_port, args.baud, args.borrower_log, self._parse_borrower_line)
        self.donor_link = SerialLink(args.donor_port, args.baud, args.donor_log, self._parse_donor_line)
        self.can = DirectMaxwellCan(args.can_iface, args.can_log, args.absolute_current_scale)

        self.home_module: Optional[mxr.ModuleInfo] = None
        self.donor_module: Optional[mxr.ModuleInfo] = None

        self.phase = "bootstrap"
        self.phase_started_at = time.time()
        self.phase_metrics: dict[str, PhaseMetrics] = {}
        self.events: list[str] = []
        self.failures: list[str] = []
        self.notes: list[str] = []
        self.capability_clamps: list[str] = []
        self.capability_warnings: list[str] = []
        self.status_miss_counts = {"borrower": 0, "donor": 0}

        self.home_single_limit_a = 0.0
        self.home_shared_limit_a = 0.0
        self.donor_shared_limit_a = 0.0
        self.donor_bootstrap_limit_a = 0.0
        self.donor_release_hold_a = 0.0

        self.current_plan = {"home": ControlPlan(), "donor": ControlPlan()}
        self.plan_active = {"home": False, "donor": False}
        self.last_set_refresh = {"home": 0.0, "donor": 0.0}
        self.desired_relays: dict[tuple[str, int], bool] = {
            ("borrower", self.args.borrower_power_relay): False,
            ("donor", self.args.donor_bridge_relay): False,
        }
        self.last_relay_refresh: dict[tuple[str, int], float] = {}
        self.last_feedback_cmd = 0.0
        self.last_progress = 0.0
        self.last_borrower_hb = 0.0
        self.last_donor_hb = 0.0
        self.last_borrower_auth = 0.0
        self.last_donor_idle = 0.0
        self.last_borrower_status = 0.0
        self.last_donor_status = 0.0
        self.last_arm = 0.0
        self.last_start = 0.0
        self.remote_start_sent = False
        self.last_telemetry_poll = 0.0
        self.last_start_recovery_at = time.time()
        self.start_recovery_count = 0

        self.current_demand_started_at: Optional[float] = None
        self.session_protocol_baseline: tuple[int, int, int] = (0, 0, 0)
        self.attach_done = False
        self.release_done = False
        self.stop_requested = False
        self.stop_started_at: Optional[float] = None
        self.stop_hard_sent = False
        self.protocol_stop_verified = False
        self.vehicle_stop_requested = False
        self.vehicle_stop_reason = ""
        self.vehicle_stop_seen_at: Optional[float] = None

    def _state(self, plc_name: str) -> tuple[PlcLiveState, threading.RLock]:
        if plc_name == "donor":
            return self.donor, self.donor_lock
        return self.borrower, self.borrower_lock

    def _link(self, plc_name: str) -> SerialLink:
        return self.donor_link if plc_name == "donor" else self.borrower_link

    def _parse_status(self, line: str) -> Optional[PlcStatus]:
        match = STATUS_RE.search(line)
        if not match:
            return None
        return PlcStatus(
            mode_id=int(match.group(1)),
            mode_name=match.group(2),
            plc_id=int(match.group(3)),
            connector_id=int(match.group(4)),
            controller_id=int(match.group(5)),
            module_addr=int(match.group(6), 16),
            local_group=int(match.group(7)),
            module_id=match.group(8),
            can_stack=int(match.group(9)),
            module_mgr=int(match.group(10)),
            cp=match.group(11),
            duty=int(match.group(12)),
            relay1=int(match.group(19)),
            relay2=int(match.group(20)),
            relay3=int(match.group(21)),
            alloc_sz=int(match.group(22)),
        )

    def _parse_line_common(self, plc_name: str, line: str) -> None:
        state, lock = self._state(plc_name)
        now = time.time()

        status = self._parse_status(line)
        if status is not None:
            with lock:
                state.last_status = status
                state.mode_id = status.mode_id
                state.mode_name = status.mode_name
                state.can_stack = status.can_stack
                state.module_mgr = status.module_mgr
                state.cp_phase = status.cp
                state.cp_duty_pct = status.duty
                state.cp_connected = status.cp in ("B", "B1", "B2", "C", "D")
                if status.cp == "B1":
                    if state.b1_since_ts is None:
                        state.b1_since_ts = now
                elif status.cp != "B1":
                    state.b1_since_ts = None
                state.relay_states[1] = status.relay1 != 0
                state.relay_states[2] = status.relay2 != 0
                state.relay_states[3] = status.relay3 != 0
            self.status_queues[plc_name].append(status)
            return

        ack_match = ACK_RE.search(line)
        if ack_match:
            self.ack_queues[plc_name].append(
                Ack(
                    cmd_hex=int(ack_match.group(1), 16),
                    seq=int(ack_match.group(2)),
                    status=int(ack_match.group(3)),
                    detail0=int(ack_match.group(4)),
                    detail1=int(ack_match.group(5)),
                )
            )
            return

        relay_match = RELAY_EVT_RE.search(line)
        if relay_match:
            relay_idx = int(relay_match.group(1))
            closed = relay_match.group(2) == "CLOSED"
            with lock:
                state.relay_states[relay_idx] = closed

        cp_match = CP_TRANSITION_RE.search(line)
        if cp_match:
            cp_phase = cp_match.group(1)
            with lock:
                state.cp_phase = cp_phase
                state.cp_connected = cp_phase in ("B", "B1", "B2", "C", "D")
                if cp_phase == "B1":
                    state.b1_since_ts = now
                elif cp_phase != "B1":
                    state.b1_since_ts = None

        cp_evt = CP_EVT_RE.search(line)
        if cp_evt:
            with lock:
                state.cp_phase = cp_evt.group(1)
                state.cp_duty_pct = int(cp_evt.group(2))
                state.cp_connected = cp_evt.group(3) == "1"
                if state.cp_phase == "B1":
                    if state.b1_since_ts is None:
                        state.b1_since_ts = now
                elif state.cp_phase != "B1":
                    state.b1_since_ts = None
            return

        slac_evt = SLAC_EVT_RE.search(line)
        if slac_evt:
            with lock:
                state.slac_session = slac_evt.group(1) == "1"
                state.slac_matched = slac_evt.group(2) == "1"
                state.slac_failures = int(slac_evt.group(4))
            return

        hlc_evt = HLC_EVT_RE.search(line)
        if hlc_evt:
            with lock:
                state.hlc_ready = hlc_evt.group(1) == "1"
                state.hlc_active = hlc_evt.group(2) == "1"
                state.precharge_seen = hlc_evt.group(5) == "1"
                state.precharge_ready = hlc_evt.group(6) == "1"
                state.fb_valid = hlc_evt.group(7) == "1"
                state.fb_ready = hlc_evt.group(8) == "1"
                state.fb_v = safe_float(hlc_evt.group(9))
                state.fb_i = safe_float(hlc_evt.group(10))
            return

        session_evt = SESSION_EVT_RE.search(line)
        if session_evt:
            with lock:
                state.session_started = session_evt.group(1) == "1"
                state.slac_matched = session_evt.group(2) == "1"
                state.relay_states[1] = session_evt.group(3) == "1"
                state.relay_states[2] = session_evt.group(4) == "1"
                state.relay_states[3] = session_evt.group(5) == "1"
                state.stop_active = session_evt.group(6) == "1"
                state.stop_hard = session_evt.group(7) == "1"
                state.stop_done = session_evt.group(8) == "1"
                state.meter_wh = int(session_evt.group(9))
                state.soc = int(session_evt.group(10))
            return

        bms_evt = BMS_EVT_RE.search(line)
        if bms_evt:
            stage_name = normalize_stage_name(bms_evt.group(2))
            with lock:
                state.bms_stage_id = int(bms_evt.group(1))
                state.bms_stage_name = stage_name
                state.bms_valid = bms_evt.group(3) == "1"
                state.delivery_ready = bms_evt.group(4) == "1"
                state.target_v = clamp_non_negative(bms_evt.group(5))
                state.target_i = clamp_non_negative(bms_evt.group(6))
                state.fb_valid = bms_evt.group(7) == "1"
                state.fb_ready = bms_evt.group(8) == "1"
                state.fb_v = clamp_non_negative(bms_evt.group(9))
                state.fb_i = clamp_non_negative(bms_evt.group(10))
                state.fb_curr_lim = bms_evt.group(11) == "1"
                state.fb_volt_lim = bms_evt.group(12) == "1"
                state.fb_pwr_lim = bms_evt.group(13) == "1"
                state.last_bms_ts = now
                if stage_name == "current_demand" and state.first_current_demand_ts is None:
                    state.first_current_demand_ts = now
            return

        identity_evt = IDENTITY_EVT_RE.search(line)
        if identity_evt:
            with lock:
                state.identity_seen = True
                state.identity_source = identity_evt.group(1)
                state.identity_value = identity_evt.group(4).upper()
            return

        hlc_req = HLC_REQ_RE.search(line)
        if hlc_req:
            kind = hlc_req.group(1)
            with lock:
                if kind == "PowerDeliveryReq":
                    state.power_delivery_req_count += 1
                    state.last_power_delivery_req_ts = now
                elif kind == "WeldingDetectionReq":
                    state.welding_req_count += 1
                    state.last_welding_req_ts = now
                elif kind == "SessionStopReq":
                    state.session_stop_req_count += 1
                    state.last_session_stop_req_ts = now
            if plc_name == "borrower" and self.current_demand_started_at is not None and not self.stop_requested:
                self.vehicle_stop_requested = True
                self.vehicle_stop_reason = kind
                if self.vehicle_stop_seen_at is None:
                    self.vehicle_stop_seen_at = now

    def _parse_borrower_line(self, line: str) -> None:
        self._parse_line_common("borrower", line)

    def _parse_donor_line(self, line: str) -> None:
        self._parse_line_common("donor", line)

    def _send(self, plc_name: str, cmd: str) -> None:
        self._link(plc_name).command(cmd)

    def _wait_for_status(self, plc_name: str, timeout_s: float = 2.0) -> PlcStatus:
        deadline = time.time() + timeout_s
        queue = self.status_queues[plc_name]
        while time.time() < deadline:
            if queue:
                return queue.popleft()
            link = self._link(plc_name)
            if link.reader_error:
                raise RuntimeError(f"{plc_name} serial reader failed: {link.reader_error}")
            time.sleep(0.02)
        raise RuntimeError(f"timeout waiting for {plc_name} status")

    def _wait_for_ack(self, plc_name: str, cmd_hex: int, timeout_s: float = 5.0) -> Ack:
        deadline = time.time() + timeout_s
        queue = self.ack_queues[plc_name]
        while time.time() < deadline:
            while queue:
                ack = queue.popleft()
                if ack.cmd_hex == cmd_hex:
                    return ack
            link = self._link(plc_name)
            if link.reader_error:
                raise RuntimeError(f"{plc_name} serial reader failed: {link.reader_error}")
            time.sleep(0.02)
        raise RuntimeError(f"timeout waiting for {plc_name} ack 0x{cmd_hex:02X}")

    def _query_status(self, plc_name: str) -> PlcStatus:
        self._send(plc_name, "CTRL STATUS")
        return self._wait_for_status(plc_name)

    def _poll_status_best_effort(self, plc_name: str) -> None:
        try:
            self._query_status(plc_name)
            self.status_miss_counts[plc_name] = 0
        except RuntimeError as exc:
            self.status_miss_counts[plc_name] += 1
            miss_count = self.status_miss_counts[plc_name]
            print(f"[WARN] {plc_name} status poll miss {miss_count}: {exc}", flush=True)
            if miss_count >= self.args.max_status_misses:
                raise RuntimeError(f"{plc_name} status repeatedly missing ({miss_count} misses)")

    def _set_relay(self, plc_name: str, relay_idx: int, closed: bool, hold_ms: int, require_ack: bool = True) -> None:
        mask = 1 << (relay_idx - 1)
        state_mask = mask if closed else 0
        self._send(plc_name, f"CTRL RELAY {mask} {state_mask} {max(0, hold_ms)}")
        if require_ack:
            ack = self._wait_for_ack(plc_name, ACK_CMD_RELAY)
            if ack.status != ACK_OK:
                raise RuntimeError(
                    f"{plc_name} Relay{relay_idx} rejected status={ack.status} "
                    f"detail0={ack.detail0} detail1={ack.detail1}"
                )

    def _relay_is_closed(self, plc_name: str, relay_idx: int) -> bool:
        state, lock = self._state(plc_name)
        with lock:
            return state.relay_states.get(relay_idx) is True

    def _relay_is_open(self, plc_name: str, relay_idx: int) -> bool:
        state, lock = self._state(plc_name)
        with lock:
            return state.relay_states.get(relay_idx) is False

    def _bootstrap_plc(self, plc_name: str, plc_id: int) -> PlcStatus:
        self._send(plc_name, "CTRL STATUS")
        status = self._wait_for_status(plc_name)
        self._send(plc_name, "CTRL STOP hard 3000")
        ack = self._wait_for_ack(plc_name, ACK_CMD_STOP)
        if ack.status != ACK_OK:
            raise RuntimeError(f"{plc_name} STOP hard rejected status={ack.status}")
        self._send(plc_name, "CTRL RESET")
        time.sleep(0.2)
        if status.mode_id != 2 or status.controller_id != self.args.controller_id:
            self._send(plc_name, f"CTRL MODE 2 {plc_id} {self.args.controller_id}")
            time.sleep(0.3)
        self._send(plc_name, f"CTRL HB {self.args.heartbeat_timeout_ms}")
        ack = self._wait_for_ack(plc_name, ACK_CMD_HEARTBEAT)
        if ack.status != ACK_OK:
            raise RuntimeError(f"{plc_name} HB rejected status={ack.status}")
        self._send(plc_name, "CTRL STOP clear 3000")
        ack = self._wait_for_ack(plc_name, ACK_CMD_STOP)
        if ack.status != ACK_OK:
            raise RuntimeError(f"{plc_name} STOP clear rejected status={ack.status}")
        self._send(plc_name, f"CTRL AUTH deny {self.args.auth_ttl_ms}")
        ack = self._wait_for_ack(plc_name, ACK_CMD_AUTH)
        if ack.status != ACK_OK:
            raise RuntimeError(f"{plc_name} AUTH deny rejected status={ack.status}")
        self._send(plc_name, "CTRL SLAC disarm 3000")
        ack = self._wait_for_ack(plc_name, ACK_CMD_SLAC)
        if ack.status != ACK_OK:
            raise RuntimeError(f"{plc_name} SLAC disarm rejected status={ack.status}")
        self._send(plc_name, "CTRL FEEDBACK 1 0 0 0 0 0 0")
        ack = self._wait_for_ack(plc_name, ACK_CMD_FEEDBACK)
        if ack.status != ACK_OK:
            raise RuntimeError(f"{plc_name} FEEDBACK reset rejected status={ack.status}")
        for relay_idx in (1, 2, 3):
            self._set_relay(plc_name, relay_idx, False, 0)
        status = self._query_status(plc_name)
        if status.mode_id != 2:
            raise RuntimeError(f"{plc_name} did not enter mode=2 controller_uart_router")
        if status.can_stack != 0 or status.module_mgr != 0:
            raise RuntimeError(
                f"{plc_name} mode2 routing mismatch: can_stack={status.can_stack} module_mgr={status.module_mgr}"
            )
        return status

    def _effective_current(self, requested_a: float, rated_a: Optional[float], label: str) -> float:
        rated = rated_a if rated_a and rated_a > 0.0 else self.args.default_rated_current_a
        if not self.args.enforce_rated_current:
            if rated > 0.0 and requested_a > (rated + 0.05):
                note = f"{label} exceeds reported rated current {rated:.1f} A; using requested {requested_a:.1f} A"
                self.capability_warnings.append(note)
                print(f"[WARN] {note}", flush=True)
            return max(0.0, requested_a)
        effective = min(max(0.0, requested_a), max(0.0, rated))
        if effective + 0.05 < requested_a:
            note = f"{label} clipped from {requested_a:.1f} A to rated {effective:.1f} A"
            self.capability_clamps.append(note)
            print(f"[WARN] {note}", flush=True)
        return effective

    def _compute_limits(self) -> None:
        if not self.home_module or not self.donor_module:
            raise RuntimeError("modules not loaded")
        self.home_single_limit_a = self._effective_current(
            self.args.single_home_current_a, self.home_module.rated_current_a, "home_single"
        )
        self.home_shared_limit_a = self._effective_current(
            self.args.shared_home_current_a, self.home_module.rated_current_a, "home_shared"
        )
        self.donor_shared_limit_a = self._effective_current(
            self.args.shared_donor_current_a, self.donor_module.rated_current_a, "donor_shared"
        )
        self.donor_bootstrap_limit_a = min(
            self.args.donor_bootstrap_current_a,
            self.donor_shared_limit_a if self.donor_shared_limit_a > 0.0 else self.args.donor_bootstrap_current_a,
        )
        self.donor_release_hold_a = min(
            self.args.donor_release_hold_a,
            self.donor_shared_limit_a if self.donor_shared_limit_a > 0.0 else self.args.donor_release_hold_a,
        )

    def _set_phase(self, name: str) -> None:
        if self.phase == name:
            return
        self.phase = name
        self.phase_started_at = time.time()
        self.phase_metrics.setdefault(name, PhaseMetrics(name=name))
        self.events.append(f"{ts()} phase={name}")
        print(f"[PHASE] {name}", flush=True)

    def _borrower_stage_snapshot(self) -> tuple[str, float, float, Optional[float], bool]:
        with self.borrower_lock:
            stage_name = self.borrower.bms_stage_name
            stage_ts = self.borrower.last_bms_ts
            target_v = self.borrower.target_v
            target_i = self.borrower.target_i
            precharge_seen = self.borrower.precharge_seen
        if stage_ts is not None and (time.time() - stage_ts) <= self.args.request_fresh_s:
            normalized = normalize_stage_name(stage_name)
            if normalized in ("precharge", "power_delivery", "current_demand"):
                return normalized, target_v, target_i, stage_ts, True
        if precharge_seen and self.current_demand_started_at is None:
            return "precharge", target_v, target_i, stage_ts, False
        return "", target_v, target_i, stage_ts, False

    def _vehicle_stop_pending(self) -> bool:
        return self.vehicle_stop_requested and not self.stop_requested

    def _target_voltage(self) -> float:
        stage, target_v, _target_i, _ts, _fresh = self._borrower_stage_snapshot()
        if stage and target_v > 10.0:
            return target_v
        return self.args.target_voltage_v

    def _requested_current(self) -> float:
        stage, _target_v, target_i, _ts, _fresh = self._borrower_stage_snapshot()
        if stage in ("power_delivery", "current_demand"):
            return max(0.0, target_i)
        return 0.0

    def _single_home_current(self, requested_i: float) -> float:
        if requested_i <= 0.0:
            return 0.0
        return min(requested_i, self.home_single_limit_a)

    def _shared_currents(self, requested_i: float) -> tuple[float, float, float]:
        total_limit = max(0.0, self.home_shared_limit_a) + max(0.0, self.donor_shared_limit_a)
        if requested_i <= 0.0 or total_limit <= 0.0:
            return 0.0, 0.0, total_limit
        capped_total = min(requested_i, total_limit)
        ratio = self.home_shared_limit_a / total_limit if total_limit > 0.0 else 0.5
        home = min(self.home_shared_limit_a, capped_total * ratio)
        donor = min(self.donor_shared_limit_a, capped_total - home)
        leftover = capped_total - (home + donor)
        if leftover > 0.01:
            home_extra = min(leftover, max(0.0, self.home_shared_limit_a - home))
            home += home_extra
            leftover -= home_extra
            donor += min(leftover, max(0.0, self.donor_shared_limit_a - donor))
        return max(0.0, home), max(0.0, donor), total_limit

    def _actual_attached_modules(self) -> list[mxr.ModuleInfo]:
        modules: list[mxr.ModuleInfo] = []
        borrower_closed = self._relay_is_closed("borrower", self.args.borrower_power_relay)
        donor_closed = self._relay_is_closed("donor", self.args.donor_bridge_relay)
        if borrower_closed and self.home_module:
            modules.append(self.home_module)
        if borrower_closed and donor_closed and self.donor_module:
            modules.append(self.donor_module)
        return modules

    def _present_voltage_estimate(self) -> float:
        attached = self._actual_attached_modules()
        voltages = [module_voltage_value(module) for module in attached if module_voltage_value(module) > 5.0]
        if voltages:
            return min(voltages)
        if self.home_module:
            home_v = module_voltage_value(self.home_module)
            if home_v > 5.0:
                return home_v
        if self.donor_module:
            donor_v = module_voltage_value(self.donor_module)
            if donor_v > 5.0:
                return donor_v
        return 0.0

    def _bus_voltage_estimate(self) -> float:
        attached = self._actual_attached_modules()
        voltages = [module_voltage_value(module) for module in attached if module_voltage_value(module) > 5.0]
        if not voltages:
            return 0.0
        return min(voltages)

    def _present_current_estimate(self) -> float:
        if not self._relay_is_closed("borrower", self.args.borrower_power_relay):
            return 0.0
        total = 0.0
        for module in self._actual_attached_modules():
            total += module_current_value(module)
        return total

    def _record_metrics(self) -> None:
        metrics = self.phase_metrics.setdefault(self.phase, PhaseMetrics(name=self.phase))
        home_v = module_voltage_value(self.home_module) if self.home_module else 0.0
        donor_v = module_voltage_value(self.donor_module) if self.donor_module else 0.0
        metrics.observe(
            bus_v=self._bus_voltage_estimate(),
            home_v=home_v,
            donor_v=donor_v,
            total_i=self._present_current_estimate(),
        )
        if time.time() - self.phase_started_at < self.args.transition_grace_s:
            return
        threshold = 0.0
        if self.phase in ("home_only", "post_release", "release_ramp"):
            threshold = self.args.min_home_bus_voltage_v
        elif self.phase in ("share_prepare", "share_ramp", "shared"):
            threshold = self.args.min_shared_bus_voltage_v
        if threshold > 0.0 and self._relay_is_closed("borrower", self.args.borrower_power_relay):
            bus_v = self._bus_voltage_estimate()
            if bus_v > 0.0 and bus_v < threshold:
                metrics.low_voltage_events += 1
                msg = f"{self.phase} low bus voltage {bus_v:.1f} V < {threshold:.1f} V"
                if metrics.low_voltage_events == 1 and self.args.fail_on_low_bus_voltage:
                    self.failures.append(msg)
                metrics.notes.append(msg)

    def _check_runtime_mode(self) -> None:
        for plc_name in ("borrower", "donor"):
            state, lock = self._state(plc_name)
            with lock:
                mode_id = state.mode_id
                can_stack = state.can_stack
                module_mgr = state.module_mgr
            if mode_id not in (-1, 2):
                raise RuntimeError(f"{plc_name} left mode=2 and entered mode={mode_id}")
            if can_stack not in (-1, 0):
                raise RuntimeError(f"{plc_name} reported can_stack={can_stack}; PLC CAN must stay OFF in UART mode")
            if module_mgr not in (-1, 0):
                raise RuntimeError(f"{plc_name} reported module_mgr={module_mgr}; PLC module manager must stay OFF")

    def _tick_borrower_contract(self, now: float) -> None:
        if (now - self.last_borrower_hb) * 1000.0 >= self.args.heartbeat_ms:
            self._send("borrower", f"CTRL HB {self.args.heartbeat_timeout_ms}")
            self.last_borrower_hb = now

        if self.stop_requested or self._vehicle_stop_pending():
            if now - self.last_borrower_status >= self.args.status_interval_s:
                self._send("borrower", "CTRL STATUS")
                self.last_borrower_status = now
            if now - self.last_borrower_auth >= 1.0:
                self._send("borrower", f"CTRL AUTH deny {self.args.auth_ttl_ms}")
                self.last_borrower_auth = now
            if now - self.last_arm >= 1.5:
                self._send("borrower", "CTRL SLAC disarm 3000")
                self.last_arm = now
            return

        with self.borrower_lock:
            slac_matched = self.borrower.slac_matched
            hlc_active = self.borrower.hlc_active
            cp_phase = self.borrower.cp_phase
            b1_since_ts = self.borrower.b1_since_ts
            identity_seen = self.borrower.identity_seen
            precharge_seen = self.borrower.precharge_seen

        if now - self.last_borrower_status >= self.args.status_interval_s:
            self._send("borrower", "CTRL STATUS")
            self.last_borrower_status = now

        auth_state = "grant" if (identity_seen or slac_matched or hlc_active or precharge_seen) else "pending"
        if now - self.last_borrower_auth >= 1.0:
            self._send("borrower", f"CTRL AUTH {auth_state} {self.args.auth_ttl_ms}")
            self.last_borrower_auth = now

        waiting_for_slac = (not slac_matched) and (not hlc_active)
        at_b1 = cp_phase == "B1"
        if waiting_for_slac and at_b1 and (now - self.last_arm) >= 4.0:
            self._send("borrower", f"CTRL SLAC arm {self.args.arm_ms}")
            self.last_arm = now

        if waiting_for_slac and at_b1 and (not self.remote_start_sent) and b1_since_ts is not None and (now - b1_since_ts) >= 0.6:
            self._send("borrower", f"CTRL SLAC start {self.args.arm_ms}")
            self.last_start = now
            self.remote_start_sent = True
            print("[CTRL] borrower remote start -> B1 to B2", flush=True)
        elif waiting_for_slac and at_b1 and (now - self.last_start) >= 4.0:
            self._send("borrower", f"CTRL SLAC start {self.args.arm_ms}")
            self.last_start = now
        elif not at_b1:
            self.remote_start_sent = False

    def _tick_donor_idle(self, now: float) -> None:
        if (now - self.last_donor_hb) * 1000.0 >= self.args.heartbeat_ms:
            self._send("donor", f"CTRL HB {self.args.heartbeat_timeout_ms}")
            self.last_donor_hb = now
        if now - self.last_donor_status >= max(5.0, self.args.status_interval_s):
            self._send("donor", "CTRL STATUS")
            self.last_donor_status = now
        if now - self.last_donor_idle >= 2.0:
            self._send("donor", f"CTRL AUTH deny {self.args.auth_ttl_ms}")
            self._send("donor", "CTRL SLAC disarm 3000")
            self.last_donor_idle = now

    def _tick_relays(self, now: float) -> None:
        for plc_name, relay_idx in (
            ("borrower", self.args.borrower_power_relay),
            ("donor", self.args.donor_bridge_relay),
        ):
            desired = self.desired_relays[(plc_name, relay_idx)]
            last = self.last_relay_refresh.get((plc_name, relay_idx), 0.0)
            if desired:
                if (now - last) >= self.args.relay_refresh_s:
                    self._set_relay(plc_name, relay_idx, True, self.args.relay_hold_ms, require_ack=False)
                    self.last_relay_refresh[(plc_name, relay_idx)] = now
            else:
                if not self._relay_is_open(plc_name, relay_idx) or (now - last) >= 0.8:
                    self._set_relay(plc_name, relay_idx, False, 0, require_ack=False)
                    self.last_relay_refresh[(plc_name, relay_idx)] = now

    def _tick_module_outputs(self, now: float) -> None:
        if self.home_module:
            plan = self.current_plan["home"]
            if plan.enabled and (now - self.last_set_refresh["home"]) >= self.args.control_interval_s:
                self.can.set_output(self.home_module, plan.voltage_v, plan.current_a)
                self.last_set_refresh["home"] = now
                self.plan_active["home"] = True
            elif (not plan.enabled) and self.plan_active["home"]:
                self.can.stop_output(self.home_module)
                self.plan_active["home"] = False

        if self.donor_module:
            plan = self.current_plan["donor"]
            if plan.enabled and (now - self.last_set_refresh["donor"]) >= self.args.control_interval_s:
                self.can.set_output(self.donor_module, plan.voltage_v, plan.current_a)
                self.last_set_refresh["donor"] = now
                self.plan_active["donor"] = True
            elif (not plan.enabled) and self.plan_active["donor"]:
                self.can.stop_output(self.donor_module)
                self.plan_active["donor"] = False

    def _tick_feedback(self, now: float, stage: str, target_v: float, requested_i: float) -> None:
        if (now - self.last_feedback_cmd) < self.args.feedback_interval_s:
            return
        if stage == "precharge":
            present_v = self._present_voltage_estimate()
            ready = target_v > 10.0 and abs(present_v - target_v) <= self.args.feedback_voltage_tolerance_v
            self._send(
                "borrower",
                f"CTRL FEEDBACK 1 {1 if ready else 0} {present_v:.1f} 0.0 0 0 0",
            )
        elif stage in ("power_delivery", "current_demand"):
            present_v = self._present_voltage_estimate()
            present_i = self._present_current_estimate()
            power_path_active = self._relay_is_closed("borrower", self.args.borrower_power_relay)
            ready = target_v > 10.0 and power_path_active
            if self.attach_done and not self.release_done:
                _home_i, _donor_i, total_cap = self._shared_currents(requested_i)
            else:
                total_cap = self.home_single_limit_a
            current_limited = requested_i > (total_cap + 0.1)
            power_limited = current_limited
            self._send(
                "borrower",
                f"CTRL FEEDBACK 1 {1 if ready else 0} {present_v:.1f} {present_i:.1f} "
                f"{1 if current_limited else 0} 0 {1 if power_limited else 0}",
            )
        else:
            self._send("borrower", "CTRL FEEDBACK 1 0 0 0 0 0 0")
        self.last_feedback_cmd = now

    def _tick_protocol_stop_feedback(self, now: float) -> None:
        if (now - self.last_feedback_cmd) < self.args.feedback_interval_s:
            return
        present_v = self._present_voltage_estimate()
        present_i = self._present_current_estimate()
        power_path_active = self._relay_is_closed("borrower", self.args.borrower_power_relay) and present_v > 10.0
        self._send(
            "borrower",
            f"CTRL FEEDBACK 1 {1 if power_path_active else 0} {present_v:.1f} "
            f"{present_i if power_path_active else 0.0:.1f} 0 0 0 1",
        )
        self.last_feedback_cmd = now

    def _tick_telemetry(self, now: float) -> None:
        if (now - self.last_telemetry_poll) < self.args.telemetry_interval_s:
            return
        if self.home_module:
            self.can.refresh(self.home_module, response_window_s=self.args.telemetry_response_s)
        if self.donor_module:
            self.can.refresh(self.donor_module, response_window_s=self.args.telemetry_response_s)
        self._record_metrics()
        self.last_telemetry_poll = now

    def _tick_progress(self, now: float, stage: str) -> None:
        if (now - self.last_progress) < 5.0:
            return
        requested_i = self._requested_current()
        charge_elapsed = -1.0 if self.current_demand_started_at is None else (now - self.current_demand_started_at)
        home_v = module_voltage_value(self.home_module) if self.home_module else -1.0
        home_i = module_current_value(self.home_module) if self.home_module else -1.0
        donor_v = module_voltage_value(self.donor_module) if self.donor_module else -1.0
        donor_i = module_current_value(self.donor_module) if self.donor_module else -1.0
        print(
            f"[PROGRESS] phase={self.phase} stage={stage or '-'} charge_s={charge_elapsed:.1f} "
            f"relay1={int(self._relay_is_closed('borrower', self.args.borrower_power_relay))} "
            f"relay2={int(self._relay_is_closed('donor', self.args.donor_bridge_relay))} "
            f"targetV={self._target_voltage():.1f} targetI={requested_i:.1f} "
            f"busV={self._present_voltage_estimate():.1f} busI={self._present_current_estimate():.1f} "
            f"homeV={home_v:.1f} homeI={home_i:.1f} "
            f"donorV={donor_v:.1f} donorI={donor_i:.1f}",
            flush=True,
        )
        self.last_progress = now

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
            self.borrower.slac_session = False
            self.borrower.slac_matched = False
            self.borrower.hlc_active = False
            self.borrower.precharge_seen = False
            self.borrower.precharge_ready = False
            self.borrower.bms_stage_id = 0
            self.borrower.bms_stage_name = ""
            self.borrower.bms_valid = False
            self.borrower.delivery_ready = False
            self.borrower.target_v = 0.0
            self.borrower.target_i = 0.0
            self.borrower.last_bms_ts = None
        self._send("borrower", "CTRL STOP hard 3000")
        ack = self._wait_for_ack("borrower", ACK_CMD_STOP)
        if ack.status != ACK_OK:
            raise RuntimeError(f"borrower STOP hard rejected during recovery status={ack.status}")
        self._send("borrower", "CTRL RESET")
        time.sleep(0.2)
        self._send("borrower", f"CTRL HB {self.args.heartbeat_timeout_ms}")
        ack = self._wait_for_ack("borrower", ACK_CMD_HEARTBEAT)
        if ack.status != ACK_OK:
            raise RuntimeError(f"borrower HB rejected during recovery status={ack.status}")
        self._send("borrower", "CTRL STOP clear 3000")
        ack = self._wait_for_ack("borrower", ACK_CMD_STOP)
        if ack.status != ACK_OK:
            raise RuntimeError(f"borrower STOP clear rejected during recovery status={ack.status}")
        self._send("borrower", f"CTRL AUTH deny {self.args.auth_ttl_ms}")
        ack = self._wait_for_ack("borrower", ACK_CMD_AUTH)
        if ack.status != ACK_OK:
            raise RuntimeError(f"borrower AUTH deny rejected during recovery status={ack.status}")
        self._send("borrower", "CTRL SLAC disarm 3000")
        ack = self._wait_for_ack("borrower", ACK_CMD_SLAC)
        if ack.status != ACK_OK:
            raise RuntimeError(f"borrower SLAC disarm rejected during recovery status={ack.status}")
        self._send("borrower", "CTRL FEEDBACK 1 0 0 0 0 0 0")
        ack = self._wait_for_ack("borrower", ACK_CMD_FEEDBACK)
        if ack.status != ACK_OK:
            raise RuntimeError(f"borrower FEEDBACK reset rejected during recovery status={ack.status}")
        self._send("borrower", "CTRL STATUS")

    def _service_cycle(self, stage: str, target_v: float, requested_i: float) -> None:
        now = time.time()
        self._check_runtime_mode()
        self._tick_borrower_contract(now)
        self._tick_donor_idle(now)
        if self._vehicle_stop_pending():
            self.current_plan["home"] = ControlPlan(False, 0.0, 0.0)
            self.current_plan["donor"] = ControlPlan(False, 0.0, 0.0)
            self.desired_relays[("borrower", self.args.borrower_power_relay)] = False
            self.desired_relays[("donor", self.args.donor_bridge_relay)] = False
            self._tick_module_outputs(now)
            self._tick_protocol_stop_feedback(now)
            self._tick_relays(now)
        else:
            self._tick_relays(now)
            self._tick_module_outputs(now)
            self._tick_feedback(now, stage, target_v, requested_i)
        self._tick_telemetry(now)
        self._tick_progress(now, "stop_request" if self._vehicle_stop_pending() else stage)

    def _wait_until(self, timeout_s: float, predicate, description: str, stage: str, target_v: float, requested_i: float) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if self._vehicle_stop_pending():
                raise VehicleInitiatedStop(self.vehicle_stop_reason or "vehicle initiated stop")
            if predicate():
                return
            self._service_cycle(stage, target_v, requested_i)
            time.sleep(0.05)
        raise RuntimeError(description)

    def _donor_voltage_matched(self, target_v: float) -> bool:
        if not self.donor_module or module_is_off(self.donor_module):
            return False
        donor_v = module_voltage_value(self.donor_module)
        return donor_v > 10.0 and abs(donor_v - target_v) <= self.args.match_tolerance_v

    def _bus_continuity_threshold(self, baseline_v: float, max_dip_v: float) -> float:
        if baseline_v > 10.0:
            return max(self.args.collapse_bus_voltage_v, baseline_v - max(0.0, max_dip_v))
        return self.args.collapse_bus_voltage_v

    def _bus_continuity_ok(self, baseline_v: float, max_dip_v: float) -> bool:
        bus_v = self._bus_voltage_estimate()
        if bus_v <= 0.0:
            return False
        return bus_v >= self._bus_continuity_threshold(baseline_v, max_dip_v)

    def _shared_path_engaged(self, baseline_bus_v: float, donor_current_floor_a: float) -> bool:
        if not self.home_module or not self.donor_module:
            return False
        home_v = module_voltage_value(self.home_module)
        donor_v = module_voltage_value(self.donor_module)
        donor_i = module_current_value(self.donor_module)
        bus_v = self._bus_voltage_estimate()
        if home_v <= 10.0 or donor_v <= 10.0 or bus_v <= 10.0:
            return False
        if not self._bus_continuity_ok(baseline_bus_v, self.args.attach_max_bus_dip_v):
            return False
        voltage_window = max(self.args.match_tolerance_v, 20.0)
        return (
            abs(home_v - donor_v) <= voltage_window
            and abs(donor_v - bus_v) <= voltage_window
            and donor_i >= max(0.0, donor_current_floor_a)
        )

    def _enter_idle_output(self) -> None:
        self.current_plan["home"] = ControlPlan(False, 0.0, 0.0)
        self.current_plan["donor"] = ControlPlan(False, 0.0, 0.0)
        self.desired_relays[("borrower", self.args.borrower_power_relay)] = False
        self.desired_relays[("donor", self.args.donor_bridge_relay)] = False

    def _apply_precharge_plan(self, target_v: float) -> None:
        self.current_plan["home"] = ControlPlan(True, target_v, self.args.precharge_current_a)
        self.current_plan["donor"] = ControlPlan(False, 0.0, 0.0)
        self.desired_relays[("borrower", self.args.borrower_power_relay)] = False
        self.desired_relays[("donor", self.args.donor_bridge_relay)] = False

    def _apply_home_only_plan(self, target_v: float, requested_i: float, post_release: bool = False) -> None:
        current_a = self._single_home_current(requested_i)
        self.current_plan["home"] = ControlPlan(True, target_v, current_a)
        self.current_plan["donor"] = ControlPlan(False, 0.0, 0.0)
        self.desired_relays[("borrower", self.args.borrower_power_relay)] = True
        self.desired_relays[("donor", self.args.donor_bridge_relay)] = False
        self._set_phase("post_release" if post_release else "home_only")

    def _apply_shared_plan(self, target_v: float, requested_i: float) -> None:
        home_i, donor_i, _total_cap = self._shared_currents(requested_i)
        self.current_plan["home"] = ControlPlan(True, target_v, home_i)
        self.current_plan["donor"] = ControlPlan(True, target_v, donor_i)
        self.desired_relays[("borrower", self.args.borrower_power_relay)] = True
        self.desired_relays[("donor", self.args.donor_bridge_relay)] = True
        self._set_phase("shared")

    def _execute_attach_transition(self) -> None:
        if self.attach_done:
            return
        target_v = self._target_voltage()
        requested_i = self._requested_current()
        single_home_i = self._single_home_current(requested_i)
        shared_home_i, shared_donor_i, _total_cap = self._shared_currents(requested_i)
        baseline_bus_v = self._bus_voltage_estimate()
        donor_bootstrap_i = min(
            self.donor_bootstrap_limit_a if self.donor_bootstrap_limit_a > 0.0 else self.args.donor_bootstrap_current_a,
            max(shared_donor_i, self.args.donor_bootstrap_current_a),
        )
        donor_current_floor_a = min(
            max(0.5, self.args.attach_min_donor_current_a),
            max(donor_bootstrap_i, shared_donor_i if shared_donor_i > 0.0 else donor_bootstrap_i),
        )
        settle_current_floor_a = min(0.5, donor_current_floor_a)

        self._set_phase("share_prepare")
        print("[PHASE] donor isolated precharge before bus attach", flush=True)
        self.current_plan["home"] = ControlPlan(True, target_v, single_home_i)
        self.current_plan["donor"] = ControlPlan(True, target_v, donor_bootstrap_i)
        self.desired_relays[("borrower", self.args.borrower_power_relay)] = True
        self.desired_relays[("donor", self.args.donor_bridge_relay)] = False
        self._wait_until(
            self.args.attach_timeout_s,
            lambda: self._donor_voltage_matched(target_v),
            "donor module failed isolated voltage match before attach",
            "current_demand",
            target_v,
            requested_i,
        )

        self.desired_relays[("donor", self.args.donor_bridge_relay)] = True
        self._wait_until(
            self.args.attach_timeout_s,
            lambda: self._relay_is_closed("donor", self.args.donor_bridge_relay),
            "donor bridge relay did not close",
            "current_demand",
            target_v,
            requested_i,
        )

        settle_started = time.time()
        settle_deadline = settle_started + self.args.attach_timeout_s
        while time.time() < settle_deadline:
            if self._vehicle_stop_pending():
                raise VehicleInitiatedStop(self.vehicle_stop_reason or "vehicle initiated stop")
            self._service_cycle("current_demand", target_v, requested_i)
            if (time.time() - settle_started) >= self.args.attach_settle_s:
                if not self._bus_continuity_ok(baseline_bus_v, self.args.attach_max_bus_dip_v):
                    threshold_v = self._bus_continuity_threshold(baseline_bus_v, self.args.attach_max_bus_dip_v)
                    raise RuntimeError(
                        f"donor attach collapsed bus continuity: {self._bus_voltage_estimate():.1f} V < {threshold_v:.1f} V"
                    )
                if self._shared_path_engaged(baseline_bus_v, settle_current_floor_a):
                    break
            time.sleep(0.05)
        else:
            raise RuntimeError("donor bridge closed but shared current did not engage")

        self._set_phase("share_ramp")
        start_home = single_home_i
        start_donor = donor_bootstrap_i
        ramp_started = time.time()
        ramp_deadline = ramp_started + max(0.2, self.args.share_ramp_s)
        verify_deadline = ramp_deadline + self.args.attach_timeout_s
        while time.time() < verify_deadline:
            now = time.time()
            if self._vehicle_stop_pending():
                raise VehicleInitiatedStop(self.vehicle_stop_reason or "vehicle initiated stop")
            progress = 1.0 if now >= ramp_deadline else ((now - ramp_started) / max(0.2, self.args.share_ramp_s))
            home_cmd = start_home + ((shared_home_i - start_home) * progress)
            donor_cmd = start_donor + ((shared_donor_i - start_donor) * progress)
            self.current_plan["home"] = ControlPlan(True, target_v, max(0.0, home_cmd))
            self.current_plan["donor"] = ControlPlan(True, target_v, max(0.0, donor_cmd))
            self._service_cycle("current_demand", target_v, requested_i)
            donor_actual = module_current_value(self.donor_module)
            if not self._bus_continuity_ok(baseline_bus_v, self.args.attach_max_bus_dip_v):
                threshold_v = self._bus_continuity_threshold(baseline_bus_v, self.args.attach_max_bus_dip_v)
                raise RuntimeError(
                    f"share ramp bus dip exceeded continuity limit: {self._bus_voltage_estimate():.1f} V < {threshold_v:.1f} V"
                )
            if (
                progress >= 1.0
                and donor_actual >= donor_current_floor_a
                and self._shared_path_engaged(baseline_bus_v, donor_current_floor_a)
            ):
                break
            time.sleep(0.05)
        else:
            raise RuntimeError("share ramp did not stabilize")

        self.current_plan["home"] = ControlPlan(True, target_v, shared_home_i)
        self.current_plan["donor"] = ControlPlan(True, target_v, shared_donor_i)
        self.attach_done = True
        self._set_phase("shared")

    def _execute_release_transition(self) -> None:
        if self.release_done:
            return
        target_v = self._target_voltage()
        requested_i = self._requested_current()
        shared_home_i, shared_donor_i, _total_cap = self._shared_currents(requested_i)
        single_home_i = self._single_home_current(requested_i)

        self._set_phase("release_ramp")
        ramp_started = time.time()
        ramp_deadline = ramp_started + max(0.2, self.args.release_ramp_s)
        donor_quiet_deadline = ramp_deadline + self.args.release_timeout_s
        while time.time() < donor_quiet_deadline:
            now = time.time()
            if self._vehicle_stop_pending():
                raise VehicleInitiatedStop(self.vehicle_stop_reason or "vehicle initiated stop")
            progress = 1.0 if now >= ramp_deadline else ((now - ramp_started) / max(0.2, self.args.release_ramp_s))
            home_cmd = shared_home_i + ((single_home_i - shared_home_i) * progress)
            donor_cmd = shared_donor_i + ((self.donor_release_hold_a - shared_donor_i) * progress)
            self.current_plan["home"] = ControlPlan(True, target_v, max(0.0, home_cmd))
            self.current_plan["donor"] = ControlPlan(True, target_v, max(0.0, donor_cmd))
            self._service_cycle("current_demand", target_v, requested_i)
            donor_actual = module_current_value(self.donor_module)
            if progress >= 1.0 and donor_actual <= self.args.release_donor_current_a:
                break
            time.sleep(0.05)

        self.desired_relays[("donor", self.args.donor_bridge_relay)] = False
        self.current_plan["home"] = ControlPlan(True, target_v, single_home_i)
        self.current_plan["donor"] = ControlPlan(True, target_v, self.donor_release_hold_a)
        self._wait_until(
            self.args.release_timeout_s,
            lambda: self._relay_is_open("donor", self.args.donor_bridge_relay),
            "donor bridge relay did not open during release",
            "current_demand",
            target_v,
            requested_i,
        )
        post_open_deadline = time.time() + self.args.release_settle_s
        while time.time() < post_open_deadline:
            self._service_cycle("current_demand", target_v, requested_i)
            time.sleep(0.05)
        if self.donor_module:
            self.can.stop_output(self.donor_module)
            time.sleep(0.05)
            self.can.stop_output(self.donor_module)
        self.current_plan["donor"] = ControlPlan(False, 0.0, 0.0)
        self.release_done = True
        self._set_phase("post_release")

    def _stop_verification_ok(self) -> bool:
        with self.borrower_lock:
            borrower_power_open = self.borrower.relay_states.get(self.args.borrower_power_relay) is False
            borrower_stop_done = self.borrower.stop_done
            borrower_session_inactive = (not self.borrower.session_started) and (not self.borrower.hlc_active)
        with self.donor_lock:
            donor_bridge_open = self.donor.relay_states.get(self.args.donor_bridge_relay) is False
        home_off = True if self.home_module is None else module_is_off(self.home_module)
        donor_off = True if self.donor_module is None else module_is_off(self.donor_module)
        return borrower_power_open and donor_bridge_open and (borrower_stop_done or borrower_session_inactive) and home_off and donor_off

    def _protocol_stop_counters(self) -> tuple[int, int, int]:
        with self.borrower_lock:
            return (
                self.borrower.power_delivery_req_count,
                self.borrower.welding_req_count,
                self.borrower.session_stop_req_count,
            )

    def _wait_for_protocol_stop(
        self,
        baseline: Optional[tuple[int, int, int]] = None,
        vehicle_initiated: bool = False,
    ) -> bool:
        baseline_pd, baseline_wd, baseline_ss = baseline if baseline is not None else self._protocol_stop_counters()
        current_pd, current_wd, current_ss = self._protocol_stop_counters()
        saw_power_delivery = current_pd > baseline_pd
        saw_welding = current_wd > baseline_wd
        saw_session_stop = current_ss > baseline_ss
        relay_opened = False
        relay_open_requested = False
        logged_welding = False
        logged_session_stop = False
        logged_power_delivery = False
        self._set_phase("stop_request")
        target_v = self._target_voltage()
        requested_i = self._requested_current()
        if requested_i <= 0.0:
            requested_i = max(
                self.args.single_home_current_a,
                self.args.shared_home_current_a + self.args.shared_donor_current_a,
            )

        deadline = time.time() + self.args.protocol_stop_timeout_s
        last_auth = 0.0
        last_status = 0.0
        last_hb = 0.0

        if vehicle_initiated:
            self.current_plan["home"] = ControlPlan(False, 0.0, 0.0)
            self.current_plan["donor"] = ControlPlan(False, 0.0, 0.0)
            self.desired_relays[("borrower", self.args.borrower_power_relay)] = False
            self.desired_relays[("donor", self.args.donor_bridge_relay)] = False
        if vehicle_initiated:
            self.events.append(f"{ts()} protocol_stop=vehicle:{self.vehicle_stop_reason or 'PowerDeliveryReq'}")
        else:
            self.events.append(f"{ts()} protocol_stop=notify")
        if saw_power_delivery and not relay_open_requested:
            self.current_plan["home"] = ControlPlan(False, 0.0, 0.0)
            self.current_plan["donor"] = ControlPlan(False, 0.0, 0.0)
            self.desired_relays[("borrower", self.args.borrower_power_relay)] = False
            relay_open_requested = True
            self.events.append(f"{ts()} protocol_stop=power_delivery")

        while time.time() < deadline:
            now = time.time()
            if (now - last_hb) * 1000.0 >= self.args.heartbeat_ms:
                self._send("borrower", f"CTRL HB {self.args.heartbeat_timeout_ms}")
                last_hb = now
            if now - last_status >= self.args.status_interval_s:
                self._send("borrower", "CTRL STATUS")
                last_status = now
            if vehicle_initiated and now - last_auth >= 1.0:
                self._send("borrower", f"CTRL AUTH deny {self.args.auth_ttl_ms}")
                last_auth = now

            self._tick_runtime_mode_safe(now)
            if (not relay_open_requested) and saw_power_delivery:
                self.current_plan["home"] = ControlPlan(False, 0.0, 0.0)
                self.current_plan["donor"] = ControlPlan(False, 0.0, 0.0)
                self.desired_relays[("borrower", self.args.borrower_power_relay)] = False
                self.desired_relays[("donor", self.args.donor_bridge_relay)] = False
                relay_open_requested = True
                self.events.append(f"{ts()} protocol_stop=power_delivery")
            stage = "power_delivery" if saw_power_delivery else "stop_request"
            self._tick_protocol_stop_feedback(now)
            self._tick_module_outputs(now)
            self._tick_relays(now)
            self._tick_telemetry(now)
            self._tick_progress(now, stage)
            if relay_open_requested and self._relay_is_open("borrower", self.args.borrower_power_relay):
                relay_opened = True

            pd_count, wd_count, ss_count = self._protocol_stop_counters()
            if pd_count > baseline_pd:
                saw_power_delivery = True
            if saw_power_delivery and not logged_power_delivery:
                logged_power_delivery = True
            if wd_count > baseline_wd:
                saw_welding = True
            if ss_count > baseline_ss:
                saw_session_stop = True

            if saw_welding and relay_opened and not logged_welding:
                self.events.append(f"{ts()} protocol_stop=welding")
                logged_welding = True
            if saw_session_stop and not logged_session_stop:
                self.events.append(f"{ts()} protocol_stop=session_stop")
                logged_session_stop = True
            if saw_session_stop:
                self._send("borrower", "CTRL SLAC disarm 3000")
                return True
            time.sleep(0.05)
        return False

    def _tick_runtime_mode_safe(self, now: float) -> None:
        self._check_runtime_mode()
        if now - self.last_donor_hb >= (self.args.heartbeat_ms / 1000.0):
            self._send("donor", f"CTRL HB {self.args.heartbeat_timeout_ms}")
            self.last_donor_hb = now
        if now - self.last_donor_status >= max(5.0, self.args.status_interval_s):
            self._send("donor", "CTRL STATUS")
            self.last_donor_status = now

    def _shutdown_modules(self) -> None:
        if self.home_module:
            for _ in range(2):
                self.can.stop_output(self.home_module)
                time.sleep(0.03)
        if self.donor_module:
            for _ in range(2):
                self.can.stop_output(self.donor_module)
                time.sleep(0.03)
        self.current_plan["home"] = ControlPlan(False, 0.0, 0.0)
        self.current_plan["donor"] = ControlPlan(False, 0.0, 0.0)
        self.plan_active["home"] = False
        self.plan_active["donor"] = False

    def _execute_stop_sequence(self) -> None:
        if self.stop_requested:
            return
        vehicle_initiated = self._vehicle_stop_pending()
        baseline = self.session_protocol_baseline if vehicle_initiated else None
        protocol_stop_ok = self._wait_for_protocol_stop(baseline=baseline, vehicle_initiated=vehicle_initiated)
        self.protocol_stop_verified = protocol_stop_ok
        self.stop_requested = True
        self.stop_started_at = time.time()
        self.stop_hard_sent = self.args.stop_mode == "hard"
        self._set_phase("stopping")

        if not protocol_stop_ok:
            self.notes.append("protocol stop timeout; falling back to forced stop")
            stop_cmd = f"CTRL STOP {self.args.stop_mode} 3000"
            self._send("borrower", stop_cmd)
        self._send("borrower", f"CTRL AUTH deny {self.args.auth_ttl_ms}")
        self._send("borrower", "CTRL SLAC disarm 3000")
        self._send("donor", f"CTRL AUTH deny {self.args.auth_ttl_ms}")
        self._send("donor", "CTRL SLAC disarm 3000")
        self._send("borrower", "CTRL FEEDBACK 1 0 0 0 0 0 0")
        self.desired_relays[("borrower", self.args.borrower_power_relay)] = False
        self.desired_relays[("donor", self.args.donor_bridge_relay)] = False
        self._shutdown_modules()

        deadline = time.time() + self.args.stop_timeout_s
        stop_verified = False
        while time.time() < deadline:
            now = time.time()
            self._tick_borrower_contract(now)
            self._tick_donor_idle(now)
            self._tick_relays(now)
            self._tick_telemetry(now)
            if self.stop_started_at is not None and (not self.stop_hard_sent) and (now - self.stop_started_at) >= self.args.stop_hard_after_s:
                self._send("borrower", "CTRL STOP hard 3000")
                self.stop_hard_sent = True
            if (now - self.last_feedback_cmd) >= self.args.feedback_interval_s:
                self._send("borrower", "CTRL FEEDBACK 1 0 0 0 0 0 0")
                self.last_feedback_cmd = now
            if self._stop_verification_ok():
                stop_verified = True
                break
            time.sleep(0.05)
        if not stop_verified:
            raise RuntimeError("stop verification timeout: relays/modules did not reach OFF state")
        if not protocol_stop_ok:
            raise RuntimeError("protocol stop timeout; forced stop used")

    def _write_summary(self, result: str) -> None:
        payload = {
            "result": result,
            "phase": self.phase,
            "events": self.events,
            "notes": self.notes,
            "capability_clamps": self.capability_clamps,
            "capability_warnings": self.capability_warnings,
            "failures": self.failures,
            "current_demand_started_at": self.current_demand_started_at,
            "attach_done": self.attach_done,
            "release_done": self.release_done,
            "protocol_stop_verified": self.protocol_stop_verified,
            "vehicle_stop_requested": self.vehicle_stop_requested,
            "vehicle_stop_reason": self.vehicle_stop_reason,
            "vehicle_stop_seen_at": self.vehicle_stop_seen_at,
            "borrower": asdict(self.borrower),
            "donor": asdict(self.donor),
            "home_module": asdict(
                SummaryModule(
                    address=self.home_module.address if self.home_module else None,
                    group=self.home_module.group if self.home_module else None,
                    rated_current_a=self.home_module.rated_current_a if self.home_module else None,
                    status=module_status_text(self.home_module) if self.home_module else None,
                    voltage_v=self.home_module.voltage_v if self.home_module else None,
                    current_a=self.home_module.current_a if self.home_module else None,
                )
            ),
            "donor_module": asdict(
                SummaryModule(
                    address=self.donor_module.address if self.donor_module else None,
                    group=self.donor_module.group if self.donor_module else None,
                    rated_current_a=self.donor_module.rated_current_a if self.donor_module else None,
                    status=module_status_text(self.donor_module) if self.donor_module else None,
                    voltage_v=self.donor_module.voltage_v if self.donor_module else None,
                    current_a=self.donor_module.current_a if self.donor_module else None,
                )
            ),
            "phase_metrics": {name: metrics.to_json() for name, metrics in self.phase_metrics.items()},
        }
        ensure_parent(self.args.summary_file)
        with open(self.args.summary_file, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, indent=2)

    def run(self) -> int:
        result = "fail"
        try:
            self.borrower_link.open()
            self.donor_link.open()
            self.can.open()
            print(f"BORROWER_LOG={self.args.borrower_log}", flush=True)
            print(f"DONOR_LOG={self.args.donor_log}", flush=True)
            print(f"CAN_LOG={self.args.can_log}", flush=True)
            print(f"SUMMARY_FILE={self.args.summary_file}", flush=True)

            borrower_status = self._bootstrap_plc("borrower", self.args.borrower_plc_id)
            donor_status = self._bootstrap_plc("donor", self.args.donor_plc_id)
            print(
                f"[BOOT] borrower mode={borrower_status.mode_name} can_stack={borrower_status.can_stack} module_mgr={borrower_status.module_mgr}; "
                f"donor mode={donor_status.mode_name} can_stack={donor_status.can_stack} module_mgr={donor_status.module_mgr}",
                flush=True,
            )

            self.home_module, self.donor_module = self.can.scan_expected(
                self.args.home_addr,
                self.args.home_group,
                self.args.donor_addr,
                self.args.donor_group,
            )
            self.can.refresh(self.home_module, response_window_s=0.5)
            self.can.refresh(self.donor_module, response_window_s=0.5)
            self.can.soft_reset(self.home_module, input_mode=self.args.input_mode)
            self.can.soft_reset(self.donor_module, input_mode=self.args.input_mode)
            self._compute_limits()

            self._set_phase("wait_for_session")
            start_wall = time.time()
            total_charge_s = self.args.home_duration_s + self.args.shared_duration_s + self.args.post_release_duration_s

            while True:
                now = time.time()
                if (now - start_wall) >= self.args.max_total_s:
                    raise RuntimeError("global timeout waiting for complete charge session")

                stage, _target_v, _target_i, _stage_ts, _fresh = self._borrower_stage_snapshot()
                target_v = self._target_voltage()
                requested_i = self._requested_current()

                if stage == "current_demand" and self.current_demand_started_at is None:
                    with self.borrower_lock:
                        self.current_demand_started_at = self.borrower.first_current_demand_ts or now
                    self.session_protocol_baseline = self._protocol_stop_counters()
                    self.vehicle_stop_requested = False
                    self.vehicle_stop_reason = ""
                    self.vehicle_stop_seen_at = None
                    print("[SESSION] first CurrentDemand observed", flush=True)
                    self._set_phase("home_only")

                if self.current_demand_started_at is not None and not self.attach_done:
                    charge_elapsed = now - self.current_demand_started_at
                    if charge_elapsed >= self.args.home_duration_s:
                        self._execute_attach_transition()
                        continue

                if self.current_demand_started_at is not None and self.attach_done and not self.release_done:
                    charge_elapsed = now - self.current_demand_started_at
                    if charge_elapsed >= (self.args.home_duration_s + self.args.shared_duration_s):
                        self._execute_release_transition()
                        continue

                if self.current_demand_started_at is not None:
                    charge_elapsed = now - self.current_demand_started_at
                    if charge_elapsed >= total_charge_s:
                        self._execute_stop_sequence()
                        result = "pass" if not self.failures else "fail"
                        break

                if self.current_demand_started_at is not None and self._vehicle_stop_pending():
                    charge_elapsed = now - self.current_demand_started_at
                    msg = (
                        f"vehicle initiated protocol stop early at {charge_elapsed:.1f}s "
                        f"({self.vehicle_stop_reason or 'unknown'})"
                    )
                    self.failures.append(msg)
                    self.notes.append(msg)
                    self._execute_stop_sequence()
                    result = "fail"
                    break

                if self.current_demand_started_at is None and (now - start_wall) >= self.args.session_start_timeout_s:
                    raise RuntimeError("session never reached CurrentDemand")

                with self.borrower_lock:
                    borrower_cp_connected = self.borrower.cp_connected
                    borrower_hlc_active = self.borrower.hlc_active
                if (
                    self.current_demand_started_at is None
                    and borrower_cp_connected
                    and (not borrower_hlc_active)
                    and (now - self.last_start_recovery_at) >= self.args.start_recovery_s
                ):
                    self._recover_start_path(now)
                    continue

                if stage == "precharge":
                    self._set_phase("precharge")
                    self._apply_precharge_plan(target_v)
                elif stage in ("power_delivery", "current_demand"):
                    if self.attach_done and not self.release_done:
                        self._apply_shared_plan(target_v, requested_i)
                    else:
                        self._apply_home_only_plan(target_v, requested_i, post_release=self.release_done)
                else:
                    if self.current_demand_started_at is None:
                        self._set_phase("wait_for_session")
                    self._enter_idle_output()

                self._service_cycle(stage, target_v, requested_i)
                time.sleep(0.05)

            if result == "pass":
                print("[PASS] vehicle UART-router charging test completed", flush=True)
            else:
                print(f"[FAIL] failures={self.failures}", flush=True)
            return 0 if result == "pass" else 1
        except KeyboardInterrupt:
            self.failures.append("KeyboardInterrupt")
            print("[FAIL] interrupted", flush=True)
            return 130
        except VehicleInitiatedStop as exc:
            if self.current_demand_started_at is not None and not self.stop_requested:
                charge_elapsed = time.time() - self.current_demand_started_at
                msg = f"vehicle initiated protocol stop during transition at {charge_elapsed:.1f}s ({exc})"
                self.failures.append(msg)
                self.notes.append(msg)
                try:
                    self._execute_stop_sequence()
                except Exception as stop_exc:
                    self.failures.append(str(stop_exc))
            print(f"[FAIL] {self.failures[-1]}", flush=True)
            return 1
        except Exception as exc:
            self.failures.append(str(exc))
            print(f"[FAIL] {exc}", flush=True)
            return 1
        finally:
            try:
                self._shutdown_modules()
            except Exception:
                pass
            try:
                self.desired_relays[("borrower", self.args.borrower_power_relay)] = False
                self.desired_relays[("donor", self.args.donor_bridge_relay)] = False
                self._set_relay("borrower", self.args.borrower_power_relay, False, 0, require_ack=False)
                self._set_relay("donor", self.args.donor_bridge_relay, False, 0, require_ack=False)
            except Exception:
                pass
            self.borrower_link.close()
            self.donor_link.close()
            self.can.close()
            self._write_summary(result)
            print(f"SUMMARY_FILE_DONE={self.args.summary_file}", flush=True)


def parse_args() -> argparse.Namespace:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(description="Live vehicle mode-2 UART-router charge test with direct Maxwell CAN")
    parser.add_argument("--borrower-port", default="/dev/ttyACM0")
    parser.add_argument("--donor-port", default="/dev/ttyACM1")
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument("--can-iface", default="can0")
    parser.add_argument("--borrower-plc-id", type=int, default=1)
    parser.add_argument("--donor-plc-id", type=int, default=2)
    parser.add_argument("--controller-id", type=int, default=1)
    parser.add_argument("--borrower-power-relay", type=int, default=1)
    parser.add_argument("--donor-bridge-relay", type=int, default=2)
    parser.add_argument("--home-addr", type=int, default=1)
    parser.add_argument("--home-group", type=int, default=1)
    parser.add_argument("--donor-addr", type=int, default=3)
    parser.add_argument("--donor-group", type=int, default=2)
    parser.add_argument("--input-mode", type=int, default=3)
    parser.add_argument("--target-voltage-v", type=float, default=500.0)
    parser.add_argument("--precharge-current-a", type=float, default=2.0)
    parser.add_argument("--single-home-current-a", type=float, default=40.0)
    parser.add_argument("--shared-home-current-a", type=float, default=20.0)
    parser.add_argument("--shared-donor-current-a", type=float, default=20.0)
    parser.add_argument("--donor-bootstrap-current-a", type=float, default=5.0)
    parser.add_argument("--donor-release-hold-a", type=float, default=0.5)
    parser.add_argument("--home-duration-s", type=int, default=40)
    parser.add_argument("--shared-duration-s", type=int, default=40)
    parser.add_argument("--post-release-duration-s", type=int, default=40)
    parser.add_argument("--session-start-timeout-s", type=float, default=120.0)
    parser.add_argument("--max-total-s", type=float, default=240.0)
    parser.add_argument("--attach-timeout-s", type=float, default=25.0)
    parser.add_argument("--release-timeout-s", type=float, default=25.0)
    parser.add_argument("--share-ramp-s", type=float, default=3.0)
    parser.add_argument("--release-ramp-s", type=float, default=3.0)
    parser.add_argument("--control-interval-s", type=float, default=0.25)
    parser.add_argument("--feedback-interval-s", type=float, default=0.25)
    parser.add_argument("--telemetry-interval-s", type=float, default=0.75)
    parser.add_argument("--telemetry-response-s", type=float, default=0.25)
    parser.add_argument("--status-interval-s", type=float, default=2.0)
    parser.add_argument("--max-status-misses", type=int, default=5)
    parser.add_argument("--heartbeat-ms", type=int, default=800)
    parser.add_argument("--heartbeat-timeout-ms", type=int, default=3000)
    parser.add_argument("--auth-ttl-ms", type=int, default=6000)
    parser.add_argument("--arm-ms", type=int, default=12000)
    parser.add_argument("--request-fresh-s", type=float, default=8.0)
    parser.add_argument("--start-recovery-s", type=float, default=12.0)
    parser.add_argument("--relay-hold-ms", type=int, default=1500)
    parser.add_argument("--relay-refresh-s", type=float, default=0.6)
    parser.add_argument("--match-tolerance-v", type=float, default=15.0)
    parser.add_argument("--feedback-voltage-tolerance-v", type=float, default=15.0)
    parser.add_argument("--min-home-bus-voltage-v", type=float, default=450.0)
    parser.add_argument("--min-shared-bus-voltage-v", type=float, default=450.0)
    parser.add_argument("--fail-on-low-bus-voltage", action="store_true")
    parser.add_argument("--transition-grace-s", type=float, default=3.0)
    parser.add_argument("--collapse-bus-voltage-v", type=float, default=40.0)
    parser.add_argument("--attach-settle-s", type=float, default=1.0)
    parser.add_argument("--attach-min-donor-current-a", type=float, default=2.0)
    parser.add_argument("--attach-max-bus-dip-v", type=float, default=30.0)
    parser.add_argument("--release-donor-current-a", type=float, default=1.0)
    parser.add_argument("--release-settle-s", type=float, default=1.0)
    parser.add_argument("--default-rated-current-a", type=float, default=30.0)
    parser.add_argument("--absolute-current-scale", type=float, default=1024.0)
    parser.add_argument("--enforce-rated-current", action="store_true")
    parser.add_argument("--stop-mode", choices=("soft", "hard"), default="hard")
    parser.add_argument("--protocol-stop-timeout-s", type=float, default=20.0)
    parser.add_argument("--stop-hard-after-s", type=float, default=8.0)
    parser.add_argument("--stop-timeout-s", type=float, default=45.0)
    parser.add_argument(
        "--borrower-log",
        default=f"/home/jpi/Desktop/EVSE/plc_firmware/logs/mode2_uart_vehicle_charge_{stamp}_borrower.log",
    )
    parser.add_argument(
        "--donor-log",
        default=f"/home/jpi/Desktop/EVSE/plc_firmware/logs/mode2_uart_vehicle_charge_{stamp}_donor.log",
    )
    parser.add_argument(
        "--can-log",
        default=f"/home/jpi/Desktop/EVSE/plc_firmware/logs/mode2_uart_vehicle_charge_{stamp}_can.log",
    )
    parser.add_argument(
        "--summary-file",
        default=f"/home/jpi/Desktop/EVSE/plc_firmware/logs/mode2_uart_vehicle_charge_{stamp}_summary.json",
    )
    args = parser.parse_args()
    args.heartbeat_timeout_ms = max(3000, int(args.heartbeat_timeout_ms), int(args.heartbeat_ms) * 8)
    args.auth_ttl_ms = max(6000, int(args.auth_ttl_ms), int(args.heartbeat_timeout_ms) + 2000)
    return args


def main() -> int:
    runner = VehicleChargeRunner(parse_args())
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
