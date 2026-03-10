#!/usr/bin/env python3
"""
Timed controller-backed shared-charging exerciser over the real PLC control-plane CAN contract.

Sequence:
  1. Home-only charge on PLC1 with module 1
  2. After N seconds from first CurrentDemand, close PLC2 Relay2 and lease module 3 to PLC1
  3. After M seconds from first CurrentDemand, shrink back to module 1 only and open PLC2 Relay2
  4. Keep running until total duration expires, then issue a soft stop

This intentionally bypasses the higher-level site controller so borrowing behavior can be tested in isolation.
By default, the harness first forces each PLC into `mode=1` over serial so the
CAN controller contract is active and aligned with the current firmware.
Passive serial logging is optional.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import os
import pathlib
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

try:
    import can  # type: ignore
except Exception as exc:
    print(f"[ERR] python-can import failed: {exc}", file=sys.stderr)
    sys.exit(2)

try:
    import serial  # type: ignore
except Exception:
    serial = None


CTRL_MSG_VERSION = 1

CAN_ID_CTRL_HEARTBEAT = 0x18FF5000
CAN_ID_CTRL_AUTH = 0x18FF5010
CAN_ID_CTRL_SLAC = 0x18FF5020
CAN_ID_CTRL_ALLOC_BEGIN = 0x18FF5030
CAN_ID_CTRL_ALLOC_DATA = 0x18FF5040
CAN_ID_CTRL_ALLOC_COMMIT = 0x18FF5050
CAN_ID_CTRL_ALLOC_ABORT = 0x18FF5060
CAN_ID_CTRL_AUX_RELAY = 0x18FF5070
CAN_ID_CTRL_SESSION = 0x18FF5080

CAN_ID_PLC_CP_STATUS = 0x18FF6010
CAN_ID_PLC_SLAC_STATUS = 0x18FF6020
CAN_ID_PLC_HLC_STATUS = 0x18FF6030
CAN_ID_PLC_POWER_STATUS = 0x18FF6040
CAN_ID_PLC_SESSION_STATUS = 0x18FF6050
CAN_ID_PLC_IDENTITY_EVT = 0x18FF6060
CAN_ID_PLC_ACK = 0x18FF6070
CAN_ID_PLC_BMS = 0x18FF6080

CTRL_AUTH_DENY = 0
CTRL_AUTH_PENDING = 1
CTRL_AUTH_GRANTED = 2

CTRL_SLAC_DISARM = 0
CTRL_SLAC_ARM = 1
CTRL_SLAC_START = 2
CTRL_SLAC_ABORT = 3

CTRL_SESSION_STOP_SOFT = 1
CTRL_SESSION_STOP_HARD = 2
CTRL_SESSION_CLEAR = 3

ACK_OK = 0

ACK_CMD_ALLOC_BEGIN = 0x13
ACK_CMD_ALLOC_DATA = 0x14
ACK_CMD_ALLOC_COMMIT = 0x15
ACK_CMD_ALLOC_ABORT = 0x16
ACK_CMD_AUX_RELAY = 0x17

CP_CONNECTED_RE = re.compile(r"\[CP\] state .* -> ([BCD])")
CP_DISCONNECT_RE = re.compile(r"\[CP\] state .* -> ([AEF])")
CURRENT_DEMAND_RE = re.compile(r'"msg":"CurrentDemand"')
SESSION_SETUP_EVCCID_RE = re.compile(r"\[HLC\] EVCCID ([0-9A-Fa-f]+)")
SESSION_SETUP_EVCCID_JSON_RE = re.compile(r'"msg":"SessionSetup".*?"evccId":"([0-9A-Fa-f]+)"')
EV_MAC_RE = re.compile(r"\[SLAC\] EV_MAC ([0-9A-Fa-f:]+)")
MODRT_GRP_RE = re.compile(
    r"\[MODRT\]\[GRP\].*en=(\d+).*assigned=(\d+).*active=(\d+).*reqV=([0-9.+-]+).*reqI=([0-9.+-]+).*cmbV=([0-9.+-]+).*cmbI=([0-9.+-]+).*availI=([0-9.+-]+)"
)
MODRT_MOD_RE = re.compile(
    r"\[MODRT\]\[MOD\].*id=([^\s]+).*lease=(\d+).*off=(\d+).*fault=(\d+).*online=(\d+).*fresh=(\d+).*V=([0-9.+-]+).*I=([0-9.+-]+)"
)


def steady_ts() -> str:
    return dt.datetime.now().isoformat(timespec="milliseconds")


def crc8_07(data: bytes) -> int:
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x80:
                crc = ((crc << 1) ^ 0x07) & 0xFF
            else:
                crc = (crc << 1) & 0xFF
    return crc


def with_node(base_id: int, node_id: int) -> int:
    return (base_id & 0x1FFFFFF0) | (node_id & 0x0F)


def encode_half_kw(limit_kw: float) -> int:
    return max(0, min(255, int(round(max(0.0, limit_kw) * 2.0))))


def clamp_100ms(value_ms: int) -> int:
    return max(1, min(255, (int(value_ms) + 99) // 100))


@dataclass
class ModuleSpec:
    module_id: str
    can_address: int
    can_group: int
    rated_power_kw: float


@dataclass
class PlcSpec:
    plc_id: int
    connector_id: int
    can_node_id: int
    serial_port: str
    modules: List[ModuleSpec]


@dataclass
class ObservedModule:
    module_id: str
    lease_active: int = 0
    off: int = 1
    fault: int = 0
    online: int = 0
    fresh: int = 0
    voltage_v: float = 0.0
    current_a: float = 0.0


@dataclass
class PlcRuntime:
    plc_id: int
    connector_id: int
    heartbeat_seen: bool = False
    cp_phase: str = "U"
    cp_duty_pct: int = 0
    b1_since: Optional[float] = None
    relay1_closed: bool = False
    relay2_closed: bool = False
    session_started: bool = False
    hlc_ready: bool = False
    hlc_active: bool = False
    precharge_seen: bool = False
    precharge_converged: bool = False
    matched: bool = False
    delivered_voltage_v: float = 0.0
    delivered_current_a: float = 0.0
    delivered_power_kw: float = 0.0
    imported_wh: float = 0.0
    soc_pct: Optional[int] = None
    bms_voltage_v: float = 0.0
    bms_current_a: float = 0.0
    bms_valid: bool = False
    bms_delivery_ready: bool = False
    current_demand_seen: bool = False
    charge_started_at: Optional[float] = None
    session_done_count: int = 0
    ev_identity: str = ""
    identity_source: str = ""
    last_group_line: str = ""
    group_assigned: int = 0
    group_active: int = 0
    modules: Dict[str, ObservedModule] = field(default_factory=dict)
    ack_log: List[str] = field(default_factory=list)


@dataclass
class IdentityAssembly:
    event_kind: int = 0
    event_id: int = 0
    expected_segments: int = 0
    bytes: bytearray = field(default_factory=bytearray)


@dataclass
class HarnessConfig:
    can_interface: str
    controller_can_id: int
    heartbeat_period_ms: int
    heartbeat_timeout_ms: int
    assignment_stale_ms: int
    slac_arm_ms: int
    plc1: PlcSpec
    plc2: PlcSpec
    local_allow_token: str
    borrow_at_s: int
    release_at_s: int
    total_s: int
    initial_module_limit_kw: float
    shared_module_limit_kw: float
    serial_bootstrap: bool
    serial_log: bool
    run_dir: pathlib.Path


@dataclass
class PendingPlanFrame:
    msg: can.Message
    note: str
    ack_cmd: int
    ack_seq: int
    attempts: int = 0
    last_sent_at: float = 0.0


class SerialMonitor(threading.Thread):
    def __init__(self, plc: PlcSpec, runtime: PlcRuntime, log_path: pathlib.Path, stop_event: threading.Event) -> None:
        super().__init__(daemon=True)
        self.plc = plc
        self.runtime = runtime
        self.log_path = log_path
        self.stop_event = stop_event
        self.ser: Optional[serial.Serial] = None
        self.lock = threading.Lock()

    def run(self) -> None:
        if serial is None:
            with open(self.log_path, "a", encoding="utf-8") as fp:
                fp.write(f"{steady_ts()} [ERR] pyserial unavailable, serial logging disabled\n")
            return
        try:
            self.ser = serial.Serial(self.plc.serial_port, 115200, timeout=0.1)
            self.ser.reset_input_buffer()
        except Exception as exc:
            with open(self.log_path, "a", encoding="utf-8") as fp:
                fp.write(f"{steady_ts()} [ERR] serial open failed: {exc}\n")
            return

        with open(self.log_path, "a", encoding="utf-8", buffering=1) as fp:
            while not self.stop_event.is_set():
                try:
                    raw = self.ser.readline()
                except Exception as exc:
                    fp.write(f"{steady_ts()} [ERR] serial read failed: {exc}\n")
                    break
                if not raw:
                    continue
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                fp.write(f"{steady_ts()} {line}\n")
                self._parse_line(line)

        try:
            if self.ser:
                self.ser.close()
        except Exception:
            pass

    def _parse_line(self, line: str) -> None:
        now = time.time()
        if CP_CONNECTED_RE.search(line):
            phase = CP_CONNECTED_RE.search(line).group(1)  # type: ignore[union-attr]
            if phase != "B" or self.runtime.cp_phase not in ("B1", "B2"):
                self.runtime.cp_phase = phase
            if phase != "B":
                self.runtime.b1_since = None
        elif CP_DISCONNECT_RE.search(line):
            self.runtime.cp_phase = CP_DISCONNECT_RE.search(line).group(1)  # type: ignore[union-attr]
            self.runtime.b1_since = None

        if CURRENT_DEMAND_RE.search(line):
            self.runtime.current_demand_seen = True
            if self.runtime.charge_started_at is None:
                self.runtime.charge_started_at = now

        ev_mac = EV_MAC_RE.search(line)
        if ev_mac:
            self.runtime.ev_identity = ev_mac.group(1).replace(":", "").upper()
            self.runtime.identity_source = "evmac"

        evccid = SESSION_SETUP_EVCCID_RE.search(line)
        if evccid:
            self.runtime.ev_identity = evccid.group(1).upper()
            self.runtime.identity_source = "evccid"
        else:
            evccid_json = SESSION_SETUP_EVCCID_JSON_RE.search(line)
            if evccid_json:
                self.runtime.ev_identity = evccid_json.group(1).upper()
                self.runtime.identity_source = "evccid"

        if "client session done rc=" in line:
            self.runtime.session_done_count += 1

        group_match = MODRT_GRP_RE.search(line)
        if group_match:
            self.runtime.last_group_line = line
            self.runtime.group_assigned = int(group_match.group(2))
            self.runtime.group_active = int(group_match.group(3))

        mod_match = MODRT_MOD_RE.search(line)
        if mod_match:
            module_id = mod_match.group(1)
            self.runtime.modules[module_id] = ObservedModule(
                module_id=module_id,
                lease_active=int(mod_match.group(2)),
                off=int(mod_match.group(3)),
                fault=int(mod_match.group(4)),
                online=int(mod_match.group(5)),
                fresh=int(mod_match.group(6)),
                voltage_v=float(mod_match.group(7)),
                current_a=float(mod_match.group(8)),
            )


def bootstrap_controller_mode(plc: PlcSpec, controller_can_id: int, log_path: pathlib.Path) -> None:
    if not plc.serial_port:
        raise RuntimeError(f"PLC{plc.plc_id} has no serial port configured")
    if serial is None:
        raise RuntimeError("pyserial is required for --serial-bootstrap")

    controller_id = max(1, min(15, int(controller_can_id) & 0x0F))
    cmds = [
        "CTRL STOP hard 3000",
        "CTRL RESET",
        f"CTRL MODE 1 {plc.plc_id} {controller_id}",
        "CTRL STATUS",
    ]

    with serial.Serial(plc.serial_port, 115200, timeout=0.1) as ser, open(log_path, "a", encoding="utf-8", buffering=1) as fp:
        time.sleep(0.3)
        ser.reset_input_buffer()
        ser.reset_output_buffer()
        for cmd in cmds:
            fp.write(f"{steady_ts()} > {cmd}\n")
            ser.write((cmd + "\n").encode("utf-8"))
            ser.flush()
            time.sleep(0.2)

        status_ok = False
        deadline = time.time() + 4.0
        while time.time() < deadline:
            raw = ser.readline()
            if not raw:
                continue
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            fp.write(f"{steady_ts()} {line}\n")
            if (
                "[SERCTRL] STATUS" in line
                and "mode=1(" in line
                and f"plc_id={plc.plc_id}" in line
                and f"controller_id={controller_id}" in line
            ):
                status_ok = True
        if not status_ok:
            raise RuntimeError(f"PLC{plc.plc_id} did not confirm mode=1/controller_id={controller_id}")


class TimedBorrowRunner:
    def __init__(self, cfg: HarnessConfig) -> None:
        self.cfg = cfg
        self.stop_event = threading.Event()
        self.bus = can.interface.Bus(channel=cfg.can_interface, bustype="socketcan")
        self.runtimes = {
            cfg.plc1.plc_id: PlcRuntime(cfg.plc1.plc_id, cfg.plc1.connector_id),
            cfg.plc2.plc_id: PlcRuntime(cfg.plc2.plc_id, cfg.plc2.connector_id),
        }
        self.serial_monitors = []
        if cfg.serial_log:
            self.serial_monitors = [
                SerialMonitor(cfg.plc1, self.runtimes[cfg.plc1.plc_id], cfg.run_dir / "plc1-serial.log", self.stop_event),
                SerialMonitor(cfg.plc2, self.runtimes[cfg.plc2.plc_id], cfg.run_dir / "plc2-serial.log", self.stop_event),
            ]
        self.can_thread = threading.Thread(target=self._can_loop, daemon=True)
        self.identity_assembly: Dict[int, IdentityAssembly] = {}
        self.seq_heartbeat = 1
        self.seq_auth = 1
        self.seq_slac = 1
        self.seq_alloc = 1
        self.seq_aux = 1
        self.seq_session = 1
        self.session_marker = int(time.time()) & 0xFFFFFF
        self.last_hb_at = 0.0
        self.last_auth_at = 0.0
        self.last_slac_arm_at = 0.0
        self.last_slac_start_at = 0.0
        self.last_plan_publish_at = 0.0
        self.auth_granted = False
        self.borrow_published = False
        self.release_published = False
        self.stop_published = False
        self.shutdown_phase = False
        self.bootstrap_complete = False
        self.bootstrap_sent = False
        self.last_bootstrap_at = 0.0
        self.remote_start_sent = False
        self.allocation_ready_since: Optional[float] = None
        self.desired_relay2_state: Dict[int, bool] = {
            cfg.plc1.plc_id: False,
            cfg.plc2.plc_id: False,
        }
        self.desired_modules: Dict[int, tuple[str, ...]] = {
            cfg.plc1.plc_id: tuple(),
            cfg.plc2.plc_id: tuple(),
        }
        self.plan_queues: Dict[int, Deque[PendingPlanFrame]] = {
            cfg.plc1.plc_id: collections.deque(),
            cfg.plc2.plc_id: collections.deque(),
        }
        self.plan_inflight: Dict[int, Optional[PendingPlanFrame]] = {
            cfg.plc1.plc_id: None,
            cfg.plc2.plc_id: None,
        }
        self.current_plan_signature = ""
        self.started_at = time.time()
        self.run_log = open(cfg.run_dir / "harness.log", "a", encoding="utf-8", buffering=1)
        self.can_log = open(cfg.run_dir / "can.log", "a", encoding="utf-8", buffering=1)

    def close(self) -> None:
        self.stop_event.set()
        try:
            self.bus.shutdown()
        except Exception:
            pass
        for mon in self.serial_monitors:
            mon.join(timeout=1.0)
        self.can_thread.join(timeout=1.0)
        self.run_log.close()
        self.can_log.close()

    def log(self, msg: str) -> None:
        line = f"{steady_ts()} {msg}"
        print(line, flush=True)
        self.run_log.write(line + "\n")

    def _next_seq(self, name: str) -> int:
        value = getattr(self, name)
        current = 1 if value == 0 else value
        value = (current + 1) & 0xFF
        if value == 0:
            value = 1
        setattr(self, name, value)
        return current

    def _make_frame(
        self,
        base_id: int,
        node_id: int,
        seq_name: str,
        b3: int = 0,
        b4: int = 0,
        b5: int = 0,
        b6: int = 0,
    ) -> can.Message:
        seq = self._next_seq(seq_name)
        payload = bytearray(
            [
                CTRL_MSG_VERSION,
                seq & 0xFF,
                self.cfg.controller_can_id & 0x0F,
                b3 & 0xFF,
                b4 & 0xFF,
                b5 & 0xFF,
                b6 & 0xFF,
                0,
            ]
        )
        payload[7] = crc8_07(payload[:7])
        arb_id = with_node(base_id, node_id)
        return can.Message(arbitration_id=arb_id, is_extended_id=True, data=payload)

    def _send(self, msg: can.Message, note: str) -> None:
        self.bus.send(msg, timeout=0.2)
        self.can_log.write(f"{steady_ts()} TX {msg.arbitration_id:08X}#{msg.data.hex().upper()} {note}\n")

    def _plan_step(self, msg: can.Message, note: str, ack_cmd: int) -> PendingPlanFrame:
        return PendingPlanFrame(msg=msg, note=note, ack_cmd=ack_cmd, ack_seq=int(msg.data[1]))

    def _kick_plan(self, plc_id: int) -> None:
        if self.plan_inflight[plc_id] is not None:
            return
        queue = self.plan_queues[plc_id]
        if not queue:
            return
        step = queue.popleft()
        self.plan_inflight[plc_id] = step
        step.attempts += 1
        step.last_sent_at = time.time()
        self._send(step.msg, step.note)

    def _retry_inflight_plans(self) -> None:
        now = time.time()
        for plc_id, step in self.plan_inflight.items():
            if step is None:
                continue
            if (now - step.last_sent_at) < 0.35:
                continue
            if step.attempts >= 5:
                self.log(f"[PLAN] plc={plc_id} giving up on {step.note}")
                self.plan_inflight[plc_id] = None
                self.plan_queues[plc_id].clear()
                continue
            step.attempts += 1
            step.last_sent_at = now
            self._send(step.msg, f"{step.note} retry={step.attempts}")

    def _send_heartbeat(self, plc: PlcSpec) -> None:
        msg = self._make_frame(
            CAN_ID_CTRL_HEARTBEAT,
            plc.can_node_id,
            "seq_heartbeat",
            clamp_100ms(self.cfg.heartbeat_timeout_ms),
            self.session_marker & 0xFF,
            (self.session_marker >> 8) & 0xFF,
            (self.session_marker >> 16) & 0xFF,
        )
        self._send(msg, f"HB plc={plc.plc_id}")

    def _send_auth(self, plc: PlcSpec, state: int) -> None:
        msg = self._make_frame(
            CAN_ID_CTRL_AUTH,
            plc.can_node_id,
            "seq_auth",
            state,
            clamp_100ms(self.cfg.assignment_stale_ms),
        )
        label = {CTRL_AUTH_DENY: "deny", CTRL_AUTH_PENDING: "pending", CTRL_AUTH_GRANTED: "grant"}.get(state, str(state))
        self._send(msg, f"AUTH plc={plc.plc_id} state={label}")

    def _send_session_clear(self, plc: PlcSpec) -> None:
        msg = self._make_frame(
            CAN_ID_CTRL_SESSION,
            plc.can_node_id,
            "seq_session",
            CTRL_SESSION_CLEAR,
            clamp_100ms(3000),
            0,
        )
        self._send(msg, f"SESSION plc={plc.plc_id} clear")

    def _send_slac(self, plc: PlcSpec, cmd: int) -> None:
        msg = self._make_frame(
            CAN_ID_CTRL_SLAC,
            plc.can_node_id,
            "seq_slac",
            cmd,
            clamp_100ms(self.cfg.slac_arm_ms),
        )
        label = {CTRL_SLAC_DISARM: "disarm", CTRL_SLAC_ARM: "arm", CTRL_SLAC_START: "start", CTRL_SLAC_ABORT: "abort"}[cmd]
        self._send(msg, f"SLAC plc={plc.plc_id} cmd={label}")

    def _send_session_stop(self, plc: PlcSpec, hard: bool) -> None:
        action = CTRL_SESSION_STOP_HARD if hard else CTRL_SESSION_STOP_SOFT
        msg = self._make_frame(
            CAN_ID_CTRL_SESSION,
            plc.can_node_id,
            "seq_session",
            action,
            clamp_100ms(3000),
            2 if hard else 1,
        )
        self._send(msg, f"STOP plc={plc.plc_id} hard={1 if hard else 0}")

    def _send_alloc_abort(self, plc: PlcSpec) -> None:
        msg = self._make_frame(CAN_ID_CTRL_ALLOC_ABORT, plc.can_node_id, "seq_alloc")
        self._send(msg, f"ALLOC_ABORT plc={plc.plc_id}")

    def _send_relay2(self, plc: PlcSpec, closed: bool) -> None:
        msg = self._make_frame(
            CAN_ID_CTRL_AUX_RELAY,
            plc.can_node_id,
            "seq_aux",
            0x01,
            0x01 if closed else 0x00,
            0x00,
        )
        self._send(msg, f"RELAY2 plc={plc.plc_id} closed={1 if closed else 0}")

    def _assignment_steps(
        self,
        plc: PlcSpec,
        modules: List[ModuleSpec],
        limit_kw: float,
        close_relay2: bool,
        force_relay_step: bool = False,
    ) -> Deque[PendingPlanFrame]:
        steps: Deque[PendingPlanFrame] = collections.deque()
        include_relay_step = force_relay_step or (self.desired_relay2_state.get(plc.plc_id, False) != close_relay2)
        self.desired_relay2_state[plc.plc_id] = close_relay2
        self.desired_modules[plc.plc_id] = tuple(module.module_id for module in modules)
        if not modules:
            abort = self._make_frame(CAN_ID_CTRL_ALLOC_ABORT, plc.can_node_id, "seq_alloc")
            steps.append(self._plan_step(abort, f"ALLOC_ABORT plc={plc.plc_id}", ACK_CMD_ALLOC_ABORT))
            if include_relay_step:
                aux = self._make_frame(
                    CAN_ID_CTRL_AUX_RELAY,
                    plc.can_node_id,
                    "seq_aux",
                    0x01,
                    0x01 if close_relay2 else 0x00,
                    0x00,
                )
                steps.append(self._plan_step(aux, f"RELAY2 plc={plc.plc_id} closed={1 if close_relay2 else 0}", ACK_CMD_AUX_RELAY))
            return steps

        txn_id = int(time.time() * 1000) & 0xFF
        begin = self._make_frame(
            CAN_ID_CTRL_ALLOC_BEGIN,
            plc.can_node_id,
            "seq_alloc",
            txn_id,
            len(modules),
            clamp_100ms(self.cfg.assignment_stale_ms),
            encode_half_kw(limit_kw),
        )
        steps.append(
            self._plan_step(
                begin,
                f"ALLOC_BEGIN plc={plc.plc_id} modules={len(modules)} limitKW={limit_kw:.1f}",
                ACK_CMD_ALLOC_BEGIN,
            )
        )

        for module in modules:
            frame = self._make_frame(
                CAN_ID_CTRL_ALLOC_DATA,
                plc.can_node_id,
                "seq_alloc",
                txn_id,
                module.can_group,
                module.can_address,
                encode_half_kw(module.rated_power_kw),
            )
            steps.append(
                self._plan_step(
                    frame,
                    f"ALLOC_DATA plc={plc.plc_id} moduleId={module.module_id} addr={module.can_address} group={module.can_group} limitKW={module.rated_power_kw:.1f}",
                    ACK_CMD_ALLOC_DATA,
                )
            )

        commit = self._make_frame(CAN_ID_CTRL_ALLOC_COMMIT, plc.can_node_id, "seq_alloc", txn_id)
        steps.append(self._plan_step(commit, f"ALLOC_COMMIT plc={plc.plc_id} modules={len(modules)}", ACK_CMD_ALLOC_COMMIT))
        if include_relay_step:
            aux = self._make_frame(
                CAN_ID_CTRL_AUX_RELAY,
                plc.can_node_id,
                "seq_aux",
                0x01,
                0x01 if close_relay2 else 0x00,
                0x00,
            )
            steps.append(self._plan_step(aux, f"RELAY2 plc={plc.plc_id} closed={1 if close_relay2 else 0}", ACK_CMD_AUX_RELAY))
        return steps

    def _queue_plan(
        self,
        plc: PlcSpec,
        modules: List[ModuleSpec],
        limit_kw: float,
        close_relay2: bool,
        force_relay_step: bool = False,
    ) -> None:
        self.plan_queues[plc.plc_id] = self._assignment_steps(plc, modules, limit_kw, close_relay2, force_relay_step)
        self.plan_inflight[plc.plc_id] = None
        self._kick_plan(plc.plc_id)

    def _publish_signature(self, name: str) -> str:
        return name

    @staticmethod
    def _uppercase_hex(raw: bytes) -> str:
        return raw.hex().upper()

    @staticmethod
    def _byte_token(raw: bytes) -> str:
        trimmed = raw.rstrip(b"\x00")
        if trimmed and all(32 <= byte <= 126 for byte in trimmed):
            return trimmed.decode("ascii", errors="replace")
        return trimmed.hex().upper()

    def _handle_identity_event(self, plc: PlcSpec, runtime: PlcRuntime, data: bytes) -> None:
        event_kind = data[2]
        event_id = data[3]
        segment_meta = data[4]
        expected_segments = (segment_meta >> 4) & 0x0F
        segment_index = segment_meta & 0x0F
        assembly = self.identity_assembly.get(plc.plc_id)
        if (
            assembly is None
            or segment_index == 0
            or assembly.event_id != event_id
            or assembly.event_kind != event_kind
            or assembly.expected_segments != expected_segments
        ):
            assembly = IdentityAssembly(
                event_kind=event_kind,
                event_id=event_id,
                expected_segments=expected_segments,
                bytes=bytearray(expected_segments * 2),
            )
            self.identity_assembly[plc.plc_id] = assembly

        offset = segment_index * 2
        if offset < len(assembly.bytes):
            assembly.bytes[offset] = data[5]
        if offset + 1 < len(assembly.bytes):
            assembly.bytes[offset + 1] = data[6]

        if expected_segments == 0 or segment_index + 1 != expected_segments:
            return

        raw = bytes(assembly.bytes)
        del self.identity_assembly[plc.plc_id]
        if event_kind == 2:
            runtime.ev_identity = self._uppercase_hex(raw[:6])
            runtime.identity_source = "evmac"
        elif event_kind == 3:
            runtime.ev_identity = self._byte_token(raw)
            runtime.identity_source = "evccid"
        elif event_kind == 4:
            runtime.ev_identity = self._byte_token(raw)
            runtime.identity_source = "emaid"
        else:
            return
        self.log(f"identity_seen source={runtime.identity_source} token={runtime.ev_identity}")

    def _publish_home_only(self) -> None:
        self._queue_plan(self.cfg.plc1, [self.cfg.plc1.modules[0]], self.cfg.initial_module_limit_kw, False)
        if self.desired_relay2_state[self.cfg.plc2.plc_id] or self.desired_modules[self.cfg.plc2.plc_id]:
            self._queue_plan(self.cfg.plc2, [], 0.0, False)

    def _publish_shared(self) -> None:
        self._queue_plan(
            self.cfg.plc2,
            [],
            0.0,
            True,
        )
        self._queue_plan(
            self.cfg.plc1,
            [self.cfg.plc1.modules[0], self.cfg.plc2.modules[0]],
            self.cfg.initial_module_limit_kw + self.cfg.shared_module_limit_kw,
            False,
        )

    def _publish_release(self) -> None:
        self._queue_plan(self.cfg.plc1, [self.cfg.plc1.modules[0]], self.cfg.initial_module_limit_kw, False)
        if self.desired_relay2_state[self.cfg.plc2.plc_id] or self.desired_modules[self.cfg.plc2.plc_id]:
            self._queue_plan(self.cfg.plc2, [], 0.0, False)

    def _publish_bootstrap_abort(self) -> None:
        self._queue_plan(self.cfg.plc1, [], 0.0, False, force_relay_step=True)
        self._queue_plan(self.cfg.plc2, [], 0.0, False, force_relay_step=True)

    def _plan_idle(self) -> bool:
        return all(value is None for value in self.plan_inflight.values()) and all(
            not queue for queue in self.plan_queues.values()
        )

    def _can_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                msg = self.bus.recv(timeout=0.2)
            except can.CanError:
                if self.stop_event.is_set():
                    break
                continue
            if msg is None:
                continue
            if not msg.is_extended_id or len(msg.data) < 8:
                continue
            self.can_log.write(f"{steady_ts()} RX {msg.arbitration_id:08X}#{bytes(msg.data).hex().upper()}\n")
            self._handle_rx(msg)

    def _spec_for_node(self, node_id: int) -> Optional[PlcSpec]:
        for plc in (self.cfg.plc1, self.cfg.plc2):
            if plc.can_node_id == node_id:
                return plc
        return None

    def _handle_rx(self, msg: can.Message) -> None:
        data = bytes(msg.data)
        if crc8_07(data[:7]) != data[7]:
            return
        node_id = msg.arbitration_id & 0x0F
        plc = self._spec_for_node(node_id)
        if plc is None:
            return
        runtime = self.runtimes[plc.plc_id]
        base = msg.arbitration_id & 0x1FFFFFF0

        if base == CAN_ID_PLC_CP_STATUS:
            state_char = chr(data[2]) if 32 <= data[2] <= 90 else runtime.cp_phase
            duty_pct = int(data[3])
            if state_char == "B" and duty_pct >= 100:
                phase = "B1"
            elif state_char == "B" and duty_pct > 0:
                phase = "B2"
            else:
                phase = state_char
            if phase != runtime.cp_phase:
                if phase == "B1":
                    runtime.b1_since = time.time()
                    self.remote_start_sent = False
                elif phase != "B1":
                    runtime.b1_since = None
                    if phase != "B2":
                        self.remote_start_sent = False
            runtime.cp_phase = phase
            runtime.cp_duty_pct = data[3]
            return
        if base == CAN_ID_PLC_SLAC_STATUS:
            runtime.session_started = data[2] != 0
            runtime.matched = data[3] != 0
            return
        if base == CAN_ID_PLC_HLC_STATUS:
            runtime.hlc_ready = (data[2] & 0x01) != 0
            runtime.hlc_active = (data[2] & 0x02) != 0
            runtime.precharge_seen = (data[6] & 0x01) != 0
            runtime.precharge_converged = (data[6] & 0x02) != 0
            return
        if base == CAN_ID_PLC_POWER_STATUS:
            runtime.delivered_voltage_v = ((data[3] << 8) | data[2]) / 10.0
            runtime.delivered_current_a = ((data[5] << 8) | data[4]) / 10.0
            runtime.delivered_power_kw = (runtime.delivered_voltage_v * runtime.delivered_current_a) / 1000.0
            if runtime.delivered_current_a > 1.0 and runtime.charge_started_at is None:
                runtime.charge_started_at = time.time()
            return
        if base == CAN_ID_PLC_SESSION_STATUS:
            runtime.session_started = (data[2] & 0x01) != 0
            runtime.relay1_closed = (data[3] & 0x01) != 0
            runtime.relay2_closed = (data[3] & 0x02) != 0
            runtime.imported_wh = float((data[5] << 8) | data[4])
            runtime.soc_pct = data[6] if data[6] <= 100 else None
            if (data[3] & 0x20) != 0 and not runtime.session_started:
                runtime.session_done_count += 1
            return
        if base == CAN_ID_PLC_IDENTITY_EVT:
            self._handle_identity_event(plc, runtime, data)
            return
        if base == CAN_ID_PLC_BMS:
            runtime.bms_voltage_v = ((data[3] << 8) | data[2]) / 10.0
            runtime.bms_current_a = ((data[5] << 8) | data[4]) / 10.0
            runtime.bms_valid = (data[6] & 0x01) != 0
            runtime.bms_delivery_ready = (data[6] & 0x02) != 0
            return
        if base == CAN_ID_PLC_ACK:
            cmd = data[2]
            seq = data[3]
            status = data[4]
            detail0 = data[5]
            detail1 = data[6]
            runtime.ack_log.append(f"cmd=0x{cmd:02X} seq={seq} status={status} detail0={detail0} detail1={detail1}")
            self.log(f"[ACK] plc={plc.plc_id} cmd=0x{cmd:02X} seq={seq} status={status} detail0={detail0} detail1={detail1}")
            if cmd == 0x10 and status == ACK_OK:
                runtime.heartbeat_seen = True
            inflight = self.plan_inflight.get(plc.plc_id)
            if inflight and inflight.ack_cmd == cmd and inflight.ack_seq == seq:
                if status == ACK_OK:
                    self.plan_inflight[plc.plc_id] = None
                    self._kick_plan(plc.plc_id)
                else:
                    self.plan_inflight[plc.plc_id] = None
                    self.plan_queues[plc.plc_id].clear()

    def _token_or_default(self) -> str:
        runtime = self.runtimes[self.cfg.plc1.plc_id]
        if runtime.ev_identity:
            return runtime.ev_identity
        return self.cfg.local_allow_token

    def _should_start_slac(self, runtime: PlcRuntime) -> bool:
        return (
            not self.shutdown_phase
            and runtime.cp_phase == "B1"
            and not runtime.matched
            and not runtime.hlc_ready
        )

    def run(self) -> int:
        if self.cfg.serial_bootstrap:
            try:
                self.log("serial_bootstrap_start")
                bootstrap_controller_mode(self.cfg.plc1, self.cfg.controller_can_id, self.cfg.run_dir / "plc1-bootstrap.log")
                bootstrap_controller_mode(self.cfg.plc2, self.cfg.controller_can_id, self.cfg.run_dir / "plc2-bootstrap.log")
                self.log("serial_bootstrap_done")
            except Exception as exc:
                self.log(f"[ERR] serial bootstrap failed: {exc}")
                return 1
        for mon in self.serial_monitors:
            mon.start()
        self.can_thread.start()
        self.log(f"run_dir={self.cfg.run_dir}")
        self.log("publishing initial bootstrap")

        while not self.stop_event.is_set():
            now = time.time()
            elapsed = now - self.started_at
            plc1 = self.runtimes[self.cfg.plc1.plc_id]

            if now - self.last_hb_at >= (self.cfg.heartbeat_period_ms / 1000.0):
                self._send_heartbeat(self.cfg.plc1)
                self._send_heartbeat(self.cfg.plc2)
                self.last_hb_at = now

            for plc_id in self.plan_queues:
                self._kick_plan(plc_id)
            self._retry_inflight_plans()

            controller_ready = self.runtimes[self.cfg.plc1.plc_id].heartbeat_seen and self.runtimes[self.cfg.plc2.plc_id].heartbeat_seen
            if controller_ready and not self.bootstrap_complete:
                if self.bootstrap_sent and now - self.last_bootstrap_at >= 2.5:
                    self.bootstrap_complete = True
                    self.last_plan_publish_at = 0.0
                    self.log("bootstrap complete, switching to home plan")
                elif not self.bootstrap_sent:
                    self._send_session_clear(self.cfg.plc1)
                    self._send_session_clear(self.cfg.plc2)
                    self._send_slac(self.cfg.plc1, CTRL_SLAC_DISARM)
                    self._send_auth(self.cfg.plc1, CTRL_AUTH_DENY)
                    self._publish_bootstrap_abort()
                    self.current_plan_signature = "bootstrap"
                    self.last_plan_publish_at = now
                    self.last_bootstrap_at = now
                    self.bootstrap_sent = True

            desired_plan = "home"
            if self.release_published:
                desired_plan = "release"
            elif self.borrow_published:
                desired_plan = "shared"
            if controller_ready and self.bootstrap_complete and desired_plan != self.current_plan_signature:
                if desired_plan == "release":
                    self._publish_release()
                elif desired_plan == "shared":
                    self._publish_shared()
                else:
                    self._publish_home_only()
                self.current_plan_signature = desired_plan
                self.last_plan_publish_at = now

            if self.bootstrap_complete and plc1.group_assigned > 0:
                if self.allocation_ready_since is None:
                    self.allocation_ready_since = now
            else:
                self.allocation_ready_since = None
                self.remote_start_sent = False

            allocation_ready = self.allocation_ready_since is not None

            desired_auth = CTRL_AUTH_GRANTED if self.auth_granted else CTRL_AUTH_PENDING
            if not self.shutdown_phase and allocation_ready and now - self.last_auth_at >= 0.8:
                self._send_auth(self.cfg.plc1, desired_auth)
                self.last_auth_at = now

            allocation_stable = allocation_ready and self.allocation_ready_since is not None and (now - self.allocation_ready_since) >= 0.6

            if allocation_stable and plc1.heartbeat_seen and self._should_start_slac(plc1):
                if now - self.last_slac_arm_at >= 4.0:
                    self._send_slac(self.cfg.plc1, CTRL_SLAC_ARM)
                    self.last_slac_arm_at = now
                if plc1.b1_since is not None and not self.remote_start_sent and (now - plc1.b1_since) >= 0.6:
                    self._send_slac(self.cfg.plc1, CTRL_SLAC_START)
                    self.last_slac_start_at = now
                    self.remote_start_sent = True
                    self.log("remote_start_triggered on B1")
                elif self.remote_start_sent and now - self.last_slac_start_at >= 4.0 and not plc1.matched:
                    self._send_slac(self.cfg.plc1, CTRL_SLAC_START)
                    self.last_slac_start_at = now
            elif plc1.cp_phase != "B1":
                self.remote_start_sent = False

            if (
                not self.shutdown_phase
                and self.remote_start_sent
                and not plc1.matched
                and not plc1.ev_identity
                and self.last_slac_start_at > 0.0
                and (now - self.last_slac_start_at) >= 8.0
            ):
                self.log("slac_retry=disarm_then_rearm_no_match")
                self._send_slac(self.cfg.plc1, CTRL_SLAC_DISARM)
                self.remote_start_sent = False
                self.last_slac_arm_at = 0.0
                self.last_slac_start_at = 0.0

            if not self.shutdown_phase and not self.auth_granted and plc1.ev_identity:
                self.auth_granted = True
                self.log(f"identity_seen source={plc1.identity_source} token={plc1.ev_identity}")
                self._send_auth(self.cfg.plc1, CTRL_AUTH_GRANTED)
                self.last_auth_at = now

            charge_started_at = plc1.charge_started_at
            if charge_started_at is not None:
                charge_elapsed = now - charge_started_at
                if not self.borrow_published and charge_elapsed >= self.cfg.borrow_at_s:
                    self.borrow_published = True
                    self.log("transition=borrow module3 leased to plc1 and plc2 relay2 close requested")
                    self._publish_shared()
                    self.last_plan_publish_at = now
                if not self.release_published and charge_elapsed >= self.cfg.release_at_s:
                    self.release_published = True
                    self.log("transition=release module3 removed from plc1 and plc2 relay2 open requested")
                    self._publish_release()
                    self.last_plan_publish_at = now

            if not self.stop_published and elapsed >= self.cfg.total_s:
                self.stop_published = True
                self.shutdown_phase = True
                self.remote_start_sent = False
                self.log("issuing final soft stop")
                self._send_session_stop(self.cfg.plc1, hard=False)
                self._send_auth(self.cfg.plc1, CTRL_AUTH_DENY)
                self._send_slac(self.cfg.plc1, CTRL_SLAC_DISARM)

            if self.stop_published and elapsed >= self.cfg.total_s + 3:
                break

            time.sleep(0.1)

        self._write_summary()
        return 0

    def _write_summary(self) -> None:
        summary = {
            "startedAt": self.started_at,
            "borrowPublished": self.borrow_published,
            "releasePublished": self.release_published,
            "authGranted": self.auth_granted,
            "plc1": self._runtime_summary(self.runtimes[self.cfg.plc1.plc_id]),
            "plc2": self._runtime_summary(self.runtimes[self.cfg.plc2.plc_id]),
        }
        with open(self.cfg.run_dir / "summary.json", "w", encoding="utf-8") as fp:
            json.dump(summary, fp, indent=2)
        self.log(f"summary={json.dumps(summary, sort_keys=True)}")

    @staticmethod
    def _runtime_summary(runtime: PlcRuntime) -> dict:
        return {
            "cpPhase": runtime.cp_phase,
            "cpDutyPct": runtime.cp_duty_pct,
            "relay1Closed": runtime.relay1_closed,
            "relay2Closed": runtime.relay2_closed,
            "sessionStarted": runtime.session_started,
            "hlcReady": runtime.hlc_ready,
            "hlcActive": runtime.hlc_active,
            "matched": runtime.matched,
            "deliveredVoltageV": runtime.delivered_voltage_v,
            "deliveredCurrentA": runtime.delivered_current_a,
            "deliveredPowerKW": runtime.delivered_power_kw,
            "importedWh": runtime.imported_wh,
            "socPct": runtime.soc_pct,
            "bmsVoltageV": runtime.bms_voltage_v,
            "bmsCurrentA": runtime.bms_current_a,
            "bmsValid": runtime.bms_valid,
            "bmsDeliveryReady": runtime.bms_delivery_ready,
            "chargeStartedAt": runtime.charge_started_at,
            "sessionDoneCount": runtime.session_done_count,
            "evIdentity": runtime.ev_identity,
            "identitySource": runtime.identity_source,
            "lastGroupLine": runtime.last_group_line,
            "groupAssigned": runtime.group_assigned,
            "groupActive": runtime.group_active,
            "modules": {k: vars(v) for k, v in runtime.modules.items()},
            "acks": runtime.ack_log[-40:],
        }


def load_harness_config(args: argparse.Namespace) -> HarnessConfig:
    with open(args.config, "r", encoding="utf-8") as fp:
        raw = json.load(fp)

    plcs: List[PlcSpec] = []
    for plc_raw in raw["plcs"]:
        modules = [
            ModuleSpec(
                module_id=str(mod["id"]),
                can_address=int(mod["canAddress"]),
                can_group=int(mod["canGroup"]),
                rated_power_kw=float(args.module_kw),
            )
            for mod in plc_raw["modules"]
        ]
        plcs.append(
            PlcSpec(
                plc_id=int(plc_raw["id"]),
                connector_id=int(plc_raw["connectorId"]),
                can_node_id=int(plc_raw.get("canNodeId", plc_raw["id"])),
                serial_port=str(plc_raw["serialPort"]),
                modules=modules,
            )
        )

    if len(plcs) < 2:
        raise SystemExit("need at least two PLCs in config")

    plc1 = next((p for p in plcs if p.plc_id == 1), plcs[0])
    plc2 = next((p for p in plcs if p.plc_id == 2), plcs[1])

    run_dir = pathlib.Path(args.run_dir or f"/home/jpi/Desktop/EVSE/plc_firmware/logs/timed_borrow_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}")
    run_dir.mkdir(parents=True, exist_ok=True)

    assignment_ttl_ms = int(args.assignment_ttl_ms)
    if assignment_ttl_ms <= 0:
        assignment_ttl_ms = max(int(raw["plcContract"]["assignmentStaleMs"]), (int(args.total_s) + 30) * 1000)

    return HarnessConfig(
        can_interface=args.can_interface,
        controller_can_id=int(raw["plcTransport"]["canControllerId"]),
        heartbeat_period_ms=int(raw["plcContract"]["heartbeatPeriodMs"]),
        heartbeat_timeout_ms=int(raw["plcContract"]["heartbeatTimeoutMs"]),
        assignment_stale_ms=assignment_ttl_ms,
        slac_arm_ms=int(raw["plcContract"]["slacArmMs"]),
        plc1=plc1,
        plc2=plc2,
        local_allow_token=(raw.get("authorization", {}).get("localAllowlist") or ["00018733A9C0"])[0],
        borrow_at_s=args.borrow_after_s,
        release_at_s=args.release_after_s,
        total_s=args.total_s,
        initial_module_limit_kw=args.module_kw,
        shared_module_limit_kw=args.module_kw,
        serial_bootstrap=bool(args.serial_bootstrap),
        serial_log=bool(args.serial_log),
        run_dir=run_dir,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Timed controller-backed borrow/release charging exerciser over SocketCAN")
    parser.add_argument(
        "--config",
        default="/home/jpi/Desktop/EVSE/controller/configs/charger-live-2plc-share15-offline-allowlist.json",
        help="controller config used only for PLC/node/module mapping",
    )
    parser.add_argument("--can-interface", default="can0")
    parser.add_argument("--module-kw", type=float, default=15.0, help="per-module leased power limit")
    parser.add_argument("--borrow-after-s", type=int, default=180)
    parser.add_argument("--release-after-s", type=int, default=300)
    parser.add_argument("--total-s", type=int, default=420)
    parser.add_argument("--assignment-ttl-ms", type=int, default=0, help="0 = auto-size to cover the full run")
    parser.add_argument(
        "--serial-bootstrap",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Force each PLC into mode=1 over serial before CAN control",
    )
    parser.add_argument("--serial-log", action="store_true", help="Enable passive PLC serial logging")
    parser.add_argument("--run-dir", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_harness_config(args)
    runner = TimedBorrowRunner(cfg)
    try:
        return runner.run()
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())
