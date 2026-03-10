#!/usr/bin/env python3
"""
Minimal controller-backed shared-charging exerciser over the real PLC CAN contract.

This bypasses /controller and OCPP completely.

Flow:
  1. Clean bootstrap on PLC1 and PLC2
  2. Allocate home module to PLC1
  3. Start SLAC/HLC on PLC1
  4. Grant auth only after EV identity is seen
  5. Start charge timing from first real CurrentDemand / delivered current
  6. After borrow_after_s, lease donor module to PLC1 and close PLC2 Relay2
  7. After release_after_s, shrink back to PLC1 home module and open PLC2 Relay2
  8. After stop_after_s, soft-stop the session

By default, the harness first forces each PLC into `mode=1` over serial so the
CAN controller contract is active and aligned with the current firmware.
Passive serial logging is still optional.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import pathlib
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

try:
    import can  # type: ignore
except Exception as exc:  # pragma: no cover - runtime dependency
    print(f"[ERR] python-can import failed: {exc}", file=sys.stderr)
    sys.exit(2)

try:
    import serial  # type: ignore
except Exception:  # pragma: no cover - optional only when serial bootstrap is disabled
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

CTRL_SESSION_STOP_SOFT = 1
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


def clamp_100ms(value_ms: int) -> int:
    return max(1, min(255, (int(value_ms) + 99) // 100))


def encode_half_kw(limit_kw: float) -> int:
    return max(0, min(255, int(round(max(0.0, limit_kw) * 2.0))))


def cp_state_char(code: int) -> str:
    if 65 <= code <= 90:
        raw = chr(code)
        return raw if raw in "ABCDEF" else "U"
    if code == 1:
        return "A"
    if code in (2, 3, 4):
        return "B"
    if code == 5:
        return "C"
    if code == 6:
        return "D"
    if code == 7:
        return "E"
    if code == 8:
        return "F"
    return "U"


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
class PlcRuntime:
    plc_id: int
    connector_id: int
    heartbeat_seen: bool = False
    cp_phase: str = "U"
    cp_duty_pct: int = 0
    b1_since: Optional[float] = None
    matched: bool = False
    hlc_ready: bool = False
    hlc_active: bool = False
    relay1_closed: bool = False
    relay2_closed: bool = False
    session_started: bool = False
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
    ev_identity: str = ""
    identity_source: str = ""
    group_assigned: int = 0
    group_active: int = 0
    last_group_line: str = ""
    modules: Dict[str, dict] = field(default_factory=dict)
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
    module_kw: float
    borrow_after_s: int
    release_after_s: int
    stop_after_s: int
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

    def run(self) -> None:
        if serial is None:
            with open(self.log_path, "a", encoding="utf-8") as fp:
                fp.write(f"{steady_ts()} [ERR] pyserial unavailable, serial logging disabled\n")
            return
        try:
            ser = serial.Serial(self.plc.serial_port, 115200, timeout=0.1)
            ser.reset_input_buffer()
        except Exception as exc:
            with open(self.log_path, "a", encoding="utf-8") as fp:
                fp.write(f"{steady_ts()} [ERR] serial open failed: {exc}\n")
            return

        with ser, open(self.log_path, "a", encoding="utf-8", buffering=1) as fp:
            while not self.stop_event.is_set():
                try:
                    raw = ser.readline()
                except Exception as exc:
                    fp.write(f"{steady_ts()} [ERR] serial read failed: {exc}\n")
                    break
                if not raw:
                    continue
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                fp.write(f"{steady_ts()} {line}\n")
                self._parse_line(line)

    def _parse_line(self, line: str) -> None:
        now = time.time()
        cp_connected = CP_CONNECTED_RE.search(line)
        if cp_connected:
            phase = cp_connected.group(1)
            if phase == "B" and self.runtime.cp_duty_pct >= 100:
                self.runtime.cp_phase = "B1"
            elif phase == "B":
                self.runtime.cp_phase = "B2"
            else:
                self.runtime.cp_phase = phase
            if self.runtime.cp_phase == "B1" and self.runtime.b1_since is None:
                self.runtime.b1_since = now
            elif self.runtime.cp_phase != "B1":
                self.runtime.b1_since = None

        cp_disconnect = CP_DISCONNECT_RE.search(line)
        if cp_disconnect:
            self.runtime.cp_phase = cp_disconnect.group(1)
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

        group_match = MODRT_GRP_RE.search(line)
        if group_match:
            self.runtime.group_assigned = int(group_match.group(2))
            self.runtime.group_active = int(group_match.group(3))
            self.runtime.last_group_line = line

        mod_match = MODRT_MOD_RE.search(line)
        if mod_match:
            self.runtime.modules[mod_match.group(1)] = {
                "lease_active": int(mod_match.group(2)),
                "off": int(mod_match.group(3)),
                "fault": int(mod_match.group(4)),
                "online": int(mod_match.group(5)),
                "fresh": int(mod_match.group(6)),
                "voltage_v": float(mod_match.group(7)),
                "current_a": float(mod_match.group(8)),
            }


def bootstrap_controller_mode(plc: PlcSpec, controller_can_id: int, log_path: pathlib.Path) -> None:
    if serial is None:
        raise RuntimeError("pyserial is required for serial bootstrap")
    if not plc.serial_port:
        raise RuntimeError(f"PLC{plc.plc_id} has no serial port configured")

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


class SimpleBorrowRunner:
    def __init__(self, cfg: HarnessConfig) -> None:
        self.cfg = cfg
        self.stop_event = threading.Event()
        self.bus = can.interface.Bus(channel=cfg.can_interface, interface="socketcan")
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
        self.run_log = open(cfg.run_dir / "harness.log", "a", encoding="utf-8", buffering=1)
        self.can_log = open(cfg.run_dir / "can.log", "a", encoding="utf-8", buffering=1)
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
        self.slac_retry_block_until = 0.0
        self.match_attempts = 0
        self.auth_granted = False
        self.borrow_published = False
        self.release_published = False
        self.stop_published = False
        self.bootstrap_done = False
        self.home_published = False
        self.current_plan = "bootstrap"
        self.last_plan_refresh_at = 0.0
        self.stop_deadline: Optional[float] = None

        self.plan_queues: Dict[int, Deque[PendingPlanFrame]] = {
            cfg.plc1.plc_id: collections.deque(),
            cfg.plc2.plc_id: collections.deque(),
        }
        self.plan_inflight: Dict[int, Optional[PendingPlanFrame]] = {
            cfg.plc1.plc_id: None,
            cfg.plc2.plc_id: None,
        }

    def log(self, message: str) -> None:
        line = f"{steady_ts()} {message}"
        print(line, flush=True)
        self.run_log.write(line + "\n")

    def close(self) -> None:
        self.stop_event.set()
        try:
            self.bus.shutdown()
        except Exception:
            pass
        for monitor in self.serial_monitors:
            monitor.join(timeout=1.0)
        self.can_thread.join(timeout=1.0)
        self.run_log.close()
        self.can_log.close()

    def _next_seq(self, name: str) -> int:
        value = getattr(self, name)
        current = 1 if value == 0 else value
        value = (current + 1) & 0xFF
        if value == 0:
            value = 1
        setattr(self, name, value)
        return current

    def _make_frame(self, base_id: int, node_id: int, seq_name: str, b3: int = 0, b4: int = 0, b5: int = 0, b6: int = 0) -> can.Message:
        seq = self._next_seq(seq_name)
        payload = bytearray([CTRL_MSG_VERSION, seq & 0xFF, self.cfg.controller_can_id & 0x0F, b3 & 0xFF, b4 & 0xFF, b5 & 0xFF, b6 & 0xFF, 0])
        payload[7] = crc8_07(payload[:7])
        return can.Message(arbitration_id=with_node(base_id, node_id), is_extended_id=True, data=payload)

    def _send(self, msg: can.Message, note: str) -> None:
        self.bus.send(msg, timeout=0.2)
        self.can_log.write(f"{steady_ts()} TX {msg.arbitration_id:08X}#{bytes(msg.data).hex().upper()} {note}\n")

    def _plan_step(self, msg: can.Message, note: str, ack_cmd: int) -> PendingPlanFrame:
        return PendingPlanFrame(msg=msg, note=note, ack_cmd=ack_cmd, ack_seq=int(msg.data[1]))

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
        self._send(msg, f"AUTH plc={plc.plc_id} state={state}")

    def _send_slac(self, plc: PlcSpec, cmd: int) -> None:
        msg = self._make_frame(
            CAN_ID_CTRL_SLAC,
            plc.can_node_id,
            "seq_slac",
            cmd,
            clamp_100ms(self.cfg.slac_arm_ms),
        )
        self._send(msg, f"SLAC plc={plc.plc_id} cmd={cmd}")

    def _send_session(self, plc: PlcSpec, action: int) -> None:
        msg = self._make_frame(
            CAN_ID_CTRL_SESSION,
            plc.can_node_id,
            "seq_session",
            action,
            clamp_100ms(3000),
            1 if action == CTRL_SESSION_STOP_SOFT else 0,
        )
        self._send(msg, f"SESSION plc={plc.plc_id} action={action}")

    def _assignment_steps(self, plc: PlcSpec, modules: List[ModuleSpec], limit_kw: float, relay2_closed: bool) -> Deque[PendingPlanFrame]:
        steps: Deque[PendingPlanFrame] = collections.deque()
        if not modules:
            abort = self._make_frame(CAN_ID_CTRL_ALLOC_ABORT, plc.can_node_id, "seq_alloc")
            steps.append(self._plan_step(abort, f"ALLOC_ABORT plc={plc.plc_id}", ACK_CMD_ALLOC_ABORT))
            aux = self._make_frame(CAN_ID_CTRL_AUX_RELAY, plc.can_node_id, "seq_aux", 0x01, 0x01 if relay2_closed else 0x00, 0x00)
            steps.append(self._plan_step(aux, f"RELAY2 plc={plc.plc_id} closed={1 if relay2_closed else 0}", ACK_CMD_AUX_RELAY))
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
        steps.append(self._plan_step(begin, f"ALLOC_BEGIN plc={plc.plc_id} modules={len(modules)} limitKW={limit_kw:.1f}", ACK_CMD_ALLOC_BEGIN))
        for module in modules:
            data = self._make_frame(
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
                    data,
                    f"ALLOC_DATA plc={plc.plc_id} moduleId={module.module_id} addr={module.can_address} group={module.can_group} limitKW={module.rated_power_kw:.1f}",
                    ACK_CMD_ALLOC_DATA,
                )
            )
        commit = self._make_frame(CAN_ID_CTRL_ALLOC_COMMIT, plc.can_node_id, "seq_alloc", txn_id)
        steps.append(self._plan_step(commit, f"ALLOC_COMMIT plc={plc.plc_id}", ACK_CMD_ALLOC_COMMIT))
        aux = self._make_frame(CAN_ID_CTRL_AUX_RELAY, plc.can_node_id, "seq_aux", 0x01, 0x01 if relay2_closed else 0x00, 0x00)
        steps.append(self._plan_step(aux, f"RELAY2 plc={plc.plc_id} closed={1 if relay2_closed else 0}", ACK_CMD_AUX_RELAY))
        return steps

    def _queue_plan(self, plc: PlcSpec, modules: List[ModuleSpec], limit_kw: float, relay2_closed: bool) -> None:
        self.plan_queues[plc.plc_id] = self._assignment_steps(plc, modules, limit_kw, relay2_closed)
        self.plan_inflight[plc.plc_id] = None
        self._kick_plan(plc.plc_id)

    def _kick_plan(self, plc_id: int) -> None:
        if self.plan_inflight[plc_id] is not None or not self.plan_queues[plc_id]:
            return
        step = self.plan_queues[plc_id].popleft()
        self.plan_inflight[plc_id] = step
        step.attempts += 1
        step.last_sent_at = time.time()
        self._send(step.msg, step.note)

    def _retry_plans(self) -> None:
        now = time.time()
        for plc_id, inflight in self.plan_inflight.items():
            if inflight is None:
                continue
            if (now - inflight.last_sent_at) < 0.4:
                continue
            if inflight.attempts >= 5:
                self.log(f"[PLAN] plc={plc_id} failed note={inflight.note}")
                self.plan_inflight[plc_id] = None
                self.plan_queues[plc_id].clear()
                continue
            inflight.attempts += 1
            inflight.last_sent_at = now
            self._send(inflight.msg, f"{inflight.note} retry={inflight.attempts}")

    def _plan_idle(self) -> bool:
        return all(value is None for value in self.plan_inflight.values()) and all(not queue for queue in self.plan_queues.values())

    def _spec_for_node(self, node_id: int) -> Optional[PlcSpec]:
        for plc in (self.cfg.plc1, self.cfg.plc2):
            if plc.can_node_id == node_id:
                return plc
        return None

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

    def _can_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                msg = self.bus.recv(timeout=0.2)
            except can.CanError:
                continue
            if msg is None or not msg.is_extended_id or len(msg.data) < 8:
                continue
            self.can_log.write(f"{steady_ts()} RX {msg.arbitration_id:08X}#{bytes(msg.data).hex().upper()}\n")
            self._handle_rx(msg)

    def _handle_rx(self, msg: can.Message) -> None:
        data = bytes(msg.data)
        if crc8_07(data[:7]) != data[7]:
            return
        plc = self._spec_for_node(msg.arbitration_id & 0x0F)
        if plc is None:
            return
        runtime = self.runtimes[plc.plc_id]
        base = msg.arbitration_id & 0x1FFFFFF0

        if base == CAN_ID_PLC_CP_STATUS:
            raw = cp_state_char(data[2])
            duty = int(data[3])
            runtime.cp_duty_pct = duty
            if raw == "B" and duty >= 100:
                runtime.cp_phase = "B1"
            elif raw == "B" and duty > 0:
                runtime.cp_phase = "B2"
            else:
                runtime.cp_phase = raw
            if runtime.cp_phase == "B1" and runtime.b1_since is None:
                runtime.b1_since = time.time()
            elif runtime.cp_phase != "B1":
                runtime.b1_since = None
            return

        if base == CAN_ID_PLC_SLAC_STATUS:
            runtime.session_started = data[2] != 0
            runtime.matched = data[3] != 0
            return

        if base == CAN_ID_PLC_HLC_STATUS:
            runtime.hlc_ready = (data[2] & 0x01) != 0
            runtime.hlc_active = (data[2] & 0x02) != 0
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
            inflight = self.plan_inflight[plc.plc_id]
            if inflight and inflight.ack_cmd == cmd and inflight.ack_seq == seq:
                self.plan_inflight[plc.plc_id] = None
                if status == ACK_OK:
                    self._kick_plan(plc.plc_id)
                else:
                    self.plan_queues[plc.plc_id].clear()

    def _bootstrap(self) -> None:
        self.log("bootstrap_start")
        self._send_session(self.cfg.plc1, CTRL_SESSION_CLEAR)
        self._send_session(self.cfg.plc2, CTRL_SESSION_CLEAR)
        self._send_slac(self.cfg.plc1, CTRL_SLAC_DISARM)
        self._send_auth(self.cfg.plc1, CTRL_AUTH_DENY)
        self._queue_plan(self.cfg.plc1, [], 0.0, False)
        self._queue_plan(self.cfg.plc2, [], 0.0, False)
        self.bootstrap_done = True

    def _publish_home(self) -> None:
        self.log("plan=home")
        self._queue_plan(self.cfg.plc1, [self.cfg.plc1.modules[0]], self.cfg.module_kw, False)
        self._queue_plan(self.cfg.plc2, [], 0.0, False)
        self.home_published = True
        self.current_plan = "home"
        self.last_plan_refresh_at = time.time()

    def _publish_shared(self) -> None:
        self.log("plan=shared")
        self._queue_plan(self.cfg.plc2, [], 0.0, True)
        self._queue_plan(self.cfg.plc1, [self.cfg.plc1.modules[0], self.cfg.plc2.modules[0]], self.cfg.module_kw * 2.0, False)
        self.borrow_published = True
        self.current_plan = "shared"
        self.last_plan_refresh_at = time.time()

    def _publish_release(self) -> None:
        self.log("plan=release")
        self._queue_plan(self.cfg.plc1, [self.cfg.plc1.modules[0]], self.cfg.module_kw, False)
        self._queue_plan(self.cfg.plc2, [], 0.0, False)
        self.release_published = True
        self.current_plan = "release"
        self.last_plan_refresh_at = time.time()

    def _refresh_active_plan(self) -> None:
        if self.current_plan == "shared":
            self._publish_shared()
        elif self.current_plan == "release":
            self._publish_release()
        elif self.current_plan == "home":
            self._publish_home()

    def _write_summary(self) -> None:
        summary = {
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
            "matched": runtime.matched,
            "hlcReady": runtime.hlc_ready,
            "hlcActive": runtime.hlc_active,
            "relay1Closed": runtime.relay1_closed,
            "relay2Closed": runtime.relay2_closed,
            "groupAssigned": runtime.group_assigned,
            "groupActive": runtime.group_active,
            "deliveredVoltageV": runtime.delivered_voltage_v,
            "deliveredCurrentA": runtime.delivered_current_a,
            "deliveredPowerKW": runtime.delivered_power_kw,
            "importedWh": runtime.imported_wh,
            "socPct": runtime.soc_pct,
            "bmsVoltageV": runtime.bms_voltage_v,
            "bmsCurrentA": runtime.bms_current_a,
            "bmsValid": runtime.bms_valid,
            "bmsDeliveryReady": runtime.bms_delivery_ready,
            "evIdentity": runtime.ev_identity,
            "identitySource": runtime.identity_source,
            "chargeStartedAt": runtime.charge_started_at,
            "lastGroupLine": runtime.last_group_line,
            "modules": runtime.modules,
            "acks": runtime.ack_log[-40:],
        }

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
        for monitor in self.serial_monitors:
            monitor.start()
        self.can_thread.start()
        self.log(f"run_dir={self.cfg.run_dir}")
        self._bootstrap()

        try:
            while not self.stop_event.is_set():
                now = time.time()
                plc1 = self.runtimes[self.cfg.plc1.plc_id]
                plc2 = self.runtimes[self.cfg.plc2.plc_id]

                if now - self.last_hb_at >= (self.cfg.heartbeat_period_ms / 1000.0):
                    self._send_heartbeat(self.cfg.plc1)
                    self._send_heartbeat(self.cfg.plc2)
                    self.last_hb_at = now

                self._retry_plans()
                self._kick_plan(self.cfg.plc1.plc_id)
                self._kick_plan(self.cfg.plc2.plc_id)

                if self.bootstrap_done and not self.home_published and plc1.heartbeat_seen and plc2.heartbeat_seen and self._plan_idle():
                    self._publish_home()

                if (
                    self.home_published
                    and not self.stop_published
                    and self._plan_idle()
                    and (now - self.last_plan_refresh_at) >= 2.5
                ):
                    self._refresh_active_plan()

                if self.home_published and not self.auth_granted:
                    if plc1.ev_identity:
                        self._send_auth(self.cfg.plc1, CTRL_AUTH_GRANTED)
                        self.auth_granted = True
                        self.last_auth_at = now
                        self.log(f"identity_seen source={plc1.identity_source} token={plc1.ev_identity}")
                    elif now - self.last_auth_at >= 1.0:
                        self._send_auth(self.cfg.plc1, CTRL_AUTH_PENDING)
                        self.last_auth_at = now
                elif self.auth_granted and now - self.last_auth_at >= 1.0:
                    self._send_auth(self.cfg.plc1, CTRL_AUTH_GRANTED)
                    self.last_auth_at = now

                if (
                    self.home_published
                    and not self.stop_published
                    and self.last_slac_start_at > 0.0
                    and not plc1.matched
                    and not plc1.hlc_ready
                    and (now - self.last_slac_start_at) >= 8.0
                ):
                    self._send_slac(self.cfg.plc1, CTRL_SLAC_DISARM)
                    self.last_slac_arm_at = 0.0
                    self.last_slac_start_at = 0.0
                    self.slac_retry_block_until = now + 1.0
                    self.log("slac_retry reset")

                if (
                    self.home_published
                    and plc1.group_assigned > 0
                    and plc1.cp_phase == "B1"
                    and not plc1.matched
                    and not plc1.hlc_ready
                    and now >= self.slac_retry_block_until
                ):
                    if self.last_slac_arm_at == 0.0 or (now - self.last_slac_arm_at) >= 10.0:
                        self._send_slac(self.cfg.plc1, CTRL_SLAC_ARM)
                        self.last_slac_arm_at = now
                    if plc1.b1_since is not None and self.last_slac_start_at < self.last_slac_arm_at and (now - plc1.b1_since) >= 0.6:
                        self._send_slac(self.cfg.plc1, CTRL_SLAC_START)
                        self.last_slac_start_at = now
                        self.match_attempts += 1
                        self.log(f"slac_start attempt={self.match_attempts}")

                if plc1.charge_started_at is not None:
                    charge_elapsed = now - plc1.charge_started_at
                    if not self.borrow_published and charge_elapsed >= self.cfg.borrow_after_s:
                        self._publish_shared()
                    if not self.release_published and charge_elapsed >= self.cfg.release_after_s:
                        self._publish_release()
                    if not self.stop_published and charge_elapsed >= self.cfg.stop_after_s:
                        self.stop_published = True
                        self.stop_deadline = now + 4.0
                        self.log("soft_stop")
                        self._send_session(self.cfg.plc1, CTRL_SESSION_STOP_SOFT)
                        self._send_auth(self.cfg.plc1, CTRL_AUTH_DENY)
                        self._send_slac(self.cfg.plc1, CTRL_SLAC_DISARM)

                if self.stop_deadline is not None and now >= self.stop_deadline:
                    break

                time.sleep(0.1)
        finally:
            self._write_summary()
        return 0


def load_config(args: argparse.Namespace) -> HarnessConfig:
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

    plc1 = next((plc for plc in plcs if plc.plc_id == 1), plcs[0])
    plc2 = next((plc for plc in plcs if plc.plc_id == 2), plcs[1])

    run_dir = pathlib.Path(
        args.run_dir
        or f"/home/jpi/Desktop/EVSE/plc_firmware/logs/simple_borrow_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    return HarnessConfig(
        can_interface=str(raw["plcTransport"]["canInterface"]),
        controller_can_id=int(raw["plcTransport"]["canControllerId"]),
        heartbeat_period_ms=int(raw["plcContract"]["heartbeatPeriodMs"]),
        heartbeat_timeout_ms=int(raw["plcContract"]["heartbeatTimeoutMs"]),
        assignment_stale_ms=int(raw["plcContract"]["assignmentStaleMs"]),
        slac_arm_ms=int(raw["plcContract"]["slacArmMs"]),
        plc1=plc1,
        plc2=plc2,
        module_kw=float(args.module_kw),
        borrow_after_s=int(args.borrow_after_s),
        release_after_s=int(args.release_after_s),
        stop_after_s=int(args.stop_after_s),
        serial_bootstrap=bool(args.serial_bootstrap),
        serial_log=bool(args.serial_log),
        run_dir=run_dir,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Direct controller-backed CAN borrow/release charging test")
    parser.add_argument(
        "--config",
        default="/home/jpi/Desktop/EVSE/controller/configs/charger-live-2plc.json",
        help="Path to live 2-PLC config JSON",
    )
    parser.add_argument("--module-kw", type=float, default=15.0, help="Temporary per-module rated power")
    parser.add_argument("--borrow-after-s", type=int, default=180, help="Borrow donor module after this many charging seconds")
    parser.add_argument("--release-after-s", type=int, default=300, help="Release donor module after this many charging seconds")
    parser.add_argument("--stop-after-s", type=int, default=600, help="Soft stop after this many charging seconds")
    parser.add_argument(
        "--serial-bootstrap",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Force each PLC into mode=1 over serial before CAN control",
    )
    parser.add_argument("--serial-log", action="store_true", help="Enable passive PLC serial logging")
    parser.add_argument("--run-dir", default="", help="Optional explicit log directory")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    cfg = load_config(args)
    runner = SimpleBorrowRunner(cfg)
    try:
        return runner.run()
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())
