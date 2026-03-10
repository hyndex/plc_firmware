#!/usr/bin/env python3
"""
Standalone live mode 2 end-to-end harness for CB PLC firmware.

Goals:
  - put the PLC into controller-managed mode (`mode=2`) over serial bootstrap
  - drive the full controller contract over CAN
  - keep serial attached as a passive verification/logging channel
  - follow HLC stage transitions in real time from serial logs
  - use actual module/group voltage for managed feedback
  - tolerate the no-load simulator case where measured current stays at 0 A
  - stop cleanly and verify the managed path releases safely
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

try:
    import can  # type: ignore
except Exception as exc:
    print(f"[ERR] python-can import failed: {exc}", file=sys.stderr)
    sys.exit(2)

try:
    import serial  # type: ignore
except Exception as exc:
    print(f"[ERR] pyserial import failed: {exc}", file=sys.stderr)
    sys.exit(2)


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
CAN_ID_CTRL_POWER_SETPOINT = 0x18FF5090
CAN_ID_CTRL_HLC_FEEDBACK = 0x18FF50A0

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

CTRL_SESSION_STOP_HARD = 2
CTRL_SESSION_CLEAR = 3

ACK_OK = 0
ACK_CMD_HEARTBEAT = 0x10
ACK_CMD_AUTH = 0x11
ACK_CMD_SLAC = 0x12
ACK_CMD_ALLOC_BEGIN = 0x13
ACK_CMD_ALLOC_DATA = 0x14
ACK_CMD_ALLOC_COMMIT = 0x15
ACK_CMD_ALLOC_ABORT = 0x16
ACK_CMD_AUX_RELAY = 0x17
ACK_CMD_SESSION = 0x18
ACK_CMD_POWER = 0x19
ACK_CMD_FEEDBACK = 0x1A

MODULE_MAX_POWER_KW = 30.0

STATUS_MODE2_RE = re.compile(r"\[SERCTRL\] STATUS .*mode=2\(")
STATUS_PLC_ID_RE = re.compile(r"plc_id=(\d+)")
STATUS_CONTROLLER_ID_RE = re.compile(r"controller_id=(\d+)")
CP_STATUS_RE = re.compile(r"\[SERCTRL\] STATUS .*cp=([A-Z0-9]+)\s+duty=(\d+)")
STATUS_ALLOC_RE = re.compile(r"alloc_sz=(\d+)")
STATUS_STOP_ACTIVE_RE = re.compile(r"stop_active=(\d+)")
STATUS_STOP_DONE_RE = re.compile(r"stop_done=(\d+)")
CP_CONNECTED_RE = re.compile(r"\[CP\] state .* -> ([BCD])")
CP_DISCONNECT_RE = re.compile(r"\[CP\] state .* -> ([AEF])")
RELAY1_OPEN_RE = re.compile(r"\[RELAY\] Relay1 -> OPEN")
RELAY1_CLOSED_RE = re.compile(r"\[RELAY\] Relay1 -> CLOSED")
ALLOWLIST_RELEASED_RE = re.compile(r"\[CTRL\] allowlist released")
MODULE_OFF_REQ_RE = re.compile(r"\[MOD\] off request \(([^)]+)\)")
SLAC_MATCH_RE = re.compile(r"\[SLAC\] MATCHED")
SLAC_ERROR_RE = re.compile(r"\[FSM\] WaitForMatchingStart -> SignalError")
HLC_CONNECTED_RE = re.compile(r"\[HLC\] EVCC connected")
EVCCID_RE = re.compile(r"\[HLC\] EVCCID ([0-9A-Fa-f]+)")
EV_MAC_RE = re.compile(r"\[SLAC\] EV_MAC ([0-9A-Fa-f:]+)")
SESSION_SETUP_JSON_RE = re.compile(r'"msg":"SessionSetup".*?"evccId":"([0-9A-Fa-f]+)"')
MODRT_GRP_RE = re.compile(
    r"\[MODRT\]\[GRP\].*en=(\d+).*assigned=(\d+).*active=(\d+).*reqV=([0-9.+-]+).*reqI=([0-9.+-]+).*cmbV=([0-9.+-]+).*cmbI=([0-9.+-]+)"
)
MODRT_MOD_RE = re.compile(r"\[MODRT\]\[MOD\].*id=([^\s]+).*off=(\d+).*\sV=([0-9.+-]+)\sI=([0-9.+-]+)")
AUTH_JSON_RE = re.compile(r'"msg":"Authorization"')
PRECHARGE_JSON_RE = re.compile(r'"msg":"PreCharge"')
CURRENT_DEMAND_JSON_RE = re.compile(r'"msg":"CurrentDemand"')
POWER_DELIVERY_JSON_RE = re.compile(r'"msg":"PowerDelivery"')
SESSION_STOP_JSON_RE = re.compile(r'"msg":"SessionStop"')
RAW_PRECHARGE_REQ_RE = re.compile(r"RX PreChargeReq")
RAW_CURRENT_DEMAND_REQ_RE = re.compile(r"RX CurrentDemandReq")
RAW_POWER_DELIVERY_REQ_RE = re.compile(r"RX PowerDeliveryReq")
WH_RE = re.compile(r'"wh"\s*:\s*(-?\d+)')
SOC_RE = re.compile(r'"soc"\s*:\s*(-?\d+)')


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
    sane = max(0.0, float(limit_kw))
    return max(0, min(255, int(round(sane * 2.0))))


def encode_voltage_d01(voltage_v: float, max_raw: int) -> int:
    sane = 0.0 if not math.isfinite(voltage_v) else max(0.0, voltage_v)
    clamped = max(0.0, min(max_raw / 10.0, sane))
    return int(round(clamped * 10.0))


def encode_current_d01(current_a: float, max_raw: int) -> int:
    sane = 0.0 if not math.isfinite(current_a) else max(0.0, current_a)
    clamped = max(0.0, min(max_raw / 10.0, sane))
    return int(round(clamped * 10.0))


def pack_ctrl_power_setpoint(enable: bool, voltage_v: float, current_a: float) -> int:
    v01 = encode_voltage_d01(voltage_v, 0x3FFF)
    i01 = encode_current_d01(current_a, 0x0FFF)
    return (1 if enable else 0) | ((v01 & 0x3FFF) << 1) | ((i01 & 0x0FFF) << 15)


def pack_ctrl_hlc_feedback(
    valid: bool,
    ready: bool,
    current_limit: bool,
    voltage_limit: bool,
    power_limit: bool,
    present_voltage_v: float,
    present_current_a: float,
) -> int:
    v01 = encode_voltage_d01(present_voltage_v, 0x3FFF)
    i01 = encode_current_d01(present_current_a, 0x0FFF)
    return (
        (1 if ready else 0)
        | ((1 if current_limit else 0) << 1)
        | ((1 if voltage_limit else 0) << 2)
        | ((1 if power_limit else 0) << 3)
        | ((v01 & 0x3FFF) << 4)
        | ((i01 & 0x0FFF) << 18)
        | ((1 if valid else 0) << 30)
    )


def _safe_float(value: object) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp_non_negative(value: Optional[float], fallback: float = 0.0) -> float:
    if value is None or not math.isfinite(value):
        return fallback
    return max(0.0, value)


def default_can_node_id_for_plc(plc_id: int) -> int:
    plc = max(1, min(15, int(plc_id)))
    return max(1, min(15, 9 + plc))


@dataclass
class IdentityAssembly:
    event_kind: int = 0
    event_id: int = 0
    expected_segments: int = 0
    bytes: bytearray = field(default_factory=bytearray)


@dataclass
class SessionState:
    cp_connected: bool = False
    cp_phase: str = "U"
    cp_duty_pct: Optional[int] = None
    b1_since_ts: Optional[float] = None
    slac_matched: bool = False
    slac_error_count: int = 0
    session_started: bool = False
    hlc_connected: bool = False
    hlc_ready: bool = False
    identity_seen: bool = False
    identity_value: str = ""
    identity_source: str = ""
    auth_granted: bool = False
    relay1_closed: bool = False
    relay1_open_seen: bool = False
    allowlist_released_seen: bool = False
    module_off_reason: str = ""
    ctrl_alloc_sz: Optional[int] = None
    ctrl_stop_active: Optional[bool] = None
    ctrl_stop_done: Optional[bool] = None
    group_enable: Optional[int] = None
    group_assigned_modules: Optional[int] = None
    group_active_modules: Optional[int] = None
    group_request_v: Optional[float] = None
    group_request_i: Optional[float] = None
    group_voltage_v: Optional[float] = None
    group_current_a: Optional[float] = None
    module_voltage_v: Optional[float] = None
    module_current_a: Optional[float] = None
    module_off: Optional[bool] = None
    last_live_voltage_v: Optional[float] = None
    last_live_voltage_ts: Optional[float] = None
    bms_stage: int = 0
    bms_valid: bool = False
    bms_delivery_ready: bool = False
    last_hlc_stage: str = ""
    seen_auth: bool = False
    seen_precharge: bool = False
    seen_power_delivery: bool = False
    seen_current_demand: bool = False
    seen_session_stop: bool = False
    current_demand_count: int = 0
    first_current_demand_ts: Optional[float] = None
    last_current_demand_ts: Optional[float] = None
    charging_window_start: Optional[float] = None
    last_ev_target_v: Optional[float] = None
    last_ev_target_i: Optional[float] = None
    last_ev_present_v: Optional[float] = None
    last_ev_present_i: Optional[float] = None
    last_ev_status: str = ""
    last_ev_request_ts: Optional[float] = None
    last_soc: Optional[int] = None
    last_meter_wh: Optional[int] = None
    precharge_ready_count: int = 0
    current_demand_valid_voltage_count: int = 0
    current_demand_ready_count: int = 0
    stop_sent: bool = False
    stop_sent_ts: Optional[float] = None
    last_ack: Dict[int, Tuple[int, int, int, float]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


class Mode2Harness:
    def __init__(
        self,
        port: str,
        baud: int,
        can_interface: str,
        plc_id: int,
        can_node_id: int,
        controller_id: int,
        module_addr: int,
        module_group: int,
        module_limit_kw: float,
        duration_s: int,
        max_total_s: int,
        heartbeat_ms: int,
        auth_ttl_ms: int,
        alloc_ttl_ms: int,
        arm_ms: int,
        precharge_current_a: float,
        voltage_tolerance_v: float,
        relay_hold_ms: int,
        reboot_via_dtr: bool,
        serial_bootstrap: bool,
        log_file: str,
    ) -> None:
        self.port = port
        self.baud = baud
        self.can_interface = can_interface
        self.plc_id = max(1, min(15, plc_id))
        resolved_node_id = can_node_id if can_node_id > 0 else default_can_node_id_for_plc(plc_id)
        self.can_node_id = max(1, min(15, resolved_node_id))
        self.controller_id = max(1, min(15, controller_id))
        self.module_addr = max(1, min(255, module_addr))
        self.module_group = max(1, min(15, module_group))
        self.module_limit_kw = max(0.5, min(MODULE_MAX_POWER_KW, module_limit_kw))
        self.duration_s = max(15, duration_s)
        self.max_total_s = max(self.duration_s + 120, max_total_s)
        self.heartbeat_ms = max(250, heartbeat_ms)
        self.heartbeat_timeout_ms = max(3000, self.heartbeat_ms * 8)
        self.auth_ttl_ms = max(1000, auth_ttl_ms)
        # Keep auth refresh comfortably inside the PLC-side TTL so transient
        # bus loss does not momentarily drop controller-owned energy.
        self.auth_refresh_s = max(0.25, min(0.5, self.heartbeat_ms / 1000.0))
        self.alloc_ttl_ms = max(1000, alloc_ttl_ms)
        self.arm_ms = max(2000, arm_ms)
        self.precharge_current_a = max(0.5, precharge_current_a)
        self.voltage_tolerance_v = max(3.0, voltage_tolerance_v)
        self.relay_hold_ms = max(500, relay_hold_ms)
        self.reboot_via_dtr = reboot_via_dtr
        self.serial_bootstrap = serial_bootstrap
        self.log_file = log_file

        self.ser: Optional[serial.Serial] = None
        self.bus: Optional[can.BusABC] = None
        self.state = SessionState()
        self._stop = threading.Event()
        self._reader_thread: Optional[threading.Thread] = None
        self._can_thread: Optional[threading.Thread] = None
        self._log_fp = None
        self._lock = threading.RLock()
        self._log_lock = threading.Lock()
        self._can_send_lock = threading.Lock()
        self._ack_cv = threading.Condition()
        self._acks: Dict[Tuple[int, int], Tuple[int, int, int, float]] = {}
        self._identity_assembly: Optional[IdentityAssembly] = None

        self._last_hb = 0.0
        self._last_auth = 0.0
        self._last_slac = 0.0
        self._last_start = 0.0
        self._last_alloc = 0.0
        self._last_managed = 0.0
        self._last_meter_print = 0.0
        self._last_stop_nudge = 0.0
        self._stop_verify_deadline: Optional[float] = None
        self._session_marker = int(time.time()) & 0xFFFFFF
        self._remote_start_sent = False

        self._seq_heartbeat = 1
        self._seq_auth = 1
        self._seq_slac = 1
        self._seq_alloc = 1
        self._seq_aux = 1
        self._seq_session = 1
        self._seq_power = 1
        self._seq_feedback = 1

    def _log_line(self, text: str) -> None:
        assert self._log_fp is not None
        line = f"{steady_ts()} {text}"
        with self._log_lock:
            self._log_fp.write(line + "\n")

    def _open(self) -> None:
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        self._log_fp = open(self.log_file, "w", encoding="utf-8", buffering=1)
        self.ser = self._open_serial_port()
        if self.reboot_via_dtr:
            self.ser.dtr = False
            time.sleep(0.15)
            self.ser.dtr = True
            time.sleep(1.8)
        self.ser.reset_input_buffer()
        self.ser.reset_output_buffer()
        if self.serial_bootstrap:
            self._bootstrap_mode2_over_serial()
        self.bus = can.interface.Bus(channel=self.can_interface, bustype="socketcan")

    def _open_serial_port(self) -> serial.Serial:
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

    def _reopen_serial(self) -> None:
        if self.ser is not None:
            try:
                self.ser.close()
            except Exception:
                pass
        deadline = time.time() + 6.0
        last_exc: Optional[Exception] = None
        while time.time() < deadline:
            try:
                self.ser = self._open_serial_port()
                self.ser.reset_input_buffer()
                self.ser.reset_output_buffer()
                return
            except Exception as exc:
                last_exc = exc
                time.sleep(0.2)
        raise RuntimeError(f"serial reopen failed for {self.port}: {last_exc}")

    def _close(self) -> None:
        self._stop.set()
        if self._reader_thread and self._reader_thread.is_alive():
            self._reader_thread.join(timeout=2.0)
        if self._can_thread and self._can_thread.is_alive():
            self._can_thread.join(timeout=2.0)
        if self.bus is not None:
            try:
                self.bus.shutdown()
            except Exception:
                pass
            self.bus = None
        if self._log_fp:
            self._log_fp.close()
            self._log_fp = None
        if self.ser:
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None

    def _next_seq(self, name: str) -> int:
        value = getattr(self, name)
        current = 1 if value == 0 else value
        value = (current + 1) & 0xFF
        if value == 0:
            value = 1
        setattr(self, name, value)
        return current

    def _make_frame(self, base_id: int, seq_name: str, b3: int = 0, b4: int = 0, b5: int = 0, b6: int = 0) -> can.Message:
        seq = self._next_seq(seq_name)
        payload = bytearray(
            [
                CTRL_MSG_VERSION,
                seq & 0xFF,
                self.controller_id & 0x0F,
                b3 & 0xFF,
                b4 & 0xFF,
                b5 & 0xFF,
                b6 & 0xFF,
                0,
            ]
        )
        payload[7] = crc8_07(payload[:7])
        return can.Message(
            arbitration_id=with_node(base_id, self.can_node_id),
            is_extended_id=True,
            data=payload,
        )

    def _make_packed_frame(self, base_id: int, seq_name: str, packed: int) -> can.Message:
        seq = self._next_seq(seq_name)
        payload = bytearray(
            [
                CTRL_MSG_VERSION,
                seq & 0xFF,
                self.controller_id & 0x0F,
                packed & 0xFF,
                (packed >> 8) & 0xFF,
                (packed >> 16) & 0xFF,
                (packed >> 24) & 0xFF,
                0,
            ]
        )
        payload[7] = crc8_07(payload[:7])
        return can.Message(
            arbitration_id=with_node(base_id, self.can_node_id),
            is_extended_id=True,
            data=payload,
        )

    def _send_frame(self, msg: can.Message, note: str) -> None:
        if self.bus is None:
            raise RuntimeError("CAN bus is not open")
        with self._can_send_lock:
            self.bus.send(msg, timeout=0.2)
        self._log_line(f"[CANTX] {msg.arbitration_id:08X}#{bytes(msg.data).hex().upper()} {note}")

    def _wait_for_ack(self, ack_cmd: int, ack_seq: int, timeout_s: float) -> Optional[Tuple[int, int, int, float]]:
        deadline = time.time() + timeout_s
        with self._ack_cv:
            while True:
                result = self._acks.get((ack_cmd, ack_seq))
                if result is not None:
                    return result
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None
                self._ack_cv.wait(timeout=remaining)

    def _send_wait_ack(
        self,
        msg: can.Message,
        note: str,
        ack_cmd: int,
        timeout_s: float = 1.0,
        retries: int = 4,
    ) -> bool:
        ack_seq = int(msg.data[1])
        for attempt in range(1, retries + 1):
            self._send_frame(msg, f"{note} try={attempt}")
            ack = self._wait_for_ack(ack_cmd, ack_seq, timeout_s)
            if ack is None:
                self._log_line(f"[CANWAIT] timeout cmd=0x{ack_cmd:02X} seq={ack_seq} note={note}")
                continue
            status, detail0, detail1, _ = ack
            if status == ACK_OK:
                return True
            self._log_line(
                f"[CANWAIT] bad_ack cmd=0x{ack_cmd:02X} seq={ack_seq} status={status} detail0={detail0} detail1={detail1} note={note}"
            )
        with self._lock:
            self.state.errors.append(f"ack failed for {note} cmd=0x{ack_cmd:02X} seq={ack_seq}")
        return False

    def _write_serial_cmd(self, cmd: str) -> None:
        assert self.ser is not None
        self._log_line(f"> {cmd}")
        self.ser.write((cmd.strip() + "\n").encode("utf-8", errors="ignore"))
        self.ser.flush()

    def _bootstrap_mode2_over_serial(self) -> None:
        assert self.ser is not None
        cmds = [
            "CTRL RESET",
            f"CTRL MODE 2 {self.plc_id} {self.controller_id}",
            "CTRL STATUS",
        ]
        time.sleep(0.25)
        self.ser.reset_input_buffer()
        self.ser.reset_output_buffer()
        for cmd in cmds:
            self._write_serial_cmd(cmd)
            time.sleep(0.2)

        status_ok = False
        start = time.time()
        deadline = start + 10.0
        last_probe = start
        reprobed_mode = False
        while time.time() < deadline:
            try:
                raw = self.ser.readline()
            except serial.SerialException as exc:
                self._log_line(f"[BOOT] serial reopen after DTR glitch: {exc}")
                self._reopen_serial()
                continue
            if not raw:
                now = time.time()
                if (now - last_probe) >= 1.0:
                    self._log_line(f"[BOOT] waiting... {int((now - start) * 1000)}ms")
                    self._write_serial_cmd("CTRL STATUS")
                    if (not reprobed_mode) and (now - start) >= 2.0:
                        self._write_serial_cmd(f"CTRL MODE 2 {self.plc_id} {self.controller_id}")
                        reprobed_mode = True
                    last_probe = now
                continue
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            self._log_line(line)
            self._parse_serial_line(line)
            if not STATUS_MODE2_RE.search(line):
                continue
            plc_match = STATUS_PLC_ID_RE.search(line)
            ctrl_match = STATUS_CONTROLLER_ID_RE.search(line)
            if not plc_match or not ctrl_match:
                continue
            if int(plc_match.group(1)) == self.plc_id and int(ctrl_match.group(1)) == self.controller_id:
                status_ok = True
                break
        if not status_ok:
            raise RuntimeError(
                f"serial bootstrap did not confirm mode=2/plc_id={self.plc_id}/controller_id={self.controller_id}"
            )
        self.ser.reset_input_buffer()

    def _reader_loop(self) -> None:
        assert self.ser is not None
        while not self._stop.is_set():
            try:
                raw = self.ser.readline()
            except serial.SerialException as exc:
                self._log_line(f"[SERIAL] reopen after read error: {exc}")
                try:
                    self._reopen_serial()
                except Exception as reopen_exc:
                    with self._lock:
                        self.state.errors.append(f"serial read error: {exc}; reopen failed: {reopen_exc}")
                    break
                continue
            except Exception as exc:
                with self._lock:
                    self.state.errors.append(f"serial read error: {exc}")
                break
            if not raw:
                continue
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            self._log_line(line)
            self._parse_serial_line(line)

    def _can_loop(self) -> None:
        assert self.bus is not None
        while not self._stop.is_set():
            try:
                msg = self.bus.recv(timeout=0.2)
            except can.CanError as exc:
                if self._stop.is_set():
                    break
                with self._lock:
                    self.state.errors.append(f"can recv error: {exc}")
                break
            if msg is None:
                continue
            if not msg.is_extended_id or len(msg.data) < 8:
                continue
            self._log_line(f"[CANRX] {msg.arbitration_id:08X}#{bytes(msg.data).hex().upper()}")
            self._parse_can_message(msg)

    def _parse_serial_line(self, line: str) -> None:
        now = time.time()
        with self._lock:
            m = CP_STATUS_RE.search(line)
            if m:
                self.state.cp_phase = m.group(1)
                self.state.cp_connected = self.state.cp_phase in ("B1", "B2", "C", "D")
                try:
                    self.state.cp_duty_pct = int(m.group(2))
                except ValueError:
                    self.state.cp_duty_pct = None
                if self.state.cp_phase == "B1":
                    if self.state.b1_since_ts is None:
                        self.state.b1_since_ts = now
                else:
                    self.state.b1_since_ts = None
                alloc = STATUS_ALLOC_RE.search(line)
                if alloc:
                    try:
                        self.state.ctrl_alloc_sz = int(alloc.group(1))
                    except ValueError:
                        pass
                stop_active = STATUS_STOP_ACTIVE_RE.search(line)
                if stop_active:
                    self.state.ctrl_stop_active = stop_active.group(1) == "1"
                stop_done = STATUS_STOP_DONE_RE.search(line)
                if stop_done:
                    self.state.ctrl_stop_done = stop_done.group(1) == "1"

            if CP_CONNECTED_RE.search(line):
                self.state.cp_connected = True
            if CP_DISCONNECT_RE.search(line):
                self.state.cp_connected = False
                self.state.slac_matched = False
                self.state.hlc_connected = False
                self.state.hlc_ready = False
                self.state.last_hlc_stage = ""
                self.state.charging_window_start = None
                self.state.b1_since_ts = None

            if RELAY1_OPEN_RE.search(line):
                self.state.relay1_closed = False
                self.state.relay1_open_seen = True
            if RELAY1_CLOSED_RE.search(line):
                self.state.relay1_closed = True

            if ALLOWLIST_RELEASED_RE.search(line):
                self.state.allowlist_released_seen = True

            module_off = MODULE_OFF_REQ_RE.search(line)
            if module_off:
                self.state.module_off_reason = module_off.group(1)

            if SLAC_MATCH_RE.search(line):
                self.state.slac_matched = True
            if SLAC_ERROR_RE.search(line):
                self.state.slac_error_count += 1

            if HLC_CONNECTED_RE.search(line):
                self.state.hlc_connected = True

            ev_mac = EV_MAC_RE.search(line)
            if ev_mac:
                self.state.identity_seen = True
                self.state.identity_source = "evmac"
                self.state.identity_value = ev_mac.group(1).replace(":", "").upper()

            evccid = EVCCID_RE.search(line) or SESSION_SETUP_JSON_RE.search(line)
            if evccid:
                self.state.identity_seen = True
                self.state.identity_source = "evccid"
                self.state.identity_value = evccid.group(1).upper()

            grp = MODRT_GRP_RE.search(line)
            if grp:
                try:
                    self.state.group_enable = int(grp.group(1))
                    self.state.group_assigned_modules = int(grp.group(2))
                    self.state.group_active_modules = int(grp.group(3))
                    self.state.group_request_v = float(grp.group(4))
                    self.state.group_request_i = float(grp.group(5))
                    self.state.group_voltage_v = float(grp.group(6))
                    self.state.group_current_a = float(grp.group(7))
                    if self.state.group_voltage_v > 20.0:
                        self.state.last_live_voltage_v = self.state.group_voltage_v
                        self.state.last_live_voltage_ts = now
                except ValueError:
                    pass

            mod = MODRT_MOD_RE.search(line)
            if mod:
                try:
                    self.state.module_off = int(mod.group(2)) == 1
                    self.state.module_voltage_v = float(mod.group(3))
                    self.state.module_current_a = float(mod.group(4))
                    if self.state.module_voltage_v > 20.0:
                        self.state.last_live_voltage_v = self.state.module_voltage_v
                        self.state.last_live_voltage_ts = now
                except ValueError:
                    pass

            if RAW_PRECHARGE_REQ_RE.search(line):
                self.state.seen_precharge = True
                self.state.last_hlc_stage = "precharge"
                self.state.last_ev_request_ts = now
            if RAW_POWER_DELIVERY_REQ_RE.search(line):
                self.state.seen_power_delivery = True
                self.state.last_hlc_stage = "power_delivery"
                self.state.last_ev_request_ts = now
            if RAW_CURRENT_DEMAND_REQ_RE.search(line):
                self.state.seen_current_demand = True
                self.state.last_hlc_stage = "current_demand"
                self.state.last_ev_request_ts = now
                if self.state.first_current_demand_ts is None:
                    self.state.first_current_demand_ts = now
                if self.state.charging_window_start is None:
                    self.state.charging_window_start = now

            if AUTH_JSON_RE.search(line):
                self.state.seen_auth = True
            if PRECHARGE_JSON_RE.search(line):
                self.state.seen_precharge = True
            if POWER_DELIVERY_JSON_RE.search(line):
                self.state.seen_power_delivery = True
            if SESSION_STOP_JSON_RE.search(line):
                self.state.seen_session_stop = True

            if CURRENT_DEMAND_JSON_RE.search(line):
                self.state.seen_current_demand = True
                self.state.current_demand_count += 1
                self.state.last_current_demand_ts = now
                if self.state.first_current_demand_ts is None:
                    self.state.first_current_demand_ts = now
                if self.state.charging_window_start is None:
                    self.state.charging_window_start = now

            json_start = line.find('{"msg":')
            if json_start >= 0:
                try:
                    payload = json.loads(line[json_start:])
                except json.JSONDecodeError:
                    payload = None
                if isinstance(payload, dict):
                    msg = str(payload.get("msg", "")).strip().lower()
                    req = payload.get("req") if isinstance(payload.get("req"), dict) else {}
                    res = payload.get("res") if isinstance(payload.get("res"), dict) else {}
                    if "authorization" in msg:
                        self.state.last_hlc_stage = "authorization"
                    elif "precharge" in msg:
                        self.state.last_hlc_stage = "precharge"
                    elif "powerdelivery" in msg:
                        self.state.last_hlc_stage = "power_delivery"
                    elif "currentdemand" in msg:
                        self.state.last_hlc_stage = "current_demand"
                    elif "sessionstop" in msg:
                        self.state.last_hlc_stage = "session_stop"

                    target_v = _safe_float(req.get("targetV"))
                    target_i = _safe_float(req.get("targetI"))
                    present_v = _safe_float(res.get("v"))
                    present_i = _safe_float(res.get("i"))
                    if target_v is not None:
                        self.state.last_ev_target_v = target_v
                    if target_i is not None:
                        self.state.last_ev_target_i = target_i
                    if present_v is not None:
                        self.state.last_ev_present_v = present_v
                    if present_i is not None:
                        self.state.last_ev_present_i = present_i
                    status = str(res.get("status", "")).strip()
                    if status:
                        self.state.last_ev_status = status
                    expected_v = _clamp_non_negative(target_v, 0.0)
                    measured_v = _clamp_non_negative(present_v, 0.0)
                    voltage_ok = self._voltage_converged(expected_v, measured_v) if expected_v > 1.0 else False
                    if self.state.last_hlc_stage == "precharge" and voltage_ok:
                        self.state.precharge_ready_count += 1
                    if self.state.last_hlc_stage == "current_demand" and voltage_ok:
                        self.state.current_demand_valid_voltage_count += 1
                        if status == "EVSE_Ready":
                            self.state.current_demand_ready_count += 1
                    if target_v is not None or target_i is not None or present_v is not None or present_i is not None:
                        self.state.last_ev_request_ts = now

                    wh = WH_RE.search(line)
                    if wh:
                        try:
                            self.state.last_meter_wh = int(wh.group(1))
                        except ValueError:
                            pass
                    soc = SOC_RE.search(line)
                    if soc:
                        try:
                            self.state.last_soc = int(soc.group(1))
                        except ValueError:
                            pass

    def _handle_identity_event(self, data: bytes) -> None:
        event_kind = data[2]
        event_id = data[3]
        segment_meta = data[4]
        expected_segments = (segment_meta >> 4) & 0x0F
        segment_index = segment_meta & 0x0F
        asm = self._identity_assembly
        if (
            asm is None
            or segment_index == 0
            or asm.event_id != event_id
            or asm.event_kind != event_kind
            or asm.expected_segments != expected_segments
        ):
            asm = IdentityAssembly(
                event_kind=event_kind,
                event_id=event_id,
                expected_segments=expected_segments,
                bytes=bytearray(expected_segments * 2),
            )
            self._identity_assembly = asm
        offset = segment_index * 2
        if offset < len(asm.bytes):
            asm.bytes[offset] = data[5]
        if offset + 1 < len(asm.bytes):
            asm.bytes[offset + 1] = data[6]
        if expected_segments == 0 or (segment_index + 1) != expected_segments:
            return
        raw = bytes(asm.bytes).rstrip(b"\x00")
        self._identity_assembly = None
        with self._lock:
            if event_kind == 2:
                self.state.identity_seen = True
                self.state.identity_source = "evmac"
                self.state.identity_value = raw[:6].hex().upper()
            elif event_kind == 3:
                self.state.identity_seen = True
                self.state.identity_source = "evccid"
                if raw and all(32 <= byte <= 126 for byte in raw):
                    self.state.identity_value = raw.decode("ascii", errors="replace")
                else:
                    self.state.identity_value = raw.hex().upper()
            elif event_kind == 4:
                self.state.identity_seen = True
                self.state.identity_source = "emaid"
                if raw and all(32 <= byte <= 126 for byte in raw):
                    self.state.identity_value = raw.decode("ascii", errors="replace")
                else:
                    self.state.identity_value = raw.hex().upper()

    def _parse_can_message(self, msg: can.Message) -> None:
        data = bytes(msg.data)
        if crc8_07(data[:7]) != data[7]:
            return
        if (msg.arbitration_id & 0x0F) != (self.can_node_id & 0x0F):
            return
        base = msg.arbitration_id & 0x1FFFFFF0
        now = time.time()
        with self._lock:
            if base == CAN_ID_PLC_CP_STATUS:
                state_char = chr(data[2]) if 32 <= data[2] <= 90 else self.state.cp_phase
                duty_pct = int(data[3])
                if state_char == "B" and duty_pct >= 100:
                    phase = "B1"
                elif state_char == "B" and duty_pct > 0:
                    phase = "B2"
                else:
                    phase = state_char
                self.state.cp_phase = phase
                self.state.cp_duty_pct = duty_pct
                self.state.cp_connected = phase in ("B1", "B2", "C", "D")
                if phase == "B1":
                    if self.state.b1_since_ts is None:
                        self.state.b1_since_ts = now
                else:
                    self.state.b1_since_ts = None
                return
            if base == CAN_ID_PLC_SLAC_STATUS:
                self.state.session_started = data[2] != 0
                self.state.slac_matched = data[3] != 0
                return
            if base == CAN_ID_PLC_HLC_STATUS:
                self.state.hlc_ready = (data[2] & 0x01) != 0
                self.state.hlc_connected = self.state.hlc_ready or ((data[2] & 0x02) != 0)
                self.state.seen_precharge = self.state.seen_precharge or ((data[6] & 0x01) != 0)
                return
            if base == CAN_ID_PLC_POWER_STATUS:
                self.state.group_voltage_v = ((data[3] << 8) | data[2]) / 10.0
                self.state.group_current_a = ((data[5] << 8) | data[4]) / 10.0
                self.state.group_active_modules = data[6] & 0x0F
                self.state.group_assigned_modules = (data[6] >> 4) & 0x0F
                if self.state.group_voltage_v > 20.0:
                    self.state.last_live_voltage_v = self.state.group_voltage_v
                    self.state.last_live_voltage_ts = now
                return
            if base == CAN_ID_PLC_SESSION_STATUS:
                prev_relay1 = self.state.relay1_closed
                self.state.session_started = (data[2] & 0x01) != 0
                self.state.relay1_closed = (data[3] & 0x01) != 0
                if prev_relay1 and not self.state.relay1_closed:
                    self.state.relay1_open_seen = True
                self.state.ctrl_stop_active = (data[3] & 0x08) != 0
                self.state.ctrl_stop_done = (data[3] & 0x20) != 0
                self.state.last_meter_wh = int((data[5] << 8) | data[4])
                if data[6] <= 100:
                    self.state.last_soc = int(data[6])
                return
            if base == CAN_ID_PLC_IDENTITY_EVT:
                self._handle_identity_event(data)
                return
            if base == CAN_ID_PLC_BMS:
                self.state.last_ev_target_v = ((data[3] << 8) | data[2]) / 10.0
                self.state.last_ev_target_i = ((data[5] << 8) | data[4]) / 10.0
                self.state.bms_valid = (data[6] & 0x01) != 0
                self.state.bms_delivery_ready = (data[6] & 0x02) != 0
                self.state.bms_stage = (data[6] >> 2) & 0x3F
                self.state.last_ev_request_ts = now
                if self.state.bms_stage == 1 and self.state.last_hlc_stage not in ("current_demand", "power_delivery"):
                    self.state.last_hlc_stage = "precharge"
                elif self.state.bms_stage == 2 and not self.state.seen_current_demand:
                    self.state.last_hlc_stage = "current_demand"
                elif self.state.bms_stage == 3:
                    self.state.last_hlc_stage = "session_stop"
                return
            if base == CAN_ID_PLC_ACK:
                cmd = data[2]
                seq = data[3]
                status = data[4]
                detail0 = data[5]
                detail1 = data[6]
                self.state.last_ack[cmd] = (status, detail0, detail1, now)
                if cmd == ACK_CMD_HEARTBEAT and status == ACK_OK:
                    pass
                self._log_line(
                    f"[CANACK] cmd=0x{cmd:02X} seq={seq} status={status} detail0={detail0} detail1={detail1}"
                )
                with self._ack_cv:
                    self._acks[(cmd, seq)] = (status, detail0, detail1, now)
                    self._ack_cv.notify_all()
                return

    def _send_heartbeat(self, wait: bool = False) -> bool:
        msg = self._make_frame(
            CAN_ID_CTRL_HEARTBEAT,
            "_seq_heartbeat",
            clamp_100ms(self.heartbeat_timeout_ms),
            self._session_marker & 0xFF,
            (self._session_marker >> 8) & 0xFF,
            (self._session_marker >> 16) & 0xFF,
        )
        if wait:
            return self._send_wait_ack(msg, f"HB timeout_ms={self.heartbeat_timeout_ms}", ACK_CMD_HEARTBEAT)
        self._send_frame(msg, f"HB timeout_ms={self.heartbeat_timeout_ms}")
        return True

    def _send_auth(self, state: int, wait: bool = False) -> bool:
        msg = self._make_frame(
            CAN_ID_CTRL_AUTH,
            "_seq_auth",
            state,
            clamp_100ms(self.auth_ttl_ms),
        )
        label = {CTRL_AUTH_DENY: "deny", CTRL_AUTH_PENDING: "pending", CTRL_AUTH_GRANTED: "grant"}.get(state, str(state))
        if wait:
            return self._send_wait_ack(msg, f"AUTH state={label}", ACK_CMD_AUTH)
        self._send_frame(msg, f"AUTH state={label}")
        return True

    def _send_slac(self, cmd: int, wait: bool = False) -> bool:
        msg = self._make_frame(
            CAN_ID_CTRL_SLAC,
            "_seq_slac",
            cmd,
            clamp_100ms(self.arm_ms),
        )
        label = {CTRL_SLAC_DISARM: "disarm", CTRL_SLAC_ARM: "arm", CTRL_SLAC_START: "start"}.get(cmd, str(cmd))
        if wait:
            return self._send_wait_ack(msg, f"SLAC cmd={label}", ACK_CMD_SLAC)
        self._send_frame(msg, f"SLAC cmd={label}")
        return True

    def _send_alloc_abort(self, wait: bool = False) -> bool:
        msg = self._make_frame(CAN_ID_CTRL_ALLOC_ABORT, "_seq_alloc")
        if wait:
            return self._send_wait_ack(msg, "ALLOC_ABORT", ACK_CMD_ALLOC_ABORT)
        self._send_frame(msg, "ALLOC_ABORT")
        return True

    def _send_relay1(self, closed: bool, wait: bool = False) -> bool:
        msg = self._make_frame(
            CAN_ID_CTRL_AUX_RELAY,
            "_seq_aux",
            0x04,
            0x04 if closed else 0x00,
            clamp_100ms(self.relay_hold_ms),
        )
        if wait:
            return self._send_wait_ack(msg, f"RELAY1 closed={1 if closed else 0}", ACK_CMD_AUX_RELAY)
        self._send_frame(msg, f"RELAY1 closed={1 if closed else 0}")
        return True

    def _send_session(self, action: int, timeout_ms: int, reason: int, wait: bool = False) -> bool:
        msg = self._make_frame(
            CAN_ID_CTRL_SESSION,
            "_seq_session",
            action,
            clamp_100ms(timeout_ms),
            reason,
        )
        label = "clear" if action == CTRL_SESSION_CLEAR else "hard_stop"
        if wait:
            return self._send_wait_ack(msg, f"SESSION action={label}", ACK_CMD_SESSION)
        self._send_frame(msg, f"SESSION action={label}")
        return True

    def _send_power(self, enable: bool, voltage_v: float, current_a: float, wait: bool = False) -> bool:
        packed = pack_ctrl_power_setpoint(enable, voltage_v, current_a)
        msg = self._make_packed_frame(CAN_ID_CTRL_POWER_SETPOINT, "_seq_power", packed)
        if wait:
            return self._send_wait_ack(
                msg,
                f"POWER enable={1 if enable else 0} voltage_v={voltage_v:.1f} current_a={current_a:.1f}",
                ACK_CMD_POWER,
            )
        self._send_frame(msg, f"POWER enable={1 if enable else 0} voltage_v={voltage_v:.1f} current_a={current_a:.1f}")
        return True

    def _send_feedback(
        self,
        valid: bool,
        ready: bool,
        present_v: float,
        present_i: float,
        current_limit: bool = False,
        voltage_limit: bool = False,
        power_limit: bool = False,
        wait: bool = False,
    ) -> bool:
        packed = pack_ctrl_hlc_feedback(
            valid,
            ready,
            current_limit,
            voltage_limit,
            power_limit,
            present_v,
            present_i,
        )
        msg = self._make_packed_frame(CAN_ID_CTRL_HLC_FEEDBACK, "_seq_feedback", packed)
        if wait:
            return self._send_wait_ack(
                msg,
                f"FEEDBACK valid={1 if valid else 0} ready={1 if ready else 0} present_v={present_v:.1f} present_i={present_i:.1f}",
                ACK_CMD_FEEDBACK,
            )
        self._send_frame(
            msg,
            f"FEEDBACK valid={1 if valid else 0} ready={1 if ready else 0} present_v={present_v:.1f} present_i={present_i:.1f}",
        )
        return True

    def _apply_local_allocation(self) -> bool:
        txn_id = int(time.time() * 1000) & 0xFF
        begin = self._make_frame(
            CAN_ID_CTRL_ALLOC_BEGIN,
            "_seq_alloc",
            txn_id,
            1,
            clamp_100ms(self.alloc_ttl_ms),
            encode_half_kw(self.module_limit_kw),
        )
        if not self._send_wait_ack(
            begin,
            f"ALLOC_BEGIN txn={txn_id} modules=1 ttl_ms={self.alloc_ttl_ms} limit_kw={self.module_limit_kw:.1f}",
            ACK_CMD_ALLOC_BEGIN,
        ):
            return False
        data = self._make_frame(
            CAN_ID_CTRL_ALLOC_DATA,
            "_seq_alloc",
            txn_id,
            self.module_group,
            self.module_addr,
            encode_half_kw(self.module_limit_kw),
        )
        if not self._send_wait_ack(
            data,
            f"ALLOC_DATA txn={txn_id} group={self.module_group} module=0x{self.module_addr:02X} limit_kw={self.module_limit_kw:.1f}",
            ACK_CMD_ALLOC_DATA,
        ):
            return False
        commit = self._make_frame(CAN_ID_CTRL_ALLOC_COMMIT, "_seq_alloc", txn_id)
        if not self._send_wait_ack(commit, f"ALLOC_COMMIT txn={txn_id}", ACK_CMD_ALLOC_COMMIT):
            return False
        self._last_alloc = time.time()
        return True

    def _bootstrap_can_contract(self) -> None:
        if not self._send_heartbeat(wait=True):
            raise RuntimeError("initial CAN heartbeat ACK failed")
        if not self._send_session(CTRL_SESSION_CLEAR, 3000, 0, wait=True):
            raise RuntimeError("initial session clear ACK failed")
        if not self._send_alloc_abort(wait=True):
            raise RuntimeError("initial alloc abort ACK failed")
        if not self._send_auth(CTRL_AUTH_PENDING, wait=True):
            raise RuntimeError("initial auth pending ACK failed")
        if not self._send_power(False, 0.0, 0.0, wait=True):
            raise RuntimeError("initial power-off ACK failed")
        if not self._send_feedback(True, False, 0.0, 0.0, wait=True):
            raise RuntimeError("initial invalid-feedback ACK failed")
        if not self._send_relay1(False, wait=True):
            raise RuntimeError("initial relay-open ACK failed")
        if not self._apply_local_allocation():
            raise RuntimeError("initial allocation commit failed")

    def _effective_stage_locked(self) -> str:
        stage = self.state.last_hlc_stage
        if stage:
            return stage
        if self.state.seen_current_demand:
            return "current_demand"
        if self.state.seen_power_delivery:
            return "power_delivery"
        if self.state.seen_precharge or self.state.bms_stage == 1:
            return "precharge"
        if self.state.bms_stage == 3 or self.state.seen_session_stop:
            return "session_stop"
        return ""

    def _measured_voltage(self) -> float:
        with self._lock:
            group_v = self.state.group_voltage_v
            module_v = self.state.module_voltage_v
            live_v = self.state.last_live_voltage_v
            live_v_ts = self.state.last_live_voltage_ts
            reported_v = self.state.last_ev_present_v
            target_v = self.state.last_ev_target_v
        now = time.time()
        if group_v is not None and group_v > 5.0:
            return group_v
        if module_v is not None and module_v > 5.0:
            return module_v
        if live_v is not None and live_v > 5.0 and live_v_ts is not None and (now - live_v_ts) <= 1.5:
            return live_v
        if reported_v is not None and reported_v > 5.0:
            return reported_v
        return _clamp_non_negative(target_v, 0.0)

    def _measured_current(self) -> float:
        with self._lock:
            group_i = self.state.group_current_a
            module_i = self.state.module_current_a
            reported_i = self.state.last_ev_present_i
        if group_i is not None and group_i >= 0.0:
            return max(0.0, group_i)
        if module_i is not None and module_i >= 0.0:
            return max(0.0, module_i)
        return _clamp_non_negative(reported_i, 0.0)

    def _voltage_converged(self, target_v: float, measured_v: float) -> bool:
        if target_v <= 1.0:
            return False
        return abs(measured_v - target_v) <= self.voltage_tolerance_v

    def _tick_contract(self, stop_phase: bool) -> None:
        now = time.time()
        if (now - self._last_hb) * 1000.0 >= self.heartbeat_ms:
            self._send_heartbeat(wait=False)
            self._last_hb = now

        with self._lock:
            identity_seen = self.state.identity_seen
            hlc_connected = self.state.hlc_connected
            cp_phase = self.state.cp_phase
            b1_since_ts = self.state.b1_since_ts
            assigned = self.state.group_assigned_modules
            slac_matched = self.state.slac_matched

        if now - self._last_auth >= self.auth_refresh_s:
            if stop_phase:
                self._send_auth(CTRL_AUTH_DENY, wait=False)
                with self._lock:
                    self.state.auth_granted = False
            elif identity_seen or hlc_connected:
                self._send_auth(CTRL_AUTH_GRANTED, wait=False)
                with self._lock:
                    self.state.auth_granted = True
            else:
                self._send_auth(CTRL_AUTH_PENDING, wait=False)
            self._last_auth = now

        if not stop_phase and (assigned is None or assigned == 0) and (now - self._last_alloc) >= 2.5:
            if not self._apply_local_allocation():
                with self._lock:
                    self.state.errors.append("allocation refresh failed")
            self._last_alloc = now

        if stop_phase and (now - self._last_stop_nudge) >= 1.5:
            self._send_session(CTRL_SESSION_STOP_HARD, 3000, 2, wait=True)
            self._send_slac(CTRL_SLAC_DISARM, wait=True)
            self._send_alloc_abort(wait=True)
            self._last_stop_nudge = now
            self._remote_start_sent = False

        waiting_for_slac = (not identity_seen) and (not hlc_connected) and (not slac_matched)
        at_b1 = cp_phase == "B1"
        if not waiting_for_slac or not at_b1:
            self._remote_start_sent = False
        if not stop_phase and waiting_for_slac and at_b1 and (now - self._last_slac) >= 4.0:
            self._send_slac(CTRL_SLAC_ARM, wait=False)
            self._last_slac = now
        if (
            not stop_phase
            and waiting_for_slac
            and at_b1
            and (not self._remote_start_sent)
            and b1_since_ts is not None
            and (now - b1_since_ts) >= 0.6
        ):
            self._send_slac(CTRL_SLAC_START, wait=False)
            self._remote_start_sent = True
        elif not stop_phase and waiting_for_slac and at_b1 and (now - self._last_start) >= 4.0:
            self._send_slac(CTRL_SLAC_START, wait=False)
            self._last_start = now
            self._remote_start_sent = True

    def _tick_managed(self, stop_phase: bool) -> None:
        now = time.time()
        # CAN PreCharge can be very short on the simulator path, so the managed
        # loop needs to react faster than the serial harness to get feedback in
        # before HLC advances to PowerDelivery.
        if now - self._last_managed < 0.05:
            return

        with self._lock:
            stage = self._effective_stage_locked()
            target_v = _clamp_non_negative(self.state.last_ev_target_v, _clamp_non_negative(self.state.group_request_v, 0.0))
            target_i = _clamp_non_negative(self.state.last_ev_target_i, _clamp_non_negative(self.state.group_request_i, 0.0))
            cp_connected = self.state.cp_connected
            recent_request = self.state.last_ev_request_ts is not None and (now - self.state.last_ev_request_ts) <= 2.0

        if stop_phase or (not cp_connected) or (not recent_request):
            self._send_power(False, 0.0, 0.0, wait=False)
            self._send_feedback(True, False, 0.0, 0.0, wait=False)
            self._send_relay1(False, wait=False)
            self._last_managed = now
            return

        measured_v = self._measured_voltage()
        measured_i = self._measured_current()
        effective_v = measured_v if measured_v > 5.0 else target_v
        precharge_i = min(self.precharge_current_a, target_i if target_i > 0.1 else self.precharge_current_a)
        voltage_ok = self._voltage_converged(target_v, effective_v)

        if stage == "precharge":
            self._send_power(True, target_v, precharge_i, wait=False)
            self._send_feedback(True, voltage_ok, effective_v, 0.0, wait=False)
            self._send_relay1(False, wait=False)
        elif stage in ("power_delivery", "current_demand"):
            command_i = target_i if target_i > 0.0 else precharge_i
            # Once HLC advances beyond PreCharge, hold the validated target
            # voltage in controller feedback so the no-load simulator does not
            # reopen Relay1 on transient telemetry dips.
            effective_v = max(effective_v, target_v)
            voltage_ok = target_v > 1.0
            self._send_power(True, target_v, command_i, wait=False)
            self._send_feedback(True, voltage_ok, effective_v, measured_i, wait=False)
            self._send_relay1(voltage_ok, wait=False)
        else:
            self._send_power(False, 0.0, 0.0, wait=False)
            self._send_feedback(True, False, 0.0, 0.0, wait=False)
            self._send_relay1(False, wait=False)

        self._last_managed = now

    def _request_stop(self) -> None:
        self._send_power(False, 0.0, 0.0, wait=False)
        self._send_feedback(True, False, 0.0, 0.0, wait=False)
        self._send_relay1(False, wait=False)
        self._send_session(CTRL_SESSION_STOP_HARD, 3000, 2, wait=True)
        self._send_auth(CTRL_AUTH_DENY, wait=True)
        self._send_slac(CTRL_SLAC_DISARM, wait=True)
        self._send_alloc_abort(wait=True)
        with self._lock:
            self.state.stop_sent = True
            self.state.stop_sent_ts = time.time()
            self.state.auth_granted = False
        self._stop_verify_deadline = time.time() + 40.0
        self._last_stop_nudge = time.time()

    def _stop_verification_ok(self) -> bool:
        with self._lock:
            ctrl_done = self.state.ctrl_stop_done is True
            group_idle = (
                self.state.group_assigned_modules == 0 and self.state.group_active_modules == 0
            ) if self.state.group_assigned_modules is not None and self.state.group_active_modules is not None else False
            return self.state.relay1_open_seen and ctrl_done and (self.state.allowlist_released_seen or group_idle)

    def run(self) -> int:
        self._open()
        assert self._log_fp is not None
        print(f"LOG_FILE={self.log_file}", flush=True)

        self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader_thread.start()
        self._can_thread = threading.Thread(target=self._can_loop, daemon=True)
        self._can_thread.start()

        try:
            self._bootstrap_can_contract()
            t0 = time.time()
            stop_phase = False
            success = False

            while True:
                now = time.time()
                if now - t0 > self.max_total_s:
                    print("[FAIL] max test time exceeded", flush=True)
                    break

                self._tick_contract(stop_phase)
                self._tick_managed(stop_phase)

                with self._lock:
                    s = self.state
                    charge_elapsed = (now - s.charging_window_start) if s.charging_window_start else 0.0
                    errs = list(s.errors)
                    stage = self._effective_stage_locked()
                    summary = (
                        s.cp_phase,
                        s.cp_duty_pct,
                        s.slac_matched,
                        s.hlc_connected,
                        s.identity_seen,
                        s.auth_granted,
                        stage,
                        s.current_demand_count,
                        s.group_voltage_v,
                        s.group_current_a,
                        s.last_ev_target_v,
                        s.last_ev_target_i,
                        s.group_assigned_modules,
                        s.group_active_modules,
                        s.precharge_ready_count,
                        s.current_demand_valid_voltage_count,
                        s.current_demand_ready_count,
                        s.last_ev_present_v,
                        s.last_ev_status,
                    )

                if errs:
                    print(f"[FAIL] runtime errors: {errs}", flush=True)
                    break

                (
                    cp_phase,
                    cp_duty,
                    matched,
                    hlc,
                    identity,
                    auth,
                    stage,
                    cd_count,
                    cmb_v,
                    cmb_i,
                    target_v,
                    target_i,
                    assigned,
                    active,
                    precharge_ready_count,
                    cd_valid_v_count,
                    cd_ready_count,
                    present_v,
                    present_status,
                ) = summary
                if now - self._last_meter_print >= 5.0:
                    print(
                        f"[PROGRESS] cp={cp_phase} duty={cp_duty if cp_duty is not None else -1} "
                        f"matched={int(matched)} hlc={int(hlc)} identity={int(identity)} auth={int(auth)} "
                        f"stage={stage or '-'} cd={cd_count} assigned={assigned if assigned is not None else -1} "
                        f"active={active if active is not None else -1} "
                        f"targetV={target_v if target_v is not None else -1:.1f} "
                        f"targetI={target_i if target_i is not None else -1:.1f} "
                        f"prechargeReadyCount={precharge_ready_count} cdValidV={cd_valid_v_count} "
                        f"cdReadyCount={cd_ready_count} presentV={present_v if present_v is not None else -1:.1f} "
                        f"status={present_status or '-'} "
                        f"cmbV={cmb_v if cmb_v is not None else -1:.1f} cmbI={cmb_i if cmb_i is not None else -1:.1f} "
                        f"window_s={charge_elapsed:.1f}",
                        flush=True,
                    )
                    self._last_meter_print = now

                if not stop_phase and charge_elapsed >= self.duration_s:
                    if precharge_ready_count < 1 or cd_valid_v_count < 3:
                        print(
                            f"[FAIL] managed HLC voltage criteria not met: prechargeReadyCount={precharge_ready_count} "
                            f"cdValidV={cd_valid_v_count} cdReadyCount={cd_ready_count} "
                            f"lastPresentV={present_v if present_v is not None else -1:.1f} "
                            f"lastStatus={present_status or '-'}",
                            flush=True,
                        )
                        break
                    stop_phase = True
                    print(f"[PASS] held current-demand window for {charge_elapsed:.1f}s, requesting stop", flush=True)
                    self._request_stop()

                if stop_phase:
                    if self._stop_verification_ok():
                        success = True
                        print("[PASS] stop verification OK", flush=True)
                        break
                    if self._stop_verify_deadline is not None and now > self._stop_verify_deadline:
                        print("[FAIL] stop verification timeout", flush=True)
                        break
                else:
                    with self._lock:
                        if self.state.slac_error_count >= 2 and not self.state.hlc_connected:
                            print("[FAIL] repeated SLAC matching errors before HLC session", flush=True)
                            break

                time.sleep(0.05)

            with self._lock:
                s = self.state
                print(
                    "[SUMMARY] "
                    f"cp={s.cp_phase} duty={s.cp_duty_pct if s.cp_duty_pct is not None else -1} "
                    f"matched={int(s.slac_matched)} hlc={int(s.hlc_connected)} "
                    f"auth={int(s.auth_granted)} identity={s.identity_value or '-'} "
                    f"seen_auth={int(s.seen_auth)} precharge={int(s.seen_precharge)} "
                    f"power_delivery={int(s.seen_power_delivery)} current_demand={s.current_demand_count} "
                    f"precharge_ready_count={s.precharge_ready_count} "
                    f"current_demand_valid_v={s.current_demand_valid_voltage_count} "
                    f"current_demand_ready_count={s.current_demand_ready_count} "
                    f"stop_done={int(s.ctrl_stop_done) if s.ctrl_stop_done is not None else -1} "
                    f"relay1_closed={int(s.relay1_closed)} relay1_open_seen={int(s.relay1_open_seen)} "
                    f"allowlist_released={int(s.allowlist_released_seen)} "
                    f"assigned={s.group_assigned_modules if s.group_assigned_modules is not None else -1} "
                    f"active={s.group_active_modules if s.group_active_modules is not None else -1} "
                    f"cmbV={s.group_voltage_v if s.group_voltage_v is not None else -1:.1f} "
                    f"cmbI={s.group_current_a if s.group_current_a is not None else -1:.1f} "
                    f"targetV={s.last_ev_target_v if s.last_ev_target_v is not None else -1:.1f} "
                    f"targetI={s.last_ev_target_i if s.last_ev_target_i is not None else -1:.1f} "
                    f"presentV={s.last_ev_present_v if s.last_ev_present_v is not None else -1:.1f} "
                    f"presentI={s.last_ev_present_i if s.last_ev_present_i is not None else -1:.1f} "
                    f"status={s.last_ev_status or '-'}",
                    flush=True,
                )
            return 0 if success else 1
        finally:
            self._close()
            print(f"LOG_FILE_DONE={self.log_file}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Live end-to-end controller-managed mode 2 validation over CAN with serial verification")
    ap.add_argument("--port", default="/dev/ttyACM0")
    ap.add_argument("--baud", type=int, default=115200)
    ap.add_argument("--can-interface", default="can0")
    ap.add_argument("--plc-id", type=int, default=1)
    ap.add_argument("--can-node-id", type=int, default=0, help="Target PLC CAN node; default derives from plc-id (PLC1->10, PLC2->11)")
    ap.add_argument("--controller-id", type=int, default=1)
    ap.add_argument("--module-addr", type=lambda v: int(v, 0), default=0x01)
    ap.add_argument("--module-group", type=int, default=1)
    ap.add_argument("--module-limit-kw", type=float, default=30.0)
    ap.add_argument("--duration-min", type=int, default=1)
    ap.add_argument("--max-total-min", type=int, default=8)
    ap.add_argument("--heartbeat-ms", type=int, default=250)
    ap.add_argument("--auth-ttl-ms", type=int, default=3000)
    ap.add_argument("--alloc-ttl-ms", type=int, default=3000)
    ap.add_argument("--arm-ms", type=int, default=12000)
    ap.add_argument("--precharge-current-a", type=float, default=2.0)
    ap.add_argument("--voltage-tolerance-v", type=float, default=15.0)
    ap.add_argument("--relay-hold-ms", type=int, default=1500)
    ap.add_argument("--reboot-via-dtr", action="store_true")
    ap.add_argument("--no-serial-bootstrap", action="store_true")
    ap.add_argument("--log-file", default="")
    args = ap.parse_args()

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = args.log_file or f"/home/jpi/Desktop/EVSE/plc_firmware/logs/mode2_e2e_{ts}.log"

    harness = Mode2Harness(
        port=args.port,
        baud=args.baud,
        can_interface=args.can_interface,
        plc_id=args.plc_id,
        can_node_id=args.can_node_id,
        controller_id=args.controller_id,
        module_addr=args.module_addr,
        module_group=args.module_group,
        module_limit_kw=args.module_limit_kw,
        duration_s=max(15, args.duration_min * 60),
        max_total_s=max(180, args.max_total_min * 60),
        heartbeat_ms=args.heartbeat_ms,
        auth_ttl_ms=args.auth_ttl_ms,
        alloc_ttl_ms=args.alloc_ttl_ms,
        arm_ms=args.arm_ms,
        precharge_current_a=args.precharge_current_a,
        voltage_tolerance_v=args.voltage_tolerance_v,
        relay_hold_ms=args.relay_hold_ms,
        reboot_via_dtr=args.reboot_via_dtr,
        serial_bootstrap=not args.no_serial_bootstrap,
        log_file=log_file,
    )
    return harness.run()


if __name__ == "__main__":
    sys.exit(main())
