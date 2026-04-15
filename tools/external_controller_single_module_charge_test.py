#!/usr/bin/env python3
"""
Minimal external-controller single-module charge harness.

Purpose:
  - Run only PLC1 / connector 1 in `mode=1` (`external_controller`).
  - Drive only module 1 / group 1 directly over CAN.
  - Follow the same practical power-path shape that works in standalone:
      SLAC -> HLC -> PreCharge -> PowerDelivery -> CurrentDemand
  - Keep the controller contract simple:
      HB + AUTH grant + one SLAC start per attach + Relay1 + FEEDBACK

This intentionally avoids donor/share logic, multi-session orchestration, and
Python-side SLAC recovery churn. It lets the PLC retry SLAC naturally while the
controller contract stays alive.
"""

from __future__ import annotations

import argparse
from collections import deque
import datetime as dt
import glob
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Deque, Optional

try:
    import serial  # type: ignore
except Exception as exc:
    print(f"[ERR] pyserial import failed: {exc}", file=sys.stderr)
    sys.exit(2)


ROOT_DIR = Path(__file__).resolve().parents[2]
DC_OCPP_SCRIPTS = ROOT_DIR / "DC-OCPP" / "scripts"
if not DC_OCPP_SCRIPTS.exists():
    print(f"[ERR] Maxwell helper path not found: {DC_OCPP_SCRIPTS}", file=sys.stderr)
    sys.exit(2)
sys.path.insert(0, str(DC_OCPP_SCRIPTS))
try:
    import maxwell_test as mxr  # type: ignore
except Exception as exc:
    print(f"[ERR] failed importing DC-OCPP Maxwell helpers: {exc}", file=sys.stderr)
    sys.exit(2)


ACK_OK = 0
ACK_BAD_SEQ = 4
ACK_CMD_RELAY = 0x17
ACK_CMD_STOP = 0x18
ACK_CMD_HEARTBEAT = 0x10
ACK_CMD_AUTH = 0x11
ACK_CMD_SLAC = 0x12
ACK_CMD_FEEDBACK = 0x1A
DEFAULT_ESP_RESET_PULSE_S = 0.2
DEFAULT_ESP_RESET_WAIT_S = 14.0
SERIAL_READ_TIMEOUT_S = 0.02
SERIAL_INTER_BYTE_TIMEOUT_S = 0.002
SERIAL_WRITE_TIMEOUT_S = 0.2
SERIAL_MAX_PENDING_BYTES = 65536
SERIAL_MAX_LINE_BYTES = 4096
MXR_DEFAULT_OUTPUT_POWER_KW = 30.0
MODULE_MAX_VOLTAGE_V = 1000.0
DEFAULT_HOME_ADDR = 1
DEFAULT_HOME_GROUP = 1
DEFAULT_MODULE_VOLTAGE_HEADROOM_V = 10.0
DEFAULT_MODULE_CURRENT_HEADROOM_A = 1.0
DEFAULT_FAKE_WELDING_RESIDUAL_V = 15.0
DEFAULT_LOW_REQUEST_RUNDOWN_MAX_A = 4.0
DEFAULT_LOW_REQUEST_READY_OVERSHOOT_MAX_A = 4.0
DEFAULT_ZERO_REQUEST_READY_MAX_A = 4.0
DEFAULT_LOW_REQUEST_RUNDOWN_TRIGGER_DELTA_A = 5.0
DEFAULT_LOW_REQUEST_RUNDOWN_GRACE_S = 1.8
MODULE_WORKER_MIN_INTERVAL_S = 0.005
MODULE_WORKER_TARGET_INTERVAL_S = 0.01
MODULE_ACTIVE_FULL_REFRESH_S = 0.25
MODULE_OFF_RETRY_INTERVAL_S = 0.25


STATUS_RE = re.compile(
    r"(?:\[SERCTRL\] STATUS\s+)?mode=(\d+)\(([^)]+)\)\s+plc_id=(\d+)\s+connector_id=(\d+)\s+"
    r"controller_id=(\d+)\s+module(?:_addr|ddr)=0x([0-9A-Fa-f]+)\s+local_group=(\d+)\s+module_id=([^\s]+)\s+"
    r"can_stack=(\d)\s+module_mgr=(\d)\s+cp=([A-Z0-9]+)\s+(?:cp_mv=-?\d+\s+)?duty=(\d+)\s+hb=(\d+)\s+auth=(\d+)\s+"
    r"allow_slac=(\d+)\s+allow_energy=(\d+)\s+armed=(\d+)\s+start=(\d+)\s+"
    r"relay1=(\d+)(?:\s+relay2=(\d+)\s+relay3=(\d+)\s+alloc_sz=(\d+))?"
)
LOCAL_RE = re.compile(
    r"\[SERCTRL\] LOCAL mode=(\d+)\(([^)]+)\)\s+transport=\S+\s+modules=\S+\s+"
    r"plc_id=(\d+)\s+connector_id=(\d+)\s+controller_id=(\d+)\s+module_addr=0x([0-9A-Fa-f]+)\s+"
    r"local_group=(\d+)\s+module_id=([^\s]+)\s+can_stack=(\d)\s+module_mgr=(\d)"
)
PARTIAL_STATUS_RE = re.compile(
    r"(?:^|\s)(?:id|plc_id)=(\d+)\s+controller_id=(\d+)\s+module(?:_addr|ddr)=0x([0-9A-Fa-f]+)\s+"
    r"local_group=(\d+)\s+module_id=([^\s]+)\s+can_stack=(\d)\s+module_mgr=(\d)\s+"
    r"cp=([A-Z0-9]+)\s+(?:cp_mv=-?\d+\s+)?duty=(\d+)\s+hb=(\d+)\s+auth=(\d+)\s+"
    r"allow_slac=(\d+)\s+allow_energy=(\d+)\s+armed=(\d+)\s+start=(\d+)\s+"
    r"relay1=(\d+)(?:\s+relay2=(\d+)\s+relay3=(\d+)\s+alloc_sz=(\d+))?"
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
HLC_REQ_JSON_RE = re.compile(r"\[HLC\]\[REQ\] (\{.*\})")
POWER_DELIVERY_STOP_RUNTIME_RE = re.compile(r"\[HLC\] \{\"msg\":\"PowerDeliveryRuntime\".*\"stop\":1")
WELDING_RUNTIME_RE = re.compile(r"\[HLC\] \{\"msg\":\"WeldingDetectionRuntime\"")
SESSION_STOP_RUNTIME_RE = re.compile(r"\[HLC\] \{\"msg\":\"SessionStopRuntime\"")
CLIENT_SESSION_DONE_RE = re.compile(r"\[HLC\] client session done rc=(-?\d+)")
LOG_FRAGMENT_RE = re.compile(r"\[(?:SERCTRL|RELAY|CP|CTRL|QCA|SLAC|HLC|FSM|NET|MEM|WARN |HOST)\]")


def ts() -> str:
    return dt.datetime.now().isoformat(timespec="milliseconds")


def ensure_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def module_status_text(module: mxr.ModuleInfo) -> str:
    status = f"0x{module.status:08X}" if module.status is not None else "n/a"
    flags = mxr.status_flags(module.status)
    return f"status={status} flags={','.join(flags) if flags else '-'}"


def module_is_off(module: mxr.ModuleInfo) -> bool:
    return "OFF" in mxr.status_flags(module.status)


def module_online(module: Optional[mxr.ModuleInfo]) -> bool:
    if module is None:
        return False
    last_seen = getattr(module, "last_seen", 0.0) or 0.0
    return last_seen > 0.0 and (
        module.status is not None or module.voltage_v is not None or module.current_a is not None
    )


def module_current_value(module: Optional[mxr.ModuleInfo]) -> float:
    if not module or module.current_a is None or not math.isfinite(module.current_a):
        return 0.0
    return max(0.0, module.current_a)


def module_voltage_value(module: Optional[mxr.ModuleInfo]) -> float:
    if not module or module.voltage_v is None or not math.isfinite(module.voltage_v):
        return 0.0
    return max(0.0, module.voltage_v)


def module_available_current_value(module: Optional[mxr.ModuleInfo]) -> float:
    if not module:
        return 0.0
    rated_current_a = module.rated_current_a
    if rated_current_a is None or not math.isfinite(rated_current_a):
        return 0.0
    return max(0.0, rated_current_a)


@dataclass
class Ack:
    cmd_hex: int
    seq: int
    status: int
    detail0: int
    detail1: int
    received_at: float


@dataclass
class PlcStatus:
    mode_id: int
    mode_name: str
    plc_id: int
    connector_id: int
    controller_id: int
    module_addr: int
    local_group: int
    module_id: str
    can_stack: int
    module_mgr: int
    cp: str
    duty: int
    relay1: int
    relay2: int
    relay3: int
    alloc_sz: int

    def relay_state(self, relay_idx: int) -> int:
        return getattr(self, f"relay{relay_idx}")


class SerialLink:
    def __init__(self, port: str, baud: int, log_file: str, parse_line) -> None:
        self.port = port
        self.baud = baud
        self.log_file = log_file
        self.parse_line = parse_line
        self.ser: Optional[serial.Serial] = None
        self.log_fp = None
        self.stop_event = threading.Event()
        self.reader_thread: Optional[threading.Thread] = None
        self.reader_error: Optional[str] = None
        self.write_lock = threading.Lock()
        self.reconnect_lock = threading.Lock()
        self.last_write_ts = 0.0
        self.pending_status_line = ""
        self.rx_buffer = bytearray()

    def _build_serial(self) -> serial.Serial:
        kwargs = {
            "port": self.port,
            "baudrate": self.baud,
            "timeout": SERIAL_READ_TIMEOUT_S,
            "write_timeout": SERIAL_WRITE_TIMEOUT_S,
            "inter_byte_timeout": SERIAL_INTER_BYTE_TIMEOUT_S,
        }
        try:
            ser = serial.Serial(exclusive=True, **kwargs)
        except TypeError:
            ser = serial.Serial(**kwargs)
        return ser

    def _close_serial_locked(self) -> None:
        if self.ser:
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None

    def _reconnect(self, reason: str, timeout_s: float = 12.0) -> bool:
        with self.reconnect_lock:
            deadline = time.time() + max(1.0, timeout_s)
            last_exc: Optional[Exception] = None
            self._log(f"{ts()} [HOST] reconnecting {self.port}: {reason}")
            with self.write_lock:
                self._close_serial_locked()
                self.last_write_ts = 0.0
            time.sleep(0.1)
            while not self.stop_event.is_set() and time.time() < deadline:
                try:
                    ser = self._build_serial()
                except Exception as exc:
                    last_exc = exc
                    time.sleep(0.25)
                    continue
                with self.write_lock:
                    self._close_serial_locked()
                    self.ser = ser
                    self.last_write_ts = 0.0
                self.reader_error = None
                self.pending_status_line = ""
                self.rx_buffer = bytearray()
                self._log(f"{ts()} [HOST] reconnected {self.port}")
                return True
            detail = f"{reason}: {last_exc}" if last_exc else reason
            self.reader_error = f"reconnect failed on {self.port}: {detail}"
            self._log(f"{ts()} [HOST] {self.reader_error}")
            return False

    def open(self) -> None:
        ensure_parent(self.log_file)
        self.log_fp = open(self.log_file, "w", encoding="utf-8", buffering=1)
        self.ser = self._build_serial()
        self.stop_event.clear()
        self.reader_error = None
        self.reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self.reader_thread.start()

    def close(self) -> None:
        self.stop_event.set()
        if self.ser:
            try:
                self.ser.cancel_read()
            except Exception:
                pass
        if self.reader_thread and self.reader_thread.is_alive():
            self.reader_thread.join(timeout=2.0)
        with self.write_lock:
            self._close_serial_locked()
        if self.log_fp:
            self.log_fp.close()
            self.log_fp = None

    def _log(self, line: str) -> None:
        if self.log_fp:
            self.log_fp.write(line + "\n")

    def command(self, cmd: str) -> None:
        if not self.ser and not self._reconnect("command while port closed", timeout_s=5.0):
            raise RuntimeError("serial port not open")
        with self.write_lock:
            if not self.ser:
                raise RuntimeError("serial port not open")
            wait_s = 0.01 - (time.time() - self.last_write_ts)
            if wait_s > 0:
                time.sleep(wait_s)
            payload = (cmd.strip() + "\n").encode("utf-8", errors="ignore")
            try:
                self.ser.write(payload)
                self.ser.flush()
            except Exception as exc:
                self._log(f"{ts()} [HOST] write failed: {exc}")
                self._close_serial_locked()
                if not self._reconnect(f"write failed: {exc}", timeout_s=5.0) or not self.ser:
                    raise
                self.ser.write(payload)
                self.ser.flush()
            self.last_write_ts = time.time()
            self._log(f"{ts()} > {cmd}")

    def _reader_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                ser = self.ser
                raw = ser.read(ser.in_waiting or 1) if ser else b""
            except Exception as exc:
                self._log(f"{ts()} [HOST] read failed: {exc}")
                if self._reconnect(f"read failed: {exc}"):
                    continue
                break
            if not raw:
                continue
            self.rx_buffer.extend(raw)
            if len(self.rx_buffer) > SERIAL_MAX_PENDING_BYTES:
                self._log(f"{ts()} [HOST] dropping oversized RX buffer on {self.port}")
                self.rx_buffer.clear()
                self.pending_status_line = ""
                continue
            while True:
                newline_idx = self.rx_buffer.find(b"\n")
                if newline_idx < 0:
                    if len(self.rx_buffer) > SERIAL_MAX_LINE_BYTES:
                        self._log(f"{ts()} [HOST] dropping oversized partial line on {self.port}")
                        self.rx_buffer.clear()
                        self.pending_status_line = ""
                    break
                raw_line = bytes(self.rx_buffer[:newline_idx])
                del self.rx_buffer[: newline_idx + 1]
                line = raw_line.decode("utf-8", errors="replace").rstrip("\r")
                self._log(f"{ts()} {line}")
                for fragment in split_log_fragments(line):
                    if self.pending_status_line:
                        if fragment and not fragment.startswith("["):
                            combined = self.pending_status_line + fragment
                            self.pending_status_line = ""
                            self.parse_line(combined)
                            continue
                        self.parse_line(self.pending_status_line)
                        self.pending_status_line = ""
                    if fragment.startswith("[SERCTRL] STATUS") and "stop_done=" not in fragment:
                        self.pending_status_line = fragment
                        continue
                    self.parse_line(fragment)


class DirectMaxwellCan:
    def __init__(
        self,
        iface: str,
        log_file: str,
        abs_current_scale: float,
    ) -> None:
        self.iface = iface
        self.log_file = log_file
        self.abs_current_scale = abs_current_scale
        self.sock = None
        self.log_fp = None
        self.started: set[tuple[int, int]] = set()
        self.last_on_attempt_at: dict[tuple[int, int], float] = {}
        self.last_input_mode: dict[tuple[int, int], int] = {}
        self.telemetry_cursor: dict[tuple[int, int], int] = {}

    def open(self) -> None:
        ensure_parent(self.log_file)
        self.log_fp = open(self.log_file, "w", encoding="utf-8", buffering=1)
        self.sock = mxr.socket.socket(mxr.socket.PF_CAN, mxr.socket.SOCK_RAW, mxr.socket.CAN_RAW)
        self.sock.bind((self.iface,))

    def close(self) -> None:
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
        if self.log_fp:
            self.log_fp.close()
            self.log_fp = None

    def _log(self, line: str) -> None:
        if self.log_fp:
            self.log_fp.write(f"{ts()} {line}\n")

    def _module_responding(self, module: mxr.ModuleInfo) -> bool:
        last_seen = getattr(module, "last_seen", 0.0) or 0.0
        return (
            last_seen > 0.0
            and (
                module.status is not None
                or module.voltage_v is not None
                or module.current_a is not None
                or module.current_limit_point is not None
            )
        )

    def _probe_expected(self, address: int, group: int, response_window_s: float = 0.8) -> Optional[mxr.ModuleInfo]:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        module = mxr.ModuleInfo(address=address, group=group)
        mxr.refresh_module_details(self.sock, module, response_window_s=response_window_s)
        if not self._module_responding(module):
            return None
        self._log(
            f"PROBE addr={module.address} grp={module.group} V={module.voltage_v} I={module.current_a} "
            f"Ilim={module.current_limit_point} Pin={module.input_power_w} "
            f"mode={module.input_mode if module.input_mode is not None else 'n/a'} "
            f"rated={module.rated_current_a if module.rated_current_a is not None else 'n/a'} "
            f"{module_status_text(module)}"
        )
        return module

    def recover_interface(self) -> bool:
        commands = [
            ["sudo", "-n", "ip", "link", "set", self.iface, "down"],
            ["sudo", "-n", "ip", "link", "set", self.iface, "type", "can", "bitrate", "125000", "triple-sampling", "on", "restart-ms", "100"],
            ["sudo", "-n", "ip", "link", "set", self.iface, "up"],
        ]

        def run_all(cmds: list[list[str]]) -> bool:
            for cmd in cmds:
                proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
                if proc.returncode != 0:
                    self._log(
                        f"CAN_RECOVER fail cmd={' '.join(cmd)} rc={proc.returncode} "
                        f"stderr={proc.stderr.strip()}"
                    )
                    return False
            return True

        self._log(f"CAN_RECOVER begin iface={self.iface}")
        self.close()
        ok = run_all(commands)
        if not ok:
            commands = [cmd[2:] if cmd[:2] == ["sudo", "-n"] else cmd for cmd in commands]
            ok = run_all(commands)
        if not ok:
            return False
        self.open()
        self._log(f"CAN_RECOVER done iface={self.iface}")
        return True

    def refresh(self, module: mxr.ModuleInfo, response_window_s: float) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        mxr.refresh_module_details(self.sock, module, response_window_s=response_window_s)
        self._log(
            f"TEL addr={module.address} grp={module.group} V={module.voltage_v} I={module.current_a} "
            f"Ilim={module.current_limit_point} Pin={module.input_power_w} "
            f"mode={module.input_mode if module.input_mode is not None else 'n/a'} "
            f"rated={module.rated_current_a if module.rated_current_a is not None else 'n/a'} "
            f"{module_status_text(module)}"
        )

    def poll_telemetry(self, module: mxr.ModuleInfo, response_window_s: float) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        key = (module.address, module.group)
        if module_is_off(module):
            regs = [
                mxr.REG_GET_STATUS,
                mxr.REG_GET_VOLTAGE,
                mxr.REG_GET_CURRENT,
            ]
        else:
            regs = [
                mxr.REG_GET_CURRENT,
                mxr.REG_GET_VOLTAGE,
                mxr.REG_GET_STATUS,
            ]
        cursor = self.telemetry_cursor.get(key, 0) % len(regs)
        reg = regs[cursor]
        self.telemetry_cursor[key] = cursor + 1
        mxr.send_read(self.sock, dst_addr=module.address, group=module.group, reg=reg)
        deadline = time.monotonic() + response_window_s
        while True:
            rem = deadline - time.monotonic()
            if rem <= 0.0:
                return
            pkt = mxr.recv_one(self.sock, min(0.01, rem))
            if pkt is None:
                continue
            can_id, data = pkt
            resp = mxr.parse_maxwell_response(can_id, data)
            if not resp:
                continue
            if int(resp["src"]) != module.address or int(resp["group"]) != module.group:
                continue
            mxr.apply_response(module, resp)
            self._log(
                f"TEL addr={module.address} grp={module.group} V={module.voltage_v} I={module.current_a} "
                f"Ilim={module.current_limit_point} Pin={module.input_power_w} "
                f"mode={module.input_mode if module.input_mode is not None else 'n/a'} "
                f"rated={module.rated_current_a if module.rated_current_a is not None else 'n/a'} "
                f"{module_status_text(module)}"
            )
            return

    def soft_reset(self, module: mxr.ModuleInfo, input_mode: Optional[int] = None) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        desired_mode = input_mode if input_mode in {1, 2, 3} else module.input_mode
        if desired_mode not in {1, 2, 3}:
            desired_mode = 3
        key = (module.address, module.group)
        self.last_input_mode[key] = desired_mode
        self.started.discard(key)
        self.last_on_attempt_at.pop(key, None)
        self.telemetry_cursor.pop(key, None)
        self._log(f"RESET addr={module.address} grp={module.group} input_mode={desired_mode}")
        mxr.soft_reset_defaults(self.sock, module, input_mode=desired_mode)
        time.sleep(0.02)
        mxr.send_set_float(self.sock, module.address, module.group, 0x0020, MXR_DEFAULT_OUTPUT_POWER_KW)
        self._log(
            f"RESET-POWER addr={module.address} grp={module.group} output_power_kw={MXR_DEFAULT_OUTPUT_POWER_KW:.1f}"
        )
        time.sleep(0.02)
        self.refresh(module, response_window_s=0.5)

    def normalize_output_request(self, voltage_v: float, current_a: float) -> tuple[float, float]:
        return max(0.0, voltage_v), max(0.0, current_a)

    def _send_live_setpoint(self, module: mxr.ModuleInfo, voltage_v: float, current_raw: int) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        mxr.send_set_float(self.sock, module.address, module.group, mxr.REG_SET_VOLTAGE, voltage_v)
        time.sleep(0.005)
        mxr.send_set_int(self.sock, module.address, module.group, mxr.REG_SET_CURRENT_ABS, current_raw)

    def _startup_then_setpoint(self, module: mxr.ModuleInfo, voltage_v: float, current_a: float) -> int:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        return mxr.module_on_absolute(
            self.sock,
            module,
            voltage_v=safe_float(voltage_v),
            current_a=safe_float(current_a),
            scale=self.abs_current_scale,
        )

    def set_output(self, module: mxr.ModuleInfo, voltage_v: float, current_a: float) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        safe_voltage_v, safe_current_a = self.normalize_output_request(voltage_v, current_a)
        current_raw = int(round(max(0.0, safe_current_a) * self.abs_current_scale))
        current_raw = max(0, min(current_raw, 0xFFFFFFFF))
        key = (module.address, module.group)
        now = time.monotonic()
        if key not in self.started:
            self._log(
                f"START addr={module.address} grp={module.group} voltage_v={safe_voltage_v:.1f} current_a={safe_current_a:.2f} raw=0x{current_raw:08X}"
            )
            self._startup_then_setpoint(module, safe_voltage_v, safe_current_a)
            self.started.add(key)
            self.last_on_attempt_at[key] = now
            return
        else:
            self._log(
                f"SET addr={module.address} grp={module.group} voltage_v={safe_voltage_v:.1f} current_a={safe_current_a:.2f} raw=0x{current_raw:08X}"
            )
        if safe_current_a > 0.05 and module_is_off(module) and (now - self.last_on_attempt_at.get(key, 0.0)) >= MODULE_OFF_RETRY_INTERVAL_S:
            self._log(
                f"RE-ON addr={module.address} grp={module.group} voltage_v={safe_voltage_v:.1f} current_a={safe_current_a:.2f} raw=0x{current_raw:08X}"
            )
            self._startup_then_setpoint(module, safe_voltage_v, safe_current_a)
            self.last_on_attempt_at[key] = now
            return
        self._send_live_setpoint(module, safe_voltage_v, current_raw)

    def stop_output(self, module: mxr.ModuleInfo) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        self._log(f"STOP addr={module.address} grp={module.group}")
        mxr.send_set_int(self.sock, module.address, module.group, mxr.REG_SET_CURRENT_ABS, 0)
        time.sleep(0.01)
        mxr.module_off(self.sock, module)
        self.started.discard((module.address, module.group))
        self.last_on_attempt_at.pop((module.address, module.group), None)


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
    if not text:
        return "none"
    return text


def parse_status_line(line: str) -> Optional[PlcStatus]:
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
        relay2=int(match.group(20) or 0),
        relay3=int(match.group(21) or 0),
        alloc_sz=int(match.group(22) or 0),
    )


def parse_local_line(line: str) -> Optional[PlcStatus]:
    match = LOCAL_RE.search(line)
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
        cp="U",
        duty=100,
        relay1=0,
        relay2=0,
        relay3=0,
        alloc_sz=0,
    )


def parse_partial_status_line(line: str, baseline: Optional[PlcStatus]) -> Optional[PlcStatus]:
    if baseline is None:
        return None
    match = PARTIAL_STATUS_RE.search(line)
    if not match:
        return None
    return PlcStatus(
        mode_id=baseline.mode_id,
        mode_name=baseline.mode_name,
        plc_id=int(match.group(1)),
        connector_id=baseline.connector_id,
        controller_id=int(match.group(2)),
        module_addr=int(match.group(3), 16),
        local_group=int(match.group(4)),
        module_id=match.group(5),
        can_stack=int(match.group(6)),
        module_mgr=int(match.group(7)),
        cp=match.group(8),
        duty=int(match.group(9)),
        relay1=int(match.group(16)),
        relay2=int(match.group(17) or baseline.relay2),
        relay3=int(match.group(18) or baseline.relay3),
        alloc_sz=int(match.group(19) or baseline.alloc_sz),
    )


def split_log_fragments(line: str) -> list[str]:
    matches = list(LOG_FRAGMENT_RE.finditer(line))
    if len(matches) <= 1:
        return [line]
    fragments: list[str] = []
    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if (idx + 1) < len(matches) else len(line)
        fragment = line[start:end].strip()
        if fragment:
            fragments.append(fragment)
    return fragments or [line]


def append_serial_candidate(candidates: list[str], seen: set[str], path: str) -> None:
    if not path:
        return
    expanded = os.path.realpath(path)
    chosen = path
    if not os.path.exists(chosen) and expanded and os.path.exists(expanded):
        chosen = expanded
    if not os.path.exists(chosen):
        return
    key = os.path.realpath(chosen)
    if key in seen:
        return
    seen.add(key)
    candidates.append(chosen)


def collect_serial_candidates() -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    by_id_dir = Path("/dev/serial/by-id")
    if by_id_dir.is_dir():
        for entry in sorted(by_id_dir.iterdir()):
            append_serial_candidate(candidates, seen, str(entry))
    for pattern in ("/dev/ttyACM*", "/dev/ttyUSB*"):
        for path in sorted(glob.glob(pattern)):
            append_serial_candidate(candidates, seen, path)
    return candidates


def probe_serial_status(port: str, baud: int, timeout_s: float) -> Optional[PlcStatus]:
    kwargs = {
        "port": port,
        "baudrate": baud,
        "timeout": 0.05,
        "write_timeout": 0.5,
    }
    ser = None
    try:
        try:
            ser = serial.Serial(exclusive=True, **kwargs)
        except TypeError:
            ser = serial.Serial(**kwargs)
        ser.reset_input_buffer()
        ser.reset_output_buffer()
        deadline = time.time() + max(0.5, timeout_s)
        next_status = 0.0
        buffer = ""
        while time.time() < deadline:
            now = time.time()
            if now >= next_status:
                ser.write(b"CTRL STATUS\n")
                ser.flush()
                next_status = now + 0.25
            raw = ser.read(4096)
            if not raw:
                continue
            buffer += raw.decode(errors="ignore")
            status = parse_status_line(buffer)
            if status is not None:
                return status
            if len(buffer) > 32768:
                buffer = buffer[-16384:]
    except Exception:
        return None
    finally:
        if ser is not None:
            try:
                ser.close()
            except Exception:
                pass
    return None


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
    with open(reset_log_file, "w", encoding="utf-8") as fp:
        fp.write("".join(chunks))


def resolve_plc_port(args: argparse.Namespace) -> None:
    if args.plc_port and args.plc_port.lower() != "auto":
        if not os.path.exists(args.plc_port):
            raise RuntimeError(f"PLC serial port does not exist: {args.plc_port}")
        if args.plc_id is None:
            status = probe_serial_status(args.plc_port, args.baud, args.discovery_timeout_s)
            if status is not None:
                args.plc_id = status.plc_id
        return

    candidates = collect_serial_candidates()
    if not candidates:
        raise RuntimeError("no candidate PLC serial ports found for auto-discovery")

    discovered: list[tuple[str, PlcStatus]] = []
    for port in candidates:
        status = probe_serial_status(port, args.baud, args.discovery_timeout_s)
        if status is not None:
            discovered.append((port, status))

    if not discovered:
        raise RuntimeError("unable to read [SERCTRL] STATUS from any candidate PLC serial port")

    chosen: Optional[tuple[str, PlcStatus]] = None
    if args.plc_id is not None:
        chosen = next((item for item in discovered if item[1].plc_id == args.plc_id), None)
    if chosen is None:
        chosen = next(
            (
                item
                for item in discovered
                if item[1].module_addr == args.home_addr and item[1].local_group == args.home_group
            ),
            None,
        )
    if chosen is None:
        chosen = next((item for item in discovered if item[1].connector_id == 1), None)
    if chosen is None:
        details = ", ".join(
            f"{path}:plc_id={status.plc_id},connector={status.connector_id},module=0x{status.module_addr:02X}/g{status.local_group}"
            for path, status in discovered
        )
        raise RuntimeError(f"failed to resolve controller PLC port from discovery results: {details}")

    args.plc_port = chosen[0]
    if args.plc_id is None:
        args.plc_id = chosen[1].plc_id

    print(
        "[DISCOVERY] plc="
        f"{args.plc_port} plc_id={chosen[1].plc_id} connector_id={chosen[1].connector_id} "
        f"module=0x{chosen[1].module_addr:02X}/g{chosen[1].local_group}",
        flush=True,
    )


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
    slac_fsm: int = 0
    slac_failures: int = 0
    hlc_ready: bool = False
    hlc_active: bool = False
    precharge_seen: bool = False
    precharge_ready: bool = False
    session_started: bool = False
    stop_active: bool = False
    stop_hard: bool = False
    stop_done: bool = True
    meter_wh: int = 0
    soc: int = 0
    bms_stage_id: int = 0
    bms_stage_name: str = "none"
    bms_valid: bool = False
    delivery_ready: bool = False
    target_v: float = 0.0
    target_i: float = 0.0
    latched_target_v: float = 0.0
    latched_target_i: float = 0.0
    fb_valid: bool = False
    fb_ready: bool = False
    fb_v: float = 0.0
    fb_i: float = 0.0
    fb_curr_lim: bool = False
    fb_volt_lim: bool = False
    fb_pwr_lim: bool = False
    first_current_demand_ts: Optional[float] = None
    identity_seen: bool = False
    identity_value: str = ""
    power_delivery_req_count: int = 0
    welding_req_count: int = 0
    session_stop_req_count: int = 0
    last_power_delivery_req_ts: Optional[float] = None
    last_welding_req_ts: Optional[float] = None
    last_session_stop_req_ts: Optional[float] = None
    power_delivery_stop_runtime_count: int = 0
    welding_runtime_count: int = 0
    session_stop_runtime_count: int = 0
    client_session_done_count: int = 0
    last_power_delivery_stop_runtime_ts: Optional[float] = None
    last_welding_runtime_ts: Optional[float] = None
    last_session_stop_runtime_ts: Optional[float] = None
    last_client_session_done_ts: Optional[float] = None
    errors: list[str] = field(default_factory=list)


@dataclass
class ControlPlan:
    enabled: bool = False
    voltage_v: float = 0.0
    current_a: float = 0.0


@dataclass
class ModuleSnapshot:
    status: Optional[int] = None
    voltage_v: float = 0.0
    current_a: float = 0.0
    current_limit_point: float = 0.0
    input_power_w: float = 0.0
    input_mode: int = -1
    rated_output_current_reg_a: float = 0.0
    off: bool = False
    online: bool = False
    status_text: str = "status=n/a flags=-"
    last_update_ts: float = 0.0


def build_module_snapshot(module: Optional[mxr.ModuleInfo]) -> ModuleSnapshot:
    if module is None:
        return ModuleSnapshot()
    return ModuleSnapshot(
        status=module.status,
        voltage_v=module_voltage_value(module),
        current_a=module_current_value(module),
        current_limit_point=clamp_non_negative(getattr(module, "current_limit_point", 0.0)),
        input_power_w=clamp_non_negative(getattr(module, "input_power_w", 0.0)),
        input_mode=int(getattr(module, "input_mode", -1) or -1),
        rated_output_current_reg_a=clamp_non_negative(getattr(module, "rated_current_a", 0.0)),
        off=module.status is not None and module_is_off(module),
        online=module_online(module),
        status_text=module_status_text(module),
        last_update_ts=float(getattr(module, "last_seen", 0.0) or 0.0),
    )


class SingleModuleChargeRunner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.live = PlcLiveState("plc")
        self.live_lock = threading.RLock()
        self.status_queue: Deque[PlcStatus] = deque()
        self.ack_queue: Deque[Ack] = deque()
        self.queue_cv = threading.Condition()

        self.link = SerialLink(args.plc_port, args.baud, args.plc_log, self._parse_line)
        self.can = DirectMaxwellCan(
            args.can_iface,
            args.can_log,
            args.absolute_current_scale,
        )

        self.home_module: Optional[mxr.ModuleInfo] = None
        self.module_lock = threading.RLock()
        self.module_plan = ControlPlan()
        self.module_snapshot = ModuleSnapshot()
        self.module_worker_stop = threading.Event()
        self.module_worker_thread: Optional[threading.Thread] = None
        self.module_worker_error: Optional[str] = None
        self.module_force_refresh = False
        self.module_force_apply = False
        self.phase = "bootstrap"
        self.events: list[str] = []
        self.notes: list[str] = []
        self.failures: list[str] = []

        self.current_plan = ControlPlan()
        self.plan_active = False
        self.desired_relay = False

        self.last_relay_refresh = 0.0
        self.last_relay_command: Optional[bool] = None
        self.last_output_signature: Optional[tuple[bool, int, int]] = None
        self.last_set_refresh = 0.0
        self.last_feedback_cmd = 0.0
        self.last_feedback_signature: Optional[tuple[str, int, int, bool]] = None
        self.last_feedback_ready = False
        self.last_status_poll = 0.0
        self.last_telemetry_poll = 0.0
        self.last_full_telemetry_refresh = 0.0
        self.last_progress = 0.0
        self.last_hb = 0.0
        self.last_auth = 0.0
        self.last_slac_start = 0.0
        self.last_status_rx = 0.0
        self.remote_start_sent = False
        self.next_tx_seq: dict[int, int] = {}
        self.stage_name = "none"
        self.precharge_feedback_count = 0
        self.current_demand_rundown_until = 0.0
        self.last_feedback_requested_i = 0.0

        self.current_demand_started_at: Optional[float] = None
        self.current_demand_ready_since: Optional[float] = None
        self.charge_proof_since: Optional[float] = None
        self.charge_proof_peak_v = 0.0
        self.charge_proof_peak_i = 0.0
        self.charge_proof_peak_power_w = 0.0
        self.last_target_apply_ts = 0.0
        self.vehicle_stop_requested = False
        self.vehicle_stop_reason = ""
        self.stop_contract_started_at: Optional[float] = None
        self.stop_contract_baseline_power_delivery_stop_runtime_count = 0
        self.stop_contract_baseline_welding_req_count = 0
        self.stop_contract_baseline_welding_runtime_count = 0
        self.stop_contract_baseline_session_stop_req_count = 0
        self.stop_contract_baseline_client_session_done_count = 0

    def _note_once(self, note: str) -> None:
        if note not in self.notes:
            self.notes.append(note)

    def _reset_plc_if_requested(self) -> None:
        if not self.args.esp_reset_before_start:
            self.next_tx_seq.clear()
            return
        reset_log = self.args.plc_log + ".reset.log"
        print(
            f"[RESET] rebooting PLC on {self.args.plc_port} wait_s={self.args.esp_reset_wait_s:.1f}",
            flush=True,
        )
        reset_plc_over_serial(
            self.args.plc_port,
            self.args.baud,
            reset_log,
            self.args.esp_reset_pulse_s,
            self.args.esp_reset_wait_s,
        )
        self.next_tx_seq.clear()
        print(f"[RESET] boot log captured in {reset_log}", flush=True)

    def _set_phase(self, name: str) -> None:
        self.phase = name
        self.events.append(f"{ts()} phase={name}")
        print(f"[PHASE] {name}", flush=True)

    def _parse_line(self, line: str) -> None:
        now = time.time()

        status = parse_status_line(line)
        if status is None:
            status = parse_local_line(line)
        if status is None:
            with self.live_lock:
                baseline = self.live.last_status
            status = parse_partial_status_line(line, baseline)
        if status is not None:
            with self.live_lock:
                self.live.last_status = status
                self.live.mode_id = status.mode_id
                self.live.mode_name = status.mode_name
                self.live.can_stack = status.can_stack
                self.live.module_mgr = status.module_mgr
                self.live.cp_phase = status.cp
                self.live.cp_connected = status.cp in ("B", "B1", "B2", "C", "D")
                self.live.cp_duty_pct = status.duty
                if status.cp == "B1":
                    if self.live.b1_since_ts is None:
                        self.live.b1_since_ts = now
                else:
                    self.live.b1_since_ts = None
                self.live.relay_states[1] = status.relay1 != 0
                self.live.relay_states[2] = status.relay2 != 0
                self.live.relay_states[3] = status.relay3 != 0
                self.last_status_rx = now
            with self.queue_cv:
                self.status_queue.append(status)
                self.queue_cv.notify_all()
            return

        ack_match = ACK_RE.search(line)
        if ack_match:
            cmd_hex = int(ack_match.group(1), 16)
            seq = int(ack_match.group(2))
            with self.queue_cv:
                self._sync_tx_seq(cmd_hex, seq)
                self.ack_queue.append(
                    Ack(
                        cmd_hex=cmd_hex,
                        seq=seq,
                        status=int(ack_match.group(3)),
                        detail0=int(ack_match.group(4)),
                        detail1=int(ack_match.group(5)),
                        received_at=now,
                    )
                )
                self.queue_cv.notify_all()
            return

        relay_match = RELAY_EVT_RE.search(line)
        if relay_match:
            with self.live_lock:
                self.live.relay_states[int(relay_match.group(1))] = relay_match.group(2) == "CLOSED"
            return

        cp_match = CP_TRANSITION_RE.search(line)
        if cp_match:
            cp_phase = cp_match.group(1)
            with self.live_lock:
                self.live.cp_phase = cp_phase
                self.live.cp_connected = cp_phase in ("B", "B1", "B2", "C", "D")
                if cp_phase == "B1":
                    self.live.b1_since_ts = now
                else:
                    self.live.b1_since_ts = None
            return

        cp_evt = CP_EVT_RE.search(line)
        if cp_evt:
            with self.live_lock:
                self.live.cp_phase = cp_evt.group(1)
                self.live.cp_connected = cp_evt.group(3) == "1"
                self.live.cp_duty_pct = int(cp_evt.group(2))
                if self.live.cp_phase == "B1":
                    if self.live.b1_since_ts is None:
                        self.live.b1_since_ts = now
                else:
                    self.live.b1_since_ts = None
            return

        slac_evt = SLAC_EVT_RE.search(line)
        if slac_evt:
            with self.live_lock:
                self.live.slac_session = slac_evt.group(1) == "1"
                self.live.slac_matched = self.live.slac_session and slac_evt.group(2) == "1"
                self.live.slac_fsm = int(slac_evt.group(3))
                self.live.slac_failures = int(slac_evt.group(4))
            return

        hlc_evt = HLC_EVT_RE.search(line)
        if hlc_evt:
            with self.live_lock:
                self.live.hlc_ready = hlc_evt.group(1) == "1"
                self.live.hlc_active = hlc_evt.group(2) == "1"
                self.live.precharge_seen = hlc_evt.group(5) == "1"
                self.live.precharge_ready = hlc_evt.group(6) == "1"
                self.live.fb_valid = hlc_evt.group(7) == "1"
                self.live.fb_ready = hlc_evt.group(8) == "1"
                self.live.fb_v = safe_float(hlc_evt.group(9))
                self.live.fb_i = safe_float(hlc_evt.group(10))
            return

        session_evt = SESSION_EVT_RE.search(line)
        if session_evt:
            with self.live_lock:
                self.live.session_started = session_evt.group(1) == "1"
                self.live.slac_matched = session_evt.group(2) == "1"
                self.live.relay_states[1] = session_evt.group(3) == "1"
                self.live.relay_states[2] = session_evt.group(4) == "1"
                self.live.relay_states[3] = session_evt.group(5) == "1"
                self.live.stop_active = session_evt.group(6) == "1"
                self.live.stop_hard = session_evt.group(7) == "1"
                self.live.stop_done = session_evt.group(8) == "1"
                self.live.meter_wh = int(session_evt.group(9))
                self.live.soc = int(session_evt.group(10))
            return

        bms_evt = BMS_EVT_RE.search(line)
        if bms_evt:
            stage_name = normalize_stage_name(bms_evt.group(2))
            with self.live_lock:
                self.live.bms_stage_id = int(bms_evt.group(1))
                self.live.bms_stage_name = stage_name
                self.live.bms_valid = bms_evt.group(3) == "1"
                self.live.delivery_ready = bms_evt.group(4) == "1"
                self.live.target_v = clamp_non_negative(bms_evt.group(5))
                self.live.target_i = clamp_non_negative(bms_evt.group(6))
                if stage_name in ("precharge", "power_delivery", "current_demand"):
                    if self.live.target_v > 0.0:
                        self.live.latched_target_v = self.live.target_v
                    if self.live.target_i > 0.0:
                        self.live.latched_target_i = self.live.target_i
                self.live.fb_valid = bms_evt.group(7) == "1"
                self.live.fb_ready = bms_evt.group(8) == "1"
                self.live.fb_v = clamp_non_negative(bms_evt.group(9))
                self.live.fb_i = clamp_non_negative(bms_evt.group(10))
                self.live.fb_curr_lim = bms_evt.group(11) == "1"
                self.live.fb_volt_lim = bms_evt.group(12) == "1"
                self.live.fb_pwr_lim = bms_evt.group(13) == "1"
                if stage_name == "current_demand" and self.live.first_current_demand_ts is None:
                    self.live.first_current_demand_ts = now
            return

        identity_evt = IDENTITY_EVT_RE.search(line)
        if identity_evt:
            with self.live_lock:
                self.live.identity_seen = True
                self.live.identity_value = identity_evt.group(4).upper()
            return

        hlc_req = HLC_REQ_RE.search(line)
        req_payload = None
        if hlc_req:
            kind = hlc_req.group(1)
        else:
            hlc_req_json = HLC_REQ_JSON_RE.search(line)
            if hlc_req_json:
                try:
                    req_payload = json.loads(hlc_req_json.group(1))
                except json.JSONDecodeError:
                    req_payload = None
                kind = req_payload.get("type") if isinstance(req_payload, dict) else None
            else:
                kind = None
        if kind:
            with self.live_lock:
                if kind == "PowerDeliveryReq":
                    self.live.power_delivery_req_count += 1
                    self.live.last_power_delivery_req_ts = now
                elif kind == "CurrentDemandReq":
                    self.live.bms_stage_name = "current_demand"
                    self.live.delivery_ready = True
                    target_v = clamp_non_negative(req_payload.get("targetV", 0.0)) if isinstance(req_payload, dict) else 0.0
                    target_i = clamp_non_negative(req_payload.get("targetI", 0.0)) if isinstance(req_payload, dict) else 0.0
                    if target_v > 0.0:
                        self.live.target_v = target_v
                        self.live.latched_target_v = target_v
                    if target_i > 0.0:
                        self.live.target_i = target_i
                        self.live.latched_target_i = target_i
                elif kind == "WeldingDetectionReq":
                    self.live.welding_req_count += 1
                    self.live.last_welding_req_ts = now
                elif kind == "SessionStopReq":
                    self.live.session_stop_req_count += 1
                    self.live.last_session_stop_req_ts = now
            if self.current_demand_started_at is not None and kind in (
                "PowerDeliveryReq",
                "WeldingDetectionReq",
                "SessionStopReq",
            ):
                self.vehicle_stop_requested = True
                self.vehicle_stop_reason = kind
                self.current_plan = ControlPlan(False, 0.0, 0.0)
                self.desired_relay = False
                self.last_feedback_signature = None
            return

        if POWER_DELIVERY_STOP_RUNTIME_RE.search(line):
            with self.live_lock:
                self.live.power_delivery_stop_runtime_count += 1
                self.live.last_power_delivery_stop_runtime_ts = now
            return

        if WELDING_RUNTIME_RE.search(line):
            with self.live_lock:
                self.live.welding_runtime_count += 1
                self.live.last_welding_runtime_ts = now
            return

        if SESSION_STOP_RUNTIME_RE.search(line):
            with self.live_lock:
                self.live.session_stop_runtime_count += 1
                self.live.last_session_stop_runtime_ts = now
            return

        if CLIENT_SESSION_DONE_RE.search(line):
            with self.live_lock:
                self.live.client_session_done_count += 1
                self.live.last_client_session_done_ts = now

    def _send(self, cmd: str) -> None:
        self.link.command(cmd)

    def _wait_for_status(self, timeout_s: float = 2.0) -> PlcStatus:
        deadline = time.time() + timeout_s
        with self.queue_cv:
            while True:
                if self.status_queue:
                    return self.status_queue.popleft()
                if self.link.reader_error:
                    raise RuntimeError(f"serial reader failed: {self.link.reader_error}")
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise RuntimeError("timeout waiting for PLC status")
                self.queue_cv.wait(timeout=min(0.05, remaining))

    def _recent_status_fallback(self, max_age_s: float) -> Optional[PlcStatus]:
        with self.live_lock:
            status = self.live.last_status
        if status is None or self.last_status_rx <= 0.0:
            return None
        if (time.time() - self.last_status_rx) > max_age_s:
            return None
        return status

    def _reserve_tx_seq(self, cmd_hex: int) -> Optional[int]:
        if cmd_hex not in self.next_tx_seq:
            return None
        seq = self.next_tx_seq[cmd_hex]
        self.next_tx_seq[cmd_hex] = (seq + 1) & 0xFF
        return seq

    def _sync_tx_seq(self, cmd_hex: int, ack_seq: int) -> None:
        self.next_tx_seq[cmd_hex] = (ack_seq + 1) & 0xFF

    def _wait_for_ack(
        self,
        cmd_hex: int,
        timeout_s: float = 5.0,
        expected_seq: Optional[int] = None,
        sent_after: float = 0.0,
    ) -> Ack:
        deadline = time.time() + timeout_s
        with self.queue_cv:
            while True:
                pending = list(self.ack_queue)
                self.ack_queue.clear()
                match: Optional[Ack] = None
                for ack in pending:
                    if ack.cmd_hex != cmd_hex:
                        self.ack_queue.append(ack)
                        continue
                    if ack.received_at < sent_after:
                        continue
                    if expected_seq is not None and ack.seq != expected_seq:
                        self.ack_queue.append(ack)
                        continue
                    if match is None:
                        match = ack
                    else:
                        self.ack_queue.append(ack)
                if match is not None:
                    self._sync_tx_seq(cmd_hex, match.seq)
                    return match
                if self.link.reader_error:
                    raise RuntimeError(f"serial reader failed: {self.link.reader_error}")
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise RuntimeError(f"timeout waiting for ack 0x{cmd_hex:02X}")
                self.queue_cv.wait(timeout=min(0.05, remaining))

    def _send_expect_ack(self, cmd: str, cmd_hex: int, timeout_s: float = 5.0, retries: int = 1) -> Ack:
        attempts = max(1, retries + 1)
        last_error: Optional[Exception] = None
        for attempt in range(attempts):
            expected_seq = self._reserve_tx_seq(cmd_hex)
            sent_after = time.time()
            self._send(cmd)
            try:
                ack = self._wait_for_ack(
                    cmd_hex,
                    timeout_s=timeout_s,
                    expected_seq=expected_seq,
                    sent_after=sent_after,
                )
                if ack.status == ACK_BAD_SEQ and attempt + 1 < attempts:
                    self._note_once(f"retrying {cmd} after ACK_BAD_SEQ seq={ack.seq}")
                    time.sleep(0.05)
                    continue
                return ack
            except RuntimeError as exc:
                last_error = exc
                if attempt + 1 >= attempts:
                    raise
                self._note_once(f"retrying {cmd} after ack miss: {exc}")
                time.sleep(0.1)
        raise RuntimeError(str(last_error) if last_error else f"ack wait failed for {cmd}")

    def _send_untracked_ackable(self, cmd: str, cmd_hex: int) -> None:
        # Match the validated C++ harness: active-window FEEDBACK/HB/AUTH are
        # best-effort streamed commands, not tracked RPCs. Let incoming ACKs
        # advance the sequence opportunistically instead of pre-consuming one
        # here.
        self._send(cmd)

    def _query_status(self, timeout_s: float = 2.0, allow_stale: bool = False, stale_max_age_s: float = 3.0) -> PlcStatus:
        while self.status_queue:
            self.status_queue.popleft()
        self._send("CTRL STATUS")
        try:
            return self._wait_for_status(timeout_s=timeout_s)
        except RuntimeError as exc:
            if allow_stale and "timeout waiting for PLC status" in str(exc):
                fallback = self._recent_status_fallback(stale_max_age_s)
                if fallback is not None:
                    self._note_once(
                        f"status timeout during {self.phase}; using cached PLC status <= {stale_max_age_s:.1f}s old"
                    )
                    return fallback
            raise

    def _relay_is_closed(self) -> bool:
        with self.live_lock:
            return self.live.relay_states.get(self.args.power_relay) is True

    def _relay_is_open(self) -> bool:
        with self.live_lock:
            return self.live.relay_states.get(self.args.power_relay) is False

    def _set_relay(self, closed: bool, hold_ms: int, require_ack: bool = True) -> None:
        mask = 1 << (self.args.power_relay - 1)
        state_mask = mask if closed else 0
        cmd = f"CTRL RELAY {mask} {state_mask} {max(0, hold_ms)}"
        if require_ack:
            ack = self._send_expect_ack(cmd, ACK_CMD_RELAY, timeout_s=3.0, retries=1)
            if ack.status != ACK_OK:
                raise RuntimeError(
                    f"Relay{self.args.power_relay} rejected status={ack.status} "
                    f"detail0={ack.detail0} detail1={ack.detail1}"
                )
        else:
            self._send(cmd)

    def _send_slac_start(self, now: float) -> None:
        ack = self._send_expect_ack(f"CTRL SLAC start {self.args.arm_ms}", ACK_CMD_SLAC, timeout_s=3.0, retries=1)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL SLAC start rejected status={ack.status}")
        self.last_slac_start = now
        self.remote_start_sent = True

    def _bootstrap_plc(self) -> PlcStatus:
        try:
            status = self._query_status(timeout_s=4.0, allow_stale=True, stale_max_age_s=5.0)
        except RuntimeError as exc:
            if "timeout waiting for PLC status" not in str(exc):
                raise
            self._note_once("bootstrap status delayed after reset; continuing with configured PLC mapping")
            status = PlcStatus(
                mode_id=1,
                mode_name="external_controller",
                plc_id=int(self.args.plc_id or 0),
                connector_id=int(self.args.plc_id or 0),
                controller_id=int(self.args.controller_id),
                module_addr=int(self.args.home_addr),
                local_group=int(self.args.home_group),
                module_id=f"PLC{int(self.args.plc_id or 0)}",
                can_stack=0,
                module_mgr=0,
                cp="U",
                duty=100,
                relay1=0,
                relay2=0,
                relay3=0,
                alloc_sz=0,
            )
        if status.mode_id == 1 and status.controller_id != self.args.controller_id:
            self._note_once(
                f"adopting live controller_id={status.controller_id} from PLC status during bootstrap"
            )
            self.args.controller_id = status.controller_id
        elif status.mode_id != 1:
            self._send(f"CTRL MODE 1 {self.args.plc_id} {self.args.controller_id}")
            time.sleep(0.3)
        ack = self._send_expect_ack(f"CTRL HB {self.args.heartbeat_timeout_ms}", ACK_CMD_HEARTBEAT, timeout_s=3.0, retries=1)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL HB rejected status={ack.status}")
        ack = self._send_expect_ack("CTRL STOP clear 3000", ACK_CMD_STOP, timeout_s=3.0, retries=1)
        if ack.status != ACK_OK and ack.status != 4:
            raise RuntimeError(f"CTRL STOP clear rejected status={ack.status}")
        ack = self._send_expect_ack(f"CTRL AUTH deny {self.args.auth_ttl_ms}", ACK_CMD_AUTH, timeout_s=3.0, retries=1)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL AUTH deny rejected status={ack.status}")
        ack = self._send_expect_ack("CTRL SLAC disarm 3000", ACK_CMD_SLAC, timeout_s=3.0, retries=1)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL SLAC disarm rejected status={ack.status}")
        ack = self._send_expect_ack("CTRL FEEDBACK 1 0 0 0 0 0 0", ACK_CMD_FEEDBACK, timeout_s=3.0, retries=1)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL FEEDBACK reset rejected status={ack.status}")
        for relay_idx in (1, 2, 3):
            mask = 1 << (relay_idx - 1)
            ack = self._send_expect_ack(f"CTRL RELAY {mask} 0 0", ACK_CMD_RELAY, timeout_s=3.0, retries=1)
            if ack.status != ACK_OK:
                raise RuntimeError(f"CTRL RELAY open for relay{relay_idx} rejected status={ack.status}")
        status = self._query_status(allow_stale=True, stale_max_age_s=3.0)
        if status.mode_id != 1:
            raise RuntimeError("PLC did not enter mode=1 external_controller")
        if status.can_stack != 0 or status.module_mgr != 0:
            raise RuntimeError(
                f"external-controller routing mismatch: can_stack={status.can_stack} module_mgr={status.module_mgr}"
            )
        if (
            self.args.home_addr == DEFAULT_HOME_ADDR
            and self.args.home_group == DEFAULT_HOME_GROUP
            and (status.module_addr != self.args.home_addr or status.local_group != self.args.home_group)
        ):
            self._note_once(
                "adopting live PLC module mapping "
                f"0x{status.module_addr:02X}/g{status.local_group} "
                f"in place of default 0x{self.args.home_addr:02X}/g{self.args.home_group}"
            )
            self.args.home_addr = status.module_addr
            self.args.home_group = status.local_group
        return status

    def _probe_home_module(self) -> mxr.ModuleInfo:
        if not self.can.sock:
            raise RuntimeError("CAN socket not open")
        for attempt in range(2):
            module = self.can._probe_expected(self.args.home_addr, self.args.home_group, response_window_s=0.8)  # type: ignore[attr-defined]
            if module is not None:
                return module
            if attempt == 0:
                print("[WARN] module probe missed; recovering CAN interface", flush=True)
                if not self.can.recover_interface():
                    break
        raise RuntimeError(f"module 0x{self.args.home_addr:02X}/g{self.args.home_group} not found on {self.args.can_iface}")

    def _snapshot_module_locked(self) -> None:
        self.module_snapshot = build_module_snapshot(self.home_module)

    def _get_module_snapshot(self) -> ModuleSnapshot:
        with self.module_lock:
            return ModuleSnapshot(
                status=self.module_snapshot.status,
                voltage_v=self.module_snapshot.voltage_v,
                current_a=self.module_snapshot.current_a,
                current_limit_point=self.module_snapshot.current_limit_point,
                input_power_w=self.module_snapshot.input_power_w,
                input_mode=self.module_snapshot.input_mode,
                rated_output_current_reg_a=self.module_snapshot.rated_output_current_reg_a,
                off=self.module_snapshot.off,
                online=self.module_snapshot.online,
                status_text=self.module_snapshot.status_text,
                last_update_ts=self.module_snapshot.last_update_ts,
            )

    def _publish_module_plan(self, force_refresh: bool = False, force_apply: bool = False) -> None:
        with self.module_lock:
            self.module_plan = ControlPlan(
                enabled=self.current_plan.enabled,
                voltage_v=self.current_plan.voltage_v,
                current_a=self.current_plan.current_a,
            )
            if force_refresh:
                self.module_force_refresh = True
            if force_apply:
                self.module_force_apply = True

    def _module_worker_interval_s(self) -> float:
        return max(
            MODULE_WORKER_MIN_INTERVAL_S,
            min(
                MODULE_WORKER_TARGET_INTERVAL_S,
                self.args.control_interval_s,
                self.args.telemetry_interval_s,
            ),
        )

    def _module_worker_loop(self) -> None:
        if self.home_module is None:
            return
        last_signature: Optional[tuple[bool, int, int]] = None
        plan_active = False
        last_set_refresh = 0.0
        last_full_refresh = 0.0

        while not self.module_worker_stop.is_set():
            loop_started = time.monotonic()
            applied_now = False
            require_fresh_sample_after_apply = False
            try:
                with self.module_lock:
                    plan = ControlPlan(
                        enabled=self.module_plan.enabled,
                        voltage_v=self.module_plan.voltage_v,
                        current_a=self.module_plan.current_a,
                    )
                    force_refresh = self.module_force_refresh
                    self.module_force_refresh = False
                    force_apply = self.module_force_apply
                    self.module_force_apply = False

                safe_v, safe_i = self.can.normalize_output_request(plan.voltage_v, plan.current_a)
                signature = (plan.enabled, int(round(safe_v * 10.0)), int(round(safe_i * 100.0)))
                signature_changed = last_signature != signature
                control_refresh_s = self.args.control_interval_s
                if module_is_off(self.home_module):
                    control_refresh_s = min(control_refresh_s, MODULE_OFF_RETRY_INTERVAL_S)
                if plan.enabled and (
                    force_apply or signature_changed or (loop_started - last_set_refresh) >= control_refresh_s
                ):
                    require_fresh_sample_after_apply = force_apply or signature_changed or (not plan_active)
                    self.can.set_output(self.home_module, safe_v, safe_i)
                    plan_active = True
                    last_signature = signature
                    last_set_refresh = loop_started
                    applied_now = True
                    if not module_online(self.home_module):
                        force_refresh = True
                elif (not plan.enabled) and plan_active:
                    self.can.stop_output(self.home_module)
                    plan_active = False
                    last_signature = None
                    last_set_refresh = loop_started
                    applied_now = True
                    force_refresh = True

                should_poll = (
                    plan.enabled
                    or plan_active
                    or self.desired_relay
                    or self._relay_is_closed()
                    or self.phase in ("wait_for_session", "stopping")
                )
                if should_poll:
                    full_refresh_due = force_refresh or (loop_started - last_full_refresh) >= MODULE_ACTIVE_FULL_REFRESH_S
                    if full_refresh_due:
                        self.can.refresh(self.home_module, response_window_s=self.args.telemetry_response_s)
                        last_full_refresh = time.monotonic()
                    else:
                        self.can.poll_telemetry(self.home_module, response_window_s=self.args.telemetry_response_s)
                    with self.module_lock:
                        self._snapshot_module_locked()
                elif force_refresh:
                    self.can.refresh(self.home_module, response_window_s=self.args.telemetry_response_s)
                    last_full_refresh = time.monotonic()
                    with self.module_lock:
                        self._snapshot_module_locked()

                with self.module_lock:
                    self.plan_active = plan_active
                    self.last_output_signature = last_signature
                    self.last_set_refresh = last_set_refresh
                    if applied_now and require_fresh_sample_after_apply:
                        self.last_target_apply_ts = loop_started
            except Exception as exc:
                self.module_worker_error = str(exc)
                return

            sleep_s = self._module_worker_interval_s() - (time.monotonic() - loop_started)
            if sleep_s > 0.0:
                self.module_worker_stop.wait(sleep_s)

    def _start_module_worker(self) -> None:
        if self.home_module is None:
            raise RuntimeError("home module not ready")
        if self.module_worker_thread and self.module_worker_thread.is_alive():
            return
        with self.module_lock:
            self._snapshot_module_locked()
            self.module_plan = ControlPlan()
            self.module_force_refresh = True
            self.module_force_apply = True
        self.module_worker_error = None
        self.module_worker_stop.clear()
        self.module_worker_thread = threading.Thread(target=self._module_worker_loop, daemon=True)
        self.module_worker_thread.start()

    def _stop_module_worker(self) -> None:
        self.module_worker_stop.set()
        if self.module_worker_thread and self.module_worker_thread.is_alive():
            self.module_worker_thread.join(timeout=2.0)
        self.module_worker_thread = None

    def _check_module_worker(self) -> None:
        if self.module_worker_error:
            raise RuntimeError(f"module worker failed: {self.module_worker_error}")
        if self.module_worker_thread and not self.module_worker_thread.is_alive():
            raise RuntimeError("module worker stopped unexpectedly")

    def _target_voltage(self) -> float:
        with self.live_lock:
            if self.live.target_v > 0.0:
                return self.live.target_v
            if self.live.latched_target_v > 0.0:
                return self.live.latched_target_v
        return self.current_plan.voltage_v if self.current_plan.enabled else 0.0

    def _requested_current(self) -> float:
        with self.live_lock:
            if self.live.bms_stage_name in ("precharge", "power_delivery", "current_demand"):
                return self.live.target_i
            if self.live.target_i > 0.0:
                return self.live.target_i
        return 0.0

    def _live_stage_snapshot(self) -> tuple[str, float, float]:
        with self.live_lock:
            stage = self.live.bms_stage_name
            target_v = self.live.target_v if self.live.target_v > 0.0 else self.live.latched_target_v
            if stage in ("precharge", "power_delivery", "current_demand"):
                requested_i = self.live.target_i
            elif self.live.target_i > 0.0:
                requested_i = self.live.target_i
            else:
                requested_i = 0.0
        return stage, target_v, requested_i

    def _apply_stage_plan(self, stage: str, target_v: float, requested_i: float) -> tuple[str, float, float]:
        if stage != self.stage_name:
            prev_stage = self.stage_name
            self.stage_name = stage
            if not (prev_stage == "precharge" and stage == "power_delivery"):
                self.precharge_feedback_count = 0
        command_v, command_i = self._effective_stage_command(stage, target_v, requested_i)
        if stage == "precharge":
            self.current_plan = ControlPlan(True, command_v, command_i)
            self.desired_relay = True
        elif stage in ("power_delivery", "current_demand"):
            self.current_plan = ControlPlan(True, command_v, command_i)
            self.desired_relay = True
        else:
            self.current_plan = ControlPlan(False, 0.0, 0.0)
            self.desired_relay = False
        return stage, target_v, requested_i

    def _refresh_plan_from_live(self) -> tuple[str, float, float]:
        return self._apply_stage_plan(*self._live_stage_snapshot())

    def _aggregate_feedback_telemetry(self) -> Optional[tuple[float, float]]:
        snapshot = self._get_module_snapshot()
        if not snapshot.online:
            return None
        if snapshot.last_update_ts <= 0.0:
            return None
        if (time.monotonic() - snapshot.last_update_ts) > max(1.0, self.args.telemetry_interval_s * 4.0):
            return None
        return (snapshot.voltage_v, snapshot.current_a)

    def _effective_stage_current(self, stage: str, requested_i: float) -> float:
        if stage == "precharge":
            return requested_i if requested_i > 0.05 else max(0.0, self.args.precharge_current_a)
        return requested_i if requested_i > 0.05 else 0.0

    def _effective_stage_voltage(self, stage: str, target_v: float, requested_i: float) -> float:
        if (
            stage == "current_demand"
            and requested_i > 0.05
            and self.args.apply_current_demand_headroom
        ):
            return max(0.0, target_v - self.args.module_voltage_headroom_v)
        return max(0.0, target_v)

    def _effective_stage_command(self, stage: str, target_v: float, requested_i: float) -> tuple[float, float]:
        command_v = self._effective_stage_voltage(stage, target_v, requested_i)
        command_i = self._effective_stage_current(stage, requested_i)
        if (
            stage == "current_demand"
            and requested_i > 0.05
            and self.args.apply_current_demand_headroom
            and requested_i > DEFAULT_LOW_REQUEST_RUNDOWN_MAX_A
        ):
            # Match the known-good standalone/controller stop tail behavior:
            # keep the configured current headroom during active charging, but
            # do not undercut the EV's own low-current rundown request (for
            # example 3 A -> 2 A) right before 0 A / PowerDelivery(stop).
            command_i = max(0.0, command_i - self.args.module_current_headroom_a)
        return command_v, command_i

    def _current_demand_current_ready_safe(self, requested_i: float, present_i: float) -> bool:
        target_i = max(0.0, requested_i)
        measured_i = max(0.0, present_i)
        if target_i <= 0.05:
            return measured_i <= DEFAULT_ZERO_REQUEST_READY_MAX_A
        overshoot_tolerance_a = max(2.0, target_i * 0.15)
        return measured_i <= (target_i + overshoot_tolerance_a)

    def _current_demand_voltage_ready_safe(self, requested_v: float, applied_v: float, present_v: float) -> bool:
        target_v = max(0.0, applied_v)
        if target_v < 50.0:
            target_v = max(0.0, requested_v)
        measured_v = max(0.0, present_v)
        if target_v <= 1.0:
            return measured_v <= max(1.0, self.args.feedback_voltage_tolerance_v)
        overshoot_tolerance_v = max(20.0, target_v * 0.05)
        return measured_v <= (target_v + overshoot_tolerance_v)

    def _delivery_feedback_ready(self,
                                 relay_closed: bool,
                                 requested_v: float,
                                 applied_v: float,
                                 requested_i: float,
                                 present_v: float,
                                 present_i: float) -> bool:
        if max(requested_v, applied_v) <= 10.0 or not relay_closed or present_v < 1.0:
            return False
        if not self._current_demand_voltage_ready_safe(requested_v, applied_v, present_v):
            return False
        return self._current_demand_current_ready_safe(requested_i, present_i)

    def _precharge_feedback_ready(self, relay_closed: bool, target_v: float, present_v: float, present_i: float) -> bool:
        self.precharge_feedback_count += 1
        tolerance_v = (
            self.args.feedback_voltage_tolerance_v
            if target_v <= 1.0
            else max(self.args.feedback_voltage_tolerance_v, target_v * 0.05)
        )
        if target_v <= 1.0:
            voltage_ready = present_v <= tolerance_v
        else:
            voltage_ready = abs(present_v - target_v) <= tolerance_v or present_v >= (target_v - tolerance_v)
        accept_floor_v = max(self.args.precharge_ready_min_voltage_v, target_v * 0.05)
        current_flow_visible = present_i >= self.args.current_demand_ready_min_current_a
        strong_single_sample_ready = (
            self.precharge_feedback_count >= 1
            and present_i >= (self.args.precharge_current_a * 0.75)
        )
        relaxed_ready = (
            current_flow_visible
            and present_v >= accept_floor_v
            and (self.precharge_feedback_count >= 2 or strong_single_sample_ready)
        )
        return relay_closed and (voltage_ready or relaxed_ready)

    def _current_demand_feedback_ready(
        self,
        relay_closed: bool,
        target_applied: bool,
        sample_valid: bool,
        sample_ts: float,
    ) -> bool:
        return relay_closed and target_applied and sample_valid and sample_ts >= self.last_target_apply_ts

    def _update_current_demand_rundown(self, now: float, stage: str, requested_i: float) -> None:
        target_i = max(0.0, requested_i)
        low_request = stage == "current_demand" and 0.05 < target_i <= DEFAULT_LOW_REQUEST_RUNDOWN_MAX_A
        if low_request:
            trigger_delta = max(DEFAULT_LOW_REQUEST_RUNDOWN_TRIGGER_DELTA_A, target_i * 2.0)
            if self.last_feedback_requested_i > (target_i + trigger_delta):
                self.current_demand_rundown_until = now + DEFAULT_LOW_REQUEST_RUNDOWN_GRACE_S
        else:
            self.current_demand_rundown_until = 0.0
        self.last_feedback_requested_i = target_i

    def _current_demand_rundown_active(self, now: float, requested_i: float, present_i: float) -> bool:
        target_i = max(0.0, requested_i)
        measured_i = max(0.0, present_i)
        return (
            0.05 < target_i <= DEFAULT_LOW_REQUEST_RUNDOWN_MAX_A
            and now < self.current_demand_rundown_until
            and measured_i >= target_i
            and measured_i <= (target_i + DEFAULT_LOW_REQUEST_READY_OVERSHOOT_MAX_A)
        )

    def _relay_refresh_suppressed(self) -> bool:
        stage, _, requested_i = self._live_stage_snapshot()
        return self.vehicle_stop_requested or (stage == "current_demand" and requested_i <= 0.05)

    def _delivery_feedback_limits(self, stage: str, requested_v: float, requested_i: float) -> tuple[bool, bool, bool]:
        req_v = max(0.0, requested_v)
        req_i = max(0.0, requested_i)
        applied_i = max(0.0, self.current_plan.current_a)
        headroom_i = (
            max(0.0, self.args.module_current_headroom_a)
            if stage == "current_demand" and req_i > 0.05 and self.args.apply_current_demand_headroom
            else 0.0
        )
        req_power_kw = (req_v * req_i) / 1000.0
        effective_req_power_kw = (req_v * applied_i) / 1000.0
        requested_power_headroom_kw = (req_v * headroom_i) / 1000.0

        # Mirror the standalone CurrentDemand truthfulness rule: preserve the
        # configured 10 V / 1 A headroom physically, but do not claim EVSE
        # current/power saturation unless the request exceeds the intended
        # headroom-adjusted command or the module's own capability registers.
        current_limit = req_i > (applied_i + headroom_i + 0.1)
        power_limit = req_power_kw > (effective_req_power_kw + requested_power_headroom_kw + 0.1)
        voltage_limit = req_v > (MODULE_MAX_VOLTAGE_V + 0.5)

        # Maxwell's live current-limit register tracks the currently commanded
        # output, not the module's physical capability. Use only the rated
        # capability here so normal 10 V / 1 A headroom does not masquerade as
        # EVSE saturation.
        available_current_a = module_available_current_value(self.home_module)
        if available_current_a > 0.0 and req_i > (available_current_a + 0.1):
            current_limit = True
        available_power_kw = 0.0
        if available_current_a > 0.0 and req_v > 0.1:
            available_power_kw = min(MXR_DEFAULT_OUTPUT_POWER_KW, (req_v * available_current_a) / 1000.0)
        if available_power_kw > 0.0 and req_power_kw > (available_power_kw + 0.1):
            power_limit = True

        return current_limit, voltage_limit, power_limit

    def _send_feedback_state(
        self,
        now: float,
        *,
        valid: bool,
        ready: bool,
        present_v: float,
        present_i: float,
        current_limit: bool = False,
        voltage_limit: bool = False,
        power_limit: bool = False,
        stop_notify: bool = False,
        timeout_s: float = 2.0,
        require_ack: bool = True,
    ) -> bool:
        cmd = (
            f"CTRL FEEDBACK {1 if valid else 0} {1 if ready else 0} "
            f"{max(0.0, present_v):.1f} {max(0.0, present_i):.1f} "
            f"{1 if current_limit else 0} {1 if voltage_limit else 0} "
            f"{1 if power_limit else 0} {1 if stop_notify else 0}"
        )
        if require_ack:
            ack = self._send_expect_ack(
                cmd,
                ACK_CMD_FEEDBACK,
                timeout_s=timeout_s,
                retries=1,
            )
            if ack.status != ACK_OK:
                raise RuntimeError(f"CTRL FEEDBACK rejected status={ack.status}")
        else:
            self._send_untracked_ackable(cmd, ACK_CMD_FEEDBACK)
        self.last_feedback_cmd = now
        return ready

    def _welding_feedback_active(self, stage: str) -> bool:
        if (
            self.stop_contract_started_at is None
            or self.current_demand_started_at is None
            or not self.args.fake_welding
        ):
            return False
        with self.live_lock:
            if self.live.welding_req_count > self.stop_contract_baseline_welding_req_count:
                return True
            return self.live.power_delivery_stop_runtime_count > self.stop_contract_baseline_power_delivery_stop_runtime_count

    def _update_plan_from_stage(self) -> tuple[str, float, float]:
        return self._refresh_plan_from_live()

    def _tick_contract(self, now: float) -> None:
        with self.live_lock:
            cp_phase = self.live.cp_phase
            b1_since_ts = self.live.b1_since_ts
            slac_session = self.live.slac_session
            slac_matched = self.live.slac_matched
            hlc_active = self.live.hlc_active
            session_started = self.live.session_started

        active_window = (
            hlc_active
            or slac_matched
            or slac_session
            or session_started
            or self.stage_name in ("precharge", "power_delivery", "current_demand")
        )

        if (now - self.last_hb) * 1000.0 >= self.args.heartbeat_ms:
            if active_window:
                self._send_untracked_ackable(f"CTRL HB {self.args.heartbeat_timeout_ms}", ACK_CMD_HEARTBEAT)
            else:
                ack = self._send_expect_ack(
                    f"CTRL HB {self.args.heartbeat_timeout_ms}",
                    ACK_CMD_HEARTBEAT,
                    timeout_s=2.0,
                    retries=1,
                )
                if ack.status != ACK_OK:
                    raise RuntimeError(f"CTRL HB rejected status={ack.status}")
            self.last_hb = now

        active_auth_refresh_s = max(1.0, min(5.0, self.args.auth_ttl_ms / 4000.0))
        idle_auth_refresh_s = max(1.0, min(self.args.auth_ttl_ms / 2000.0, (self.args.auth_ttl_ms - 1500) / 1000.0))
        auth_refresh_s = active_auth_refresh_s if active_window else idle_auth_refresh_s
        if (now - self.last_auth) >= auth_refresh_s:
            if active_window:
                self._send_untracked_ackable(f"CTRL AUTH grant {self.args.auth_ttl_ms}", ACK_CMD_AUTH)
            else:
                ack = self._send_expect_ack(
                    f"CTRL AUTH grant {self.args.auth_ttl_ms}",
                    ACK_CMD_AUTH,
                    timeout_s=2.0,
                    retries=1,
                )
                if ack.status != ACK_OK:
                    raise RuntimeError(f"CTRL AUTH grant rejected status={ack.status}")
            self.last_auth = now

        explicit_status_due = (now - self.last_status_poll) >= self.args.status_interval_s
        status_stale = self.last_status_rx <= 0.0 or (now - self.last_status_rx) >= max(1.0, self.args.status_interval_s)
        if (not active_window) and explicit_status_due and status_stale:
            try:
                self._query_status(
                    timeout_s=min(1.0, self.args.status_interval_s),
                    allow_stale=True,
                    stale_max_age_s=max(3.0, self.args.status_interval_s * 2.5),
                )
            except Exception as exc:
                self._note_once(f"status poll degraded during {self.phase}: {exc}")
            self.last_status_poll = now
        elif (not active_window) and explicit_status_due:
            self.last_status_poll = now

        waiting_for_slac = (not slac_matched) and (not hlc_active)
        at_b1 = cp_phase == "B1"
        in_digital_comm_window = cp_phase in ("B2", "C", "D")
        start_window_ready = (
            waiting_for_slac
            and at_b1
            and b1_since_ts is not None
            and (now - b1_since_ts) >= self.args.b1_start_delay_s
        )
        if (start_window_ready or (waiting_for_slac and in_digital_comm_window)) and (not self.remote_start_sent):
            self._send_slac_start(now)
            if start_window_ready:
                print("[CTRL] remote start -> B1 to B2", flush=True)
        elif cp_phase == "A":
            self.remote_start_sent = False

    def _tick_relay(self, now: float) -> None:
        relay_refresh_s = max(2.0, self.args.relay_refresh_s)
        if self.desired_relay:
            refresh_due = (
                (now - self.last_relay_refresh) >= relay_refresh_s
                and not self._relay_refresh_suppressed()
            )
            if self.last_relay_command is not True or not self._relay_is_closed() or refresh_due:
                self._set_relay(True, self.args.relay_hold_ms, require_ack=False)
                self.last_relay_refresh = now
                self.last_relay_command = True
        else:
            if self.last_relay_command is not False or not self._relay_is_open():
                self._set_relay(False, 0, require_ack=False)
                self.last_relay_refresh = now
                self.last_relay_command = False

    def _tick_module_output(self, now: float) -> None:
        self._refresh_plan_from_live()
        if self.home_module is None:
            return
        signature = (
            self.current_plan.enabled,
            int(round(max(0.0, self.current_plan.voltage_v) * 10.0)),
            int(round(max(0.0, self.current_plan.current_a) * 100.0)),
        )
        force_apply = False
        force_refresh = False
        if self.current_plan.enabled:
            if self.last_output_signature != signature:
                force_apply = True
                force_refresh = True
        elif self.plan_active or self.last_output_signature is not None:
            force_apply = True
            force_refresh = True
        self._publish_module_plan(force_refresh=force_refresh, force_apply=force_apply)

    def _tick_telemetry(self, now: float) -> None:
        self._check_module_worker()
        snapshot = self._get_module_snapshot()
        if snapshot.last_update_ts <= 0.0 or (time.monotonic() - snapshot.last_update_ts) > max(1.0, self.args.telemetry_interval_s * 4.0):
            self._publish_module_plan(force_refresh=True)
        self.last_telemetry_poll = now

    def _tick_feedback(self,
                       now: float,
                       stage: str,
                       target_v: float,
                       requested_i: float,
                       *,
                       stop_notify: bool = False) -> bool:
        stage, target_v, requested_i = self._refresh_plan_from_live()
        self._update_current_demand_rundown(now, stage, requested_i)
        feedback_signature = (
            stage,
            int(round(max(0.0, target_v) * 10.0)),
            int(round(max(0.0, requested_i) * 100.0)),
            stop_notify,
        )
        welding_zero_due = False
        if stop_notify and self.stop_contract_started_at is not None and self.args.fake_welding:
            with self.live_lock:
                welding_zero_due = (
                    self.live.welding_runtime_count - self.stop_contract_baseline_welding_runtime_count
                ) >= self.args.fake_welding_residual_count
        active_charge_window = stage in ("precharge", "power_delivery", "current_demand") and not stop_notify
        feedback_interval_s = (
            self.args.active_feedback_keepalive_s if active_charge_window else self.args.feedback_interval_s
        )
        feedback_due = (
            self.last_feedback_signature != feedback_signature
            or welding_zero_due
            or (now - self.last_feedback_cmd) >= feedback_interval_s
        )
        if not feedback_due:
            return self.last_feedback_ready if self.last_feedback_signature == feedback_signature else False
        telemetry = self._aggregate_feedback_telemetry()
        welding_active = self._welding_feedback_active(stage)
        if welding_active:
            with self.live_lock:
                welding_runtime_seen_count = max(
                    0,
                    self.live.welding_runtime_count - self.stop_contract_baseline_welding_runtime_count,
                )
            present_v = 0.0
            if welding_runtime_seen_count < self.args.fake_welding_residual_count:
                present_v = self.args.fake_welding_residual_v
            ready = self._send_feedback_state(
                now,
                valid=True,
                ready=False,
                present_v=present_v,
                present_i=0.0,
                stop_notify=True,
            )
            self.last_feedback_signature = feedback_signature
            self.last_feedback_ready = ready
            return ready
        if telemetry is None:
            if stop_notify:
                ready = self._send_feedback_state(
                    now,
                    valid=True,
                    ready=False,
                    present_v=0.0,
                    present_i=0.0,
                    stop_notify=True,
                )
                self.last_feedback_signature = feedback_signature
                self.last_feedback_ready = ready
                return ready
            if active_charge_window:
                self._send_untracked_ackable("CTRL FEEDBACK 0 0 0 0 0 0 0", ACK_CMD_FEEDBACK)
            else:
                ack = self._send_expect_ack("CTRL FEEDBACK 0 0 0 0 0 0 0", ACK_CMD_FEEDBACK, timeout_s=2.0, retries=1)
                if ack.status != ACK_OK:
                    raise RuntimeError(f"CTRL FEEDBACK invalid rejected status={ack.status}")
            self.last_feedback_cmd = now
            self.last_feedback_signature = feedback_signature
            self.last_feedback_ready = False
            return False

        present_v, present_i = telemetry
        ready = False
        if stage == "precharge":
            relay_closed = self._relay_is_closed() or self.desired_relay
            ready = self._precharge_feedback_ready(relay_closed, target_v, present_v, present_i)
            ready = self._send_feedback_state(
                now,
                valid=True,
                ready=ready,
                present_v=present_v,
                present_i=present_i,
                stop_notify=stop_notify,
                require_ack=not active_charge_window,
            )
            self.last_feedback_signature = feedback_signature
            self.last_feedback_ready = ready
            return ready
        elif stage == "power_delivery":
            relay_closed = self._relay_is_closed()
            ready = self._precharge_feedback_ready(relay_closed, target_v, present_v, present_i)
            ready = self._send_feedback_state(
                now,
                valid=True,
                ready=ready,
                present_v=present_v,
                present_i=present_i,
                stop_notify=stop_notify,
                require_ack=not active_charge_window,
            )
            self.last_feedback_signature = feedback_signature
            self.last_feedback_ready = ready
            return ready
        elif stage == "current_demand":
            relay_closed = self._relay_is_closed()
            snapshot = self._get_module_snapshot()
            target_applied = self.current_plan.enabled and (self.plan_active or self.last_output_signature is not None)
            ready = self._current_demand_feedback_ready(
                relay_closed,
                target_applied,
                snapshot.online,
                snapshot.last_update_ts,
            )
            current_limit, voltage_limit, power_limit = self._delivery_feedback_limits(stage, target_v, requested_i)
            ready = self._send_feedback_state(
                now,
                valid=True,
                ready=ready,
                present_v=present_v,
                present_i=present_i,
                current_limit=current_limit,
                voltage_limit=voltage_limit,
                power_limit=power_limit,
                stop_notify=stop_notify,
                require_ack=not active_charge_window,
            )
            self.last_feedback_signature = feedback_signature
            self.last_feedback_ready = ready
            return ready
        else:
            ready = self._send_feedback_state(
                now,
                valid=True,
                ready=False,
                present_v=present_v,
                present_i=present_i,
                stop_notify=stop_notify,
            )
            self.last_feedback_signature = feedback_signature
            self.last_feedback_ready = ready
            return ready

    def _tick_cleanup_contract(self, now: float) -> None:
        if (now - self.last_hb) * 1000.0 >= self.args.heartbeat_ms:
            try:
                ack = self._send_expect_ack(
                    f"CTRL HB {self.args.heartbeat_timeout_ms}",
                    ACK_CMD_HEARTBEAT,
                    timeout_s=2.0,
                    retries=1,
                )
                if ack.status == ACK_OK:
                    self.last_hb = now
                else:
                    self._note_once(f"cleanup heartbeat rejected status={ack.status}")
            except Exception as exc:
                self._note_once(f"cleanup heartbeat degraded: {exc}")

        if (now - self.last_feedback_cmd) >= self.args.feedback_interval_s:
            try:
                ack = self._send_expect_ack(
                    "CTRL FEEDBACK 1 0 0 0 0 0 0 1",
                    ACK_CMD_FEEDBACK,
                    timeout_s=2.0,
                    retries=1,
                )
                if ack.status != ACK_OK:
                    self._note_once(f"cleanup stop-notify rejected status={ack.status}")
                self.last_feedback_cmd = now
            except Exception as exc:
                self._note_once(f"cleanup stop-notify degraded: {exc}")

        if (now - self.last_status_poll) >= max(0.25, min(1.0, self.args.status_interval_s)):
            try:
                self._query_status(timeout_s=0.8, allow_stale=True, stale_max_age_s=3.0)
            except Exception as exc:
                self._note_once(f"cleanup status degraded: {exc}")
            self.last_status_poll = now

    def _tick_progress(self, now: float, stage: str, ready: bool) -> None:
        if (now - self.last_progress) < 5.0:
            return
        snapshot = self._get_module_snapshot()
        present_v = snapshot.voltage_v
        present_i = snapshot.current_a
        present_power_w = present_v * present_i
        print(
            f"[PROGRESS] phase={self.phase} stage={stage or '-'} relay1={int(self._relay_is_closed())} "
            f"targetV={self._target_voltage():.1f} targetI={self._requested_current():.1f} "
            f"presentV={present_v:.1f} presentI={present_i:.1f} presentP={present_power_w:.0f} ready={int(ready)} "
            f"cp={self.live.cp_phase} slac={int(self.live.slac_matched)} hlc={int(self.live.hlc_ready)}",
            flush=True,
        )
        self.last_progress = now

    def _charge_success_reached(self, stage: str, ready: bool) -> bool:
        now = time.time()
        snapshot = self._get_module_snapshot()
        present_v = snapshot.voltage_v
        present_i = snapshot.current_a
        present_power_w = present_v * present_i

        if stage == "current_demand":
            if self.current_demand_started_at is None:
                self.current_demand_started_at = now
            if ready:
                if self.current_demand_ready_since is None:
                    self.current_demand_ready_since = now
            else:
                self.current_demand_ready_since = None
        else:
            self.current_demand_ready_since = None

        proof_now = (
            stage == "current_demand"
            and ready
            and present_v >= self.args.charge_success_min_voltage_v
            and present_i >= self.args.charge_success_min_current_a
            and present_power_w >= self.args.charge_success_min_power_w
        )
        if proof_now:
            if self.charge_proof_since is None:
                self.charge_proof_since = now
                print(
                    f"[PASSCHK] charge proof started V={present_v:.1f} I={present_i:.1f} P={present_power_w:.0f}",
                    flush=True,
                )
            self.charge_proof_peak_v = max(self.charge_proof_peak_v, present_v)
            self.charge_proof_peak_i = max(self.charge_proof_peak_i, present_i)
            self.charge_proof_peak_power_w = max(self.charge_proof_peak_power_w, present_power_w)
            return (now - self.charge_proof_since) >= self.args.current_demand_hold_s

        if self.charge_proof_since is not None:
            print(
                f"[PASSCHK] charge proof reset stage={stage or '-'} ready={int(ready)} "
                f"V={present_v:.1f} I={present_i:.1f} P={present_power_w:.0f}",
                flush=True,
            )
        self.charge_proof_since = None
        return False

    def _cleanup_force_stop(self) -> None:
        try:
            ack = self._send_expect_ack(
                f"CTRL STOP soft {int(self.args.stop_timeout_s * 1000.0)}",
                ACK_CMD_STOP,
                timeout_s=3.0,
                retries=1,
            )
            if ack.status not in (ACK_OK, 4):
                self.notes.append(f"CTRL STOP soft rejected status={ack.status}; forcing hard stop")
                raise RuntimeError("soft stop rejected")
        except Exception:
            try:
                self._send_expect_ack("CTRL STOP hard 3000", ACK_CMD_STOP, timeout_s=3.0, retries=1)
            except Exception:
                self.notes.append("hard stop ack missing during cleanup")

        self.current_plan = ControlPlan(False, 0.0, 0.0)
        self.desired_relay = False
        self.last_feedback_signature = None
        self._publish_module_plan(force_refresh=True, force_apply=True)

        stop_deadline = time.time() + self.args.stop_timeout_s
        self.last_feedback_cmd = 0.0
        self.last_feedback_signature = None
        while time.time() < stop_deadline:
            now = time.time()
            self._check_module_worker()
            self._tick_relay(now)
            self._tick_telemetry(now)
            with self.live_lock:
                done = self.live.stop_done
                hlc_active = self.live.hlc_active
                cp_phase = self.live.cp_phase
            self._tick_cleanup_contract(now)
            if done and self._relay_is_open():
                break
            if self._relay_is_open() and (not hlc_active) and cp_phase in ("A", "B", "B1", "B2"):
                break
            time.sleep(0.1)

        try:
            self._send_expect_ack(f"CTRL AUTH deny {self.args.auth_ttl_ms}", ACK_CMD_AUTH, timeout_s=2.0, retries=1)
        except Exception:
            pass
        try:
            self._send_expect_ack("CTRL SLAC disarm 3000", ACK_CMD_SLAC, timeout_s=2.0, retries=1)
        except Exception:
            pass
        try:
            self._send_expect_ack("CTRL FEEDBACK 1 0 0 0 0 0 0", ACK_CMD_FEEDBACK, timeout_s=2.0, retries=1)
        except Exception:
            pass
        try:
            self._set_relay(False, 0, require_ack=False)
        except Exception:
            pass

    def _cleanup(self) -> None:
        self._set_phase("stopping")
        self.stop_contract_started_at = time.time()
        with self.live_lock:
            self.stop_contract_baseline_power_delivery_stop_runtime_count = self.live.power_delivery_stop_runtime_count
            self.stop_contract_baseline_welding_req_count = self.live.welding_req_count
            self.stop_contract_baseline_welding_runtime_count = self.live.welding_runtime_count
            self.stop_contract_baseline_session_stop_req_count = self.live.session_stop_req_count
            self.stop_contract_baseline_client_session_done_count = self.live.client_session_done_count
        self.last_feedback_cmd = 0.0
        self.last_feedback_signature = None

        graceful_complete = False
        graceful_deadline = time.time() + self.args.stop_timeout_s
        while time.time() < graceful_deadline:
            now = time.time()
            self._check_module_worker()
            stage, target_v, requested_i = self._update_plan_from_stage()
            self._tick_relay(now)
            self._tick_module_output(now)
            self._tick_telemetry(now)
            self._tick_feedback(now, stage, target_v, requested_i, stop_notify=True)
            self._tick_contract(now)
            self._tick_progress(now, stage, False)
            with self.live_lock:
                done = self.live.stop_done
                hlc_active = self.live.hlc_active
                session_stop_req_count = self.live.session_stop_req_count
                client_session_done_count = self.live.client_session_done_count
            if done and self._relay_is_open():
                graceful_complete = True
                break
            if (
                client_session_done_count > self.stop_contract_baseline_client_session_done_count
                and self._relay_is_open()
            ):
                graceful_complete = True
                break
            if (
                session_stop_req_count > self.stop_contract_baseline_session_stop_req_count
                and self._relay_is_open()
                and (not hlc_active)
            ):
                graceful_complete = True
                break
            time.sleep(0.05)

        self.stop_contract_started_at = None
        if not graceful_complete:
            self.notes.append("orderly stop contract timed out; forcing PLC stop")
            self._cleanup_force_stop()
            return

        self.current_plan = ControlPlan(False, 0.0, 0.0)
        self.desired_relay = False
        self._publish_module_plan(force_refresh=True, force_apply=True)
        try:
            self._send_expect_ack(f"CTRL AUTH deny {self.args.auth_ttl_ms}", ACK_CMD_AUTH, timeout_s=2.0, retries=1)
        except Exception:
            pass
        try:
            self._send_expect_ack("CTRL SLAC disarm 3000", ACK_CMD_SLAC, timeout_s=2.0, retries=1)
        except Exception:
            pass
        try:
            self._send_expect_ack("CTRL FEEDBACK 1 0 0 0 0 0 0", ACK_CMD_FEEDBACK, timeout_s=2.0, retries=1)
        except Exception:
            pass
        try:
            self._set_relay(False, 0, require_ack=False)
        except Exception:
            pass

    def _write_summary(self, result: str) -> None:
        snapshot = self._get_module_snapshot()
        payload = {
            "result": result,
            "phase": self.phase,
            "events": self.events,
            "notes": self.notes,
            "failures": self.failures,
            "current_demand_started_at": self.current_demand_started_at,
            "current_demand_ready_since": self.current_demand_ready_since,
            "charge_proof_since": self.charge_proof_since,
            "charge_proof_peak_v": self.charge_proof_peak_v,
            "charge_proof_peak_i": self.charge_proof_peak_i,
            "charge_proof_peak_power_w": self.charge_proof_peak_power_w,
            "vehicle_stop_requested": self.vehicle_stop_requested,
            "vehicle_stop_reason": self.vehicle_stop_reason,
            "plc": {
                "mode_id": self.live.mode_id,
                "mode_name": self.live.mode_name,
                "can_stack": self.live.can_stack,
                "module_mgr": self.live.module_mgr,
                "cp_phase": self.live.cp_phase,
                "cp_connected": self.live.cp_connected,
                "cp_duty_pct": self.live.cp_duty_pct,
                "slac_session": self.live.slac_session,
                "slac_matched": self.live.slac_matched,
                "slac_fsm": self.live.slac_fsm,
                "slac_failures": self.live.slac_failures,
                "hlc_ready": self.live.hlc_ready,
                "hlc_active": self.live.hlc_active,
                "precharge_seen": self.live.precharge_seen,
                "precharge_ready": self.live.precharge_ready,
                "session_started": self.live.session_started,
                "stop_active": self.live.stop_active,
                "stop_hard": self.live.stop_hard,
                "stop_done": self.live.stop_done,
                "bms_stage_name": self.live.bms_stage_name,
                "delivery_ready": self.live.delivery_ready,
                "target_v": self.live.target_v,
                "target_i": self.live.target_i,
                "fb_v": self.live.fb_v,
                "fb_i": self.live.fb_i,
                "power_delivery_req_count": self.live.power_delivery_req_count,
                "welding_req_count": self.live.welding_req_count,
                "session_stop_req_count": self.live.session_stop_req_count,
                "power_delivery_stop_runtime_count": self.live.power_delivery_stop_runtime_count,
                "welding_runtime_count": self.live.welding_runtime_count,
                "session_stop_runtime_count": self.live.session_stop_runtime_count,
                "client_session_done_count": self.live.client_session_done_count,
                "relay1": self.live.relay_states.get(1),
            },
            "module": {
                "address": self.home_module.address if self.home_module else None,
                "group": self.home_module.group if self.home_module else None,
                "rated_output_current_reg_a": snapshot.rated_output_current_reg_a,
                "status": snapshot.status_text,
                "voltage_v": snapshot.voltage_v,
                "current_a": snapshot.current_a,
                "current_limit_point": snapshot.current_limit_point,
                "input_power_w": snapshot.input_power_w,
                "input_mode": snapshot.input_mode,
                "off": snapshot.off,
            },
        }
        ensure_parent(self.args.summary_file)
        with open(self.args.summary_file, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, indent=2)

    def run(self) -> int:
        result = "fail"
        try:
            self._reset_plc_if_requested()
            self.link.open()
            self.can.open()
            print(f"PLC_LOG={self.args.plc_log}", flush=True)
            print(f"CAN_LOG={self.args.can_log}", flush=True)
            print(f"SUMMARY_FILE={self.args.summary_file}", flush=True)

            self._set_phase("bootstrap")
            status = self._bootstrap_plc()
            print(
                f"[BOOT] mode={status.mode_name} plc_id={status.plc_id} connector_id={status.connector_id} "
                f"can_stack={status.can_stack} module_mgr={status.module_mgr}",
                flush=True,
            )

            self.home_module = self._probe_home_module()
            self.can.refresh(self.home_module, response_window_s=0.5)
            self.can.soft_reset(self.home_module, input_mode=self.args.input_mode)
            self.can.refresh(self.home_module, response_window_s=0.5)
            with self.module_lock:
                self._snapshot_module_locked()
            print(
                f"[MODULE] addr={self.home_module.address} grp={self.home_module.group} "
                f"rated={self.home_module.rated_current_a}A {module_status_text(self.home_module)}",
                flush=True,
            )
            self._start_module_worker()

            self._set_phase("wait_for_session")
            start_deadline = time.time() + self.args.session_start_timeout_s
            while True:
                if self.vehicle_stop_requested:
                    if self.current_demand_started_at is None:
                        raise RuntimeError(f"vehicle requested stop before CurrentDemand: {self.vehicle_stop_reason}")
                    raise RuntimeError(f"vehicle requested stop after CurrentDemand: {self.vehicle_stop_reason}")

                self._check_module_worker()
                stage, target_v, requested_i = self._update_plan_from_stage()
                now = time.time()
                if self.current_demand_started_at is None and now >= start_deadline:
                    self.failures.append("session never reached CurrentDemand before start timeout")
                    raise RuntimeError(self.failures[-1])
                self._tick_relay(now)
                self._tick_module_output(now)
                self._tick_telemetry(now)
                ready = self._tick_feedback(now, stage, target_v, requested_i)
                self._tick_contract(now)
                self._tick_progress(now, stage, ready)

                if self._charge_success_reached(stage, ready):
                    result = "pass"
                    module_desc = (
                        f"module {self.home_module.address}/g{self.home_module.group}"
                        if self.home_module is not None
                        else "requested module"
                    )
                    print(
                        f"[PASS] sustained charging reached with {module_desc} "
                        f"Vpk={self.charge_proof_peak_v:.1f} Ipk={self.charge_proof_peak_i:.1f} "
                        f"Ppk={self.charge_proof_peak_power_w:.0f}",
                        flush=True,
                    )
                    break
                time.sleep(0.01)

            if result != "pass":
                self.failures.append("session never reached sustained charging proof")
                raise RuntimeError(self.failures[-1])

            try:
                self._cleanup()
            except Exception as cleanup_exc:
                self._note_once(f"cleanup degraded after pass: {cleanup_exc}")
            return 0
        except Exception as exc:
            if str(exc):
                self.failures.append(str(exc))
            print(f"[FAIL] {exc}", flush=True)
            try:
                self._cleanup()
            except Exception as cleanup_exc:
                self.notes.append(f"cleanup failed: {cleanup_exc}")
            return 1
        finally:
            self._stop_module_worker()
            self._write_summary(result)
            self.can.close()
            self.link.close()


def build_arg_parser() -> argparse.ArgumentParser:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(
        description="Simple external-controller single-module end-to-end charge test"
    )
    parser.add_argument("--plc-port", default="auto")
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument("--plc-id", type=int, default=None)
    parser.add_argument("--controller-id", type=int, default=1)
    parser.add_argument("--power-relay", type=int, default=1)
    parser.add_argument("--home-addr", type=int, default=DEFAULT_HOME_ADDR)
    parser.add_argument("--home-group", type=int, default=DEFAULT_HOME_GROUP)
    parser.add_argument("--input-mode", type=int, choices=(1, 2, 3), default=None)
    parser.add_argument("--can-iface", default="can0")
    parser.add_argument("--discovery-timeout-s", type=float, default=1.5)
    parser.add_argument("--esp-reset-before-start", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--esp-reset-pulse-s", type=float, default=DEFAULT_ESP_RESET_PULSE_S)
    parser.add_argument("--esp-reset-wait-s", type=float, default=DEFAULT_ESP_RESET_WAIT_S)
    parser.add_argument("--precharge-current-a", type=float, default=2.0)
    parser.add_argument("--current-demand-hold-s", type=float, default=15.0)
    parser.add_argument("--session-start-timeout-s", type=float, default=240.0)
    parser.add_argument("--b1-start-delay-s", type=float, default=0.6)
    parser.add_argument("--control-interval-s", type=float, default=0.10)
    parser.add_argument("--feedback-interval-s", type=float, default=0.02)
    parser.add_argument("--active-feedback-keepalive-s", type=float, default=0.02)
    parser.add_argument("--telemetry-interval-s", type=float, default=0.02)
    parser.add_argument("--telemetry-response-s", type=float, default=0.10)
    parser.add_argument("--status-interval-s", type=float, default=5.0)
    parser.add_argument("--heartbeat-ms", type=int, default=4000)
    parser.add_argument("--heartbeat-timeout-ms", type=int, default=20000)
    parser.add_argument("--auth-ttl-ms", type=int, default=25000)
    parser.add_argument("--arm-ms", type=int, default=45000)
    parser.add_argument("--relay-hold-ms", type=int, default=25000)
    parser.add_argument("--relay-refresh-s", type=float, default=20.0)
    parser.add_argument("--charge-success-min-voltage-v", type=float, default=100.0)
    parser.add_argument("--charge-success-min-current-a", type=float, default=10.0)
    parser.add_argument("--charge-success-min-power-w", type=float, default=1000.0)
    parser.add_argument("--feedback-voltage-tolerance-v", type=float, default=15.0)
    parser.add_argument("--precharge-ready-min-voltage-v", type=float, default=5.0)
    parser.add_argument("--precharge-ready-min-current-ratio", type=float, default=0.25)
    parser.add_argument("--current-demand-ready-min-voltage-v", type=float, default=0.1)
    parser.add_argument("--current-demand-ready-min-current-a", type=float, default=0.05)
    parser.add_argument("--apply-current-demand-headroom", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--module-voltage-headroom-v", type=float, default=DEFAULT_MODULE_VOLTAGE_HEADROOM_V)
    parser.add_argument("--module-current-headroom-a", type=float, default=DEFAULT_MODULE_CURRENT_HEADROOM_A)
    parser.add_argument("--fake-welding", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--fake-welding-residual-v", type=float, default=DEFAULT_FAKE_WELDING_RESIDUAL_V)
    parser.add_argument("--fake-welding-residual-count", type=int, default=1)
    parser.add_argument("--absolute-current-scale", type=float, default=1024.0)
    parser.add_argument("--stop-timeout-s", type=float, default=12.0)
    parser.add_argument(
        "--plc-log",
        default=f"/home/jpi/Desktop/EVSE/plc_firmware/logs/external_controller_single_module_charge_{stamp}.log",
    )
    parser.add_argument(
        "--can-log",
        default=f"/home/jpi/Desktop/EVSE/plc_firmware/logs/external_controller_single_module_charge_{stamp}_can.log",
    )
    parser.add_argument(
        "--summary-file",
        default=f"/home/jpi/Desktop/EVSE/plc_firmware/logs/external_controller_single_module_charge_{stamp}_summary.json",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    args.heartbeat_timeout_ms = max(3000, int(args.heartbeat_timeout_ms), int(args.heartbeat_ms) * 8)
    args.auth_ttl_ms = max(6000, int(args.auth_ttl_ms), int(args.heartbeat_timeout_ms) + 2000)
    resolve_plc_port(args)
    runner = SingleModuleChargeRunner(args)
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
