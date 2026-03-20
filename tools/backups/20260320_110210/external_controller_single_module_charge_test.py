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
from dataclasses import dataclass, field
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
ACK_CMD_RELAY = 0x17
ACK_CMD_STOP = 0x18
ACK_CMD_HEARTBEAT = 0x10
ACK_CMD_AUTH = 0x11
ACK_CMD_SLAC = 0x12
ACK_CMD_FEEDBACK = 0x1A
DEFAULT_ESP_RESET_PULSE_S = 0.2
DEFAULT_ESP_RESET_WAIT_S = 14.0
SERIAL_MIN_GAP_S = 0.01
PRECHARGE_VOLTAGE_TOLERANCE_V = 15.0
PRECHARGE_ACCEPT_MIN_V = 20.0
PRECHARGE_ACCEPT_MIN_CURRENT_A = 0.5
DELIVERY_ACTIVE_MIN_V = 10.0
DELIVERY_ACTIVE_MIN_CURRENT_A = 1.0
MODULE_WORKER_MIN_INTERVAL_S = 0.005
MODULE_WORKER_TARGET_INTERVAL_S = 0.01
MODULE_WORKER_FULL_REFRESH_S = 0.25
MODULE_OFF_RETRY_INTERVAL_S = 0.25
DEFAULT_CONTROL_INTERVAL_S = 0.15


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
HLC_JSON_REQ_RE = re.compile(r'\[HLC\]\[REQ\]\s+\{"type":"([A-Za-z]+)Req"')
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


def module_telemetry_suffix(module: Optional[mxr.ModuleInfo]) -> str:
    if module is None:
        return ""
    parts: list[str] = []
    current_limit_point = getattr(module, "current_limit_point", None)
    if current_limit_point is not None and math.isfinite(current_limit_point):
        parts.append(f"Ilim={current_limit_point}")
    input_power_w = getattr(module, "input_power_w", None)
    if input_power_w is not None and math.isfinite(input_power_w):
        parts.append(f"Pin={input_power_w}")
    input_mode = getattr(module, "input_mode", None)
    if input_mode is not None:
        parts.append(f"mode={input_mode}")
    rated_output_current_a = getattr(module, "rated_current_a", None)
    if rated_output_current_a is not None and math.isfinite(rated_output_current_a):
        parts.append(f"rated_reg={rated_output_current_a}")
    return (" " + " ".join(parts)) if parts else ""


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

@dataclass
class Ack:
    cmd_hex: int
    seq: int
    status: int
    detail0: int
    detail1: int


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

    def _build_serial(self) -> serial.Serial:
        kwargs = {
            "port": self.port,
            "baudrate": self.baud,
            "timeout": 0.05,
            "write_timeout": 0.5,
        }
        try:
            ser = serial.Serial(exclusive=True, **kwargs)
        except TypeError:
            ser = serial.Serial(**kwargs)
        ser.reset_input_buffer()
        ser.reset_output_buffer()
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
            wait_s = SERIAL_MIN_GAP_S - (time.time() - self.last_write_ts)
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
                raw = self.ser.readline() if self.ser else b""
            except Exception as exc:
                self._log(f"{ts()} [HOST] read failed: {exc}")
                if self._reconnect(f"read failed: {exc}"):
                    continue
                break
            if not raw:
                continue
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
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

    def _probe_expected(self, address: int, group: int, response_window_s: float = 0.8) -> Optional[mxr.ModuleInfo]:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        module = mxr.ModuleInfo(address=address, group=group)
        mxr.refresh_module_details(self.sock, module, response_window_s=response_window_s)
        if not module_online(module):
            return None
        self._log(
            f"PROBE addr={module.address} grp={module.group} V={module.voltage_v} I={module.current_a} "
            f"{module_telemetry_suffix(module)} {module_status_text(module)}"
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
        regs = [
            mxr.REG_GET_STATUS,
            mxr.REG_GET_VOLTAGE,
            mxr.REG_GET_CURRENT,
            mxr.REG_GET_CURRENT_LIMIT_POINT,
            mxr.REG_GET_INPUT_POWER,
            mxr.REG_GET_INPUT_MODE,
            mxr.REG_GET_RATED_CURRENT,
        ]
        for reg in regs:
            mxr.send_read(self.sock, dst_addr=module.address, group=module.group, reg=reg)
            time.sleep(0.001)
        mxr.poll_responses_until(self.sock, module, time.monotonic() + response_window_s)
        self._log(
            f"TEL addr={module.address} grp={module.group} V={module.voltage_v} I={module.current_a} "
            f"{module_telemetry_suffix(module)} {module_status_text(module)}"
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
            return

    def _module_shutdown(self, module: mxr.ModuleInfo) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        mxr.send_set_int(self.sock, module.address, module.group, mxr.REG_ONOFF, 0x00010000)

    def soft_reset(self, module: mxr.ModuleInfo) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        key = (module.address, module.group)
        mxr.send_set_int(self.sock, module.address, module.group, mxr.REG_SET_CURRENT_ABS, 0)
        time.sleep(0.02)
        self._log(f"RESET addr={module.address} grp={module.group} mode=factory_minimum")
        self._module_shutdown(module)
        time.sleep(0.08)
        mxr.send_set_int(self.sock, module.address, module.group, 0x0031, 0x00010000)
        time.sleep(0.02)
        mxr.send_set_int(self.sock, module.address, module.group, 0x0031, 0x00000000)
        time.sleep(0.02)
        mxr.send_set_int(self.sock, module.address, module.group, 0x0044, 0x00010000)
        time.sleep(0.02)
        mxr.send_set_int(self.sock, module.address, module.group, 0x0044, 0x00000000)
        time.sleep(0.04)
        self.last_on_attempt_at.pop(key, None)
        self.refresh(module, response_window_s=0.4)

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
            voltage_v=voltage_v,
            current_a=current_a,
            scale=self.abs_current_scale,
        )

    def set_output(self, module: mxr.ModuleInfo, voltage_v: float, current_a: float) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        safe_voltage_v, safe_current_a = self.normalize_output_request(voltage_v, current_a)
        current_raw = int(round(max(0.0, safe_current_a) * self.abs_current_scale))
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
        self._log(
            f"SET addr={module.address} grp={module.group} voltage_v={safe_voltage_v:.1f} current_a={safe_current_a:.2f} raw=0x{current_raw:08X}"
        )
        if safe_current_a > 0.05 and module_is_off(module) and (now - self.last_on_attempt_at.get(key, 0.0)) >= 0.25:
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
        self._module_shutdown(module)
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
        relay2=int(match.group(20)),
        relay3=int(match.group(21)),
        alloc_sz=int(match.group(22)),
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
    precharge_req_count: int = 0
    current_demand_req_count: int = 0
    target_v: float = 0.0
    target_i: float = 0.0
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
    last_precharge_req_ts: Optional[float] = None
    last_current_demand_req_ts: Optional[float] = None
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
        self.desired_relay = False

        self.last_relay_refresh = 0.0
        self.last_relay_command: Optional[bool] = None
        self.last_feedback_cmd = 0.0
        self.last_feedback_precharge_req_count = 0
        self.last_feedback_power_delivery_req_count = 0
        self.last_feedback_current_demand_req_count = 0
        self.last_feedback_ready = False
        self.last_feedback_stage = "none"
        self.last_status_poll = 0.0
        self.last_progress = 0.0
        self.last_hb = 0.0
        self.last_auth = 0.0
        self.last_slac_start = 0.0
        self.last_status_rx = 0.0
        self.remote_start_sent = False
        self.last_stage_name = "none"
        self.last_plan_marker = -1

        self.current_demand_started_at: Optional[float] = None
        self.current_demand_ready_since: Optional[float] = None
        self.charge_proof_since: Optional[float] = None
        self.charge_proof_peak_v: float = 0.0
        self.charge_proof_peak_i: float = 0.0
        self.charge_proof_peak_power_w: float = 0.0
        self.vehicle_stop_requested = False
        self.vehicle_stop_reason = ""

    def _note_once(self, note: str) -> None:
        if note not in self.notes:
            self.notes.append(note)

    def _reset_plc_if_requested(self) -> None:
        if not self.args.esp_reset_before_start:
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
        print(f"[RESET] boot log captured in {reset_log}", flush=True)

    def _set_phase(self, name: str) -> None:
        self.phase = name
        self.events.append(f"{ts()} phase={name}")
        print(f"[PHASE] {name}", flush=True)

    def _parse_line(self, line: str) -> None:
        now = time.time()

        status = parse_status_line(line)
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
            self.status_queue.append(status)
            return

        ack_match = ACK_RE.search(line)
        if ack_match:
            self.ack_queue.append(
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

        hlc_json_req = HLC_JSON_REQ_RE.search(line)
        if hlc_json_req:
            kind = hlc_json_req.group(1)
            with self.live_lock:
                if kind == "PreCharge":
                    self.live.precharge_req_count += 1
                    self.live.last_precharge_req_ts = now
                elif kind == "CurrentDemand":
                    self.live.current_demand_req_count += 1
                    self.live.last_current_demand_req_ts = now
                elif kind == "PowerDelivery":
                    self.live.power_delivery_req_count += 1
                    self.live.last_power_delivery_req_ts = now
                elif kind == "WeldingDetection":
                    self.live.welding_req_count += 1
                    self.live.last_welding_req_ts = now
                elif kind == "SessionStop":
                    self.live.session_stop_req_count += 1
                    self.live.last_session_stop_req_ts = now
            if kind in ("WeldingDetection", "SessionStop") and self.current_demand_started_at is not None:
                self.vehicle_stop_requested = True
                self.vehicle_stop_reason = f"{kind}Req"
            return

        hlc_req = HLC_REQ_RE.search(line)
        if hlc_req:
            kind = hlc_req.group(1)
            with self.live_lock:
                if kind == "PowerDeliveryReq":
                    self.live.power_delivery_req_count += 1
                    self.live.last_power_delivery_req_ts = now
                elif kind == "WeldingDetectionReq":
                    self.live.welding_req_count += 1
                    self.live.last_welding_req_ts = now
                elif kind == "SessionStopReq":
                    self.live.session_stop_req_count += 1
                    self.live.last_session_stop_req_ts = now
            if self.current_demand_started_at is not None:
                self.vehicle_stop_requested = True
                self.vehicle_stop_reason = kind

    def _send(self, cmd: str) -> None:
        self.link.command(cmd)

    def _drain_ack_cmd(self, cmd_hex: int) -> None:
        if not self.ack_queue:
            return
        kept: Deque[Ack] = deque()
        while self.ack_queue:
            ack = self.ack_queue.popleft()
            if ack.cmd_hex != cmd_hex:
                kept.append(ack)
        self.ack_queue.extend(kept)

    def _wait_for_status(self, timeout_s: float = 2.0) -> PlcStatus:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if self.status_queue:
                return self.status_queue.popleft()
            if self.link.reader_error:
                raise RuntimeError(f"serial reader failed: {self.link.reader_error}")
            time.sleep(0.005)
        raise RuntimeError("timeout waiting for PLC status")

    def _recent_status_fallback(self, max_age_s: float) -> Optional[PlcStatus]:
        with self.live_lock:
            status = self.live.last_status
        if status is None or self.last_status_rx <= 0.0:
            return None
        if (time.time() - self.last_status_rx) > max_age_s:
            return None
        return status

    def _wait_for_ack(self, cmd_hex: int, timeout_s: float = 5.0) -> Ack:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            while self.ack_queue:
                ack = self.ack_queue.popleft()
                if ack.cmd_hex == cmd_hex:
                    return ack
            if self.link.reader_error:
                raise RuntimeError(f"serial reader failed: {self.link.reader_error}")
            time.sleep(0.005)
        raise RuntimeError(f"timeout waiting for ack 0x{cmd_hex:02X}")

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
        if require_ack:
            self._drain_ack_cmd(ACK_CMD_RELAY)
        self._send(f"CTRL RELAY {mask} {state_mask} {max(0, hold_ms)}")
        if require_ack:
            ack = self._wait_for_ack(ACK_CMD_RELAY)
            if ack.status != ACK_OK:
                raise RuntimeError(
                    f"Relay{self.args.power_relay} rejected status={ack.status} "
                    f"detail0={ack.detail0} detail1={ack.detail1}"
                )

    def _send_slac_start(self, now: float) -> None:
        self._drain_ack_cmd(ACK_CMD_SLAC)
        self._send(f"CTRL SLAC start {self.args.arm_ms}")
        ack = self._wait_for_ack(ACK_CMD_SLAC)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL SLAC start rejected status={ack.status}")
        self.last_slac_start = now
        self.remote_start_sent = True

    def _bootstrap_plc(self) -> PlcStatus:
        self._send("CTRL STATUS")
        status = self._wait_for_status()
        if status.mode_id != 1 or status.controller_id != self.args.controller_id:
            self._send(f"CTRL MODE 1 {self.args.plc_id} {self.args.controller_id}")
            time.sleep(0.3)
        self._drain_ack_cmd(ACK_CMD_HEARTBEAT)
        self._send(f"CTRL HB {self.args.heartbeat_timeout_ms}")
        ack = self._wait_for_ack(ACK_CMD_HEARTBEAT)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL HB rejected status={ack.status}")
        self._drain_ack_cmd(ACK_CMD_STOP)
        self._send("CTRL STOP clear 3000")
        ack = self._wait_for_ack(ACK_CMD_STOP)
        if ack.status != ACK_OK and ack.status != 4:
            raise RuntimeError(f"CTRL STOP clear rejected status={ack.status}")
        self._drain_ack_cmd(ACK_CMD_AUTH)
        self._send(f"CTRL AUTH deny {self.args.auth_ttl_ms}")
        ack = self._wait_for_ack(ACK_CMD_AUTH)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL AUTH deny rejected status={ack.status}")
        self._drain_ack_cmd(ACK_CMD_SLAC)
        self._send("CTRL SLAC disarm 3000")
        ack = self._wait_for_ack(ACK_CMD_SLAC)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL SLAC disarm rejected status={ack.status}")
        self._drain_ack_cmd(ACK_CMD_FEEDBACK)
        self._send("CTRL FEEDBACK 1 0 0 0 0 0 0")
        ack = self._wait_for_ack(ACK_CMD_FEEDBACK)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL FEEDBACK reset rejected status={ack.status}")
        for relay_idx in (1, 2, 3):
            mask = 1 << (relay_idx - 1)
            self._send(f"CTRL RELAY {mask} 0 0")
            ack = self._wait_for_ack(ACK_CMD_RELAY)
            if ack.status != ACK_OK:
                raise RuntimeError(f"CTRL RELAY open for relay{relay_idx} rejected status={ack.status}")
        status = self._query_status()
        if status.mode_id != 1:
            raise RuntimeError("PLC did not enter mode=1 external_controller")
        if status.can_stack != 0 or status.module_mgr != 0:
            raise RuntimeError(
                f"external-controller routing mismatch: can_stack={status.can_stack} module_mgr={status.module_mgr}"
            )
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
            loop_started = time.time()
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
                control_refresh_s = self.args.control_interval_s
                if module_is_off(self.home_module):
                    control_refresh_s = min(control_refresh_s, MODULE_OFF_RETRY_INTERVAL_S)
                if plan.enabled and (
                    force_apply or last_signature != signature or (loop_started - last_set_refresh) >= control_refresh_s
                ):
                    self.can.set_output(self.home_module, safe_v, safe_i)
                    plan_active = True
                    last_signature = signature
                    last_set_refresh = loop_started
                    if not module_online(self.home_module):
                        force_refresh = True
                elif (not plan.enabled) and plan_active:
                    self.can.stop_output(self.home_module)
                    plan_active = False
                    last_signature = None
                    last_set_refresh = loop_started
                    force_refresh = True

                should_poll = (
                    plan.enabled
                    or plan_active
                    or self.desired_relay
                    or self._relay_is_closed()
                    or self.phase in ("wait_for_session", "stopping")
                )
                if should_poll:
                    full_refresh_due = force_refresh or (loop_started - last_full_refresh) >= MODULE_WORKER_FULL_REFRESH_S
                    if full_refresh_due:
                        self.can.refresh(self.home_module, response_window_s=self.args.telemetry_response_s)
                        last_full_refresh = time.time()
                    else:
                        self.can.poll_telemetry(self.home_module, response_window_s=self.args.telemetry_response_s)
                    with self.module_lock:
                        self._snapshot_module_locked()
                elif force_refresh:
                    self.can.refresh(self.home_module, response_window_s=self.args.telemetry_response_s)
                    last_full_refresh = time.time()
                    with self.module_lock:
                        self._snapshot_module_locked()
            except Exception as exc:
                self.module_worker_error = str(exc)
                return

            sleep_s = self._module_worker_interval_s() - (time.time() - loop_started)
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
            return self.live.target_v if self.live.target_v > 0.0 else 0.0

    def _requested_current(self) -> float:
        with self.live_lock:
            return self.live.target_i if self.live.target_i > 0.0 else 0.0

    def _aggregate_feedback_telemetry(self) -> Optional[tuple[float, float]]:
        snapshot = self._get_module_snapshot()
        if not snapshot.online and self.home_module is None:
            return None
        return (snapshot.voltage_v, snapshot.current_a)

    def _effective_stage_current(self, stage: str, requested_i: float) -> float:
        if stage == "precharge":
            if requested_i > 0.05:
                return requested_i
            return self.args.precharge_current_a if self._target_voltage() > 10.0 else 0.0
        if stage == "current_demand":
            return requested_i if requested_i > 0.05 else 0.0
        if requested_i > 0.05:
            return requested_i
        return self.args.voltage_hold_current_a if self._target_voltage() > 10.0 else 0.0

    def _precharge_feedback_ready(self, target_v: float, present_v: float, present_i: float) -> bool:
        if not self._relay_is_closed():
            return False
        if target_v <= 1.0:
            return present_v <= PRECHARGE_VOLTAGE_TOLERANCE_V
        tolerance_v = max(PRECHARGE_VOLTAGE_TOLERANCE_V, target_v * 0.05)
        if abs(present_v - target_v) <= tolerance_v or present_v >= (target_v - tolerance_v):
            return True
        return present_v >= PRECHARGE_ACCEPT_MIN_V or present_i >= PRECHARGE_ACCEPT_MIN_CURRENT_A

    def _delivery_feedback_ready(self, present_v: float, present_i: float) -> bool:
        return self._relay_is_closed() and (
            present_v >= DELIVERY_ACTIVE_MIN_V or present_i >= DELIVERY_ACTIVE_MIN_CURRENT_A
        )

    def _update_plan_from_stage(self) -> tuple[str, float, float]:
        with self.live_lock:
            stage = self.live.bms_stage_name
        target_v = self._target_voltage()
        requested_i = self._requested_current()
        if stage == "precharge":
            self.current_plan = ControlPlan(True, target_v, self._effective_stage_current(stage, requested_i))
            self.desired_relay = True
        elif stage in ("power_delivery", "current_demand"):
            self.current_plan = ControlPlan(True, target_v, self._effective_stage_current(stage, requested_i))
            self.desired_relay = True
        else:
            self.current_plan = ControlPlan(False, 0.0, 0.0)
            self.desired_relay = False
        return stage, target_v, requested_i

    def _feedback_request_marker(self, stage: str) -> int:
        with self.live_lock:
            if stage == "precharge":
                return self.live.precharge_req_count
            if stage == "power_delivery":
                return self.live.power_delivery_req_count
            if stage == "current_demand":
                return self.live.current_demand_req_count
        return 0

    def _feedback_due(self, now: float, stage: str, force: bool) -> tuple[bool, int]:
        marker = self._feedback_request_marker(stage)
        if force:
            return True, marker
        if stage == "precharge" and marker > self.last_feedback_precharge_req_count:
            return True, marker
        if stage == "power_delivery" and marker > self.last_feedback_power_delivery_req_count:
            return True, marker
        if stage == "current_demand" and marker > self.last_feedback_current_demand_req_count:
            return True, marker
        if stage in ("precharge", "power_delivery", "current_demand"):
            keepalive_s = max(0.08, self.args.feedback_interval_s)
            return (now - self.last_feedback_cmd) >= keepalive_s, marker
        return (now - self.last_feedback_cmd) >= max(1.0, self.args.feedback_interval_s * 10.0), marker

    def _record_feedback_marker(self, stage: str, marker: int) -> None:
        if stage == "precharge":
            self.last_feedback_precharge_req_count = marker
        elif stage == "power_delivery":
            self.last_feedback_power_delivery_req_count = marker
        elif stage == "current_demand":
            self.last_feedback_current_demand_req_count = marker

    def _tick_contract(self, now: float) -> None:
        ttl_s = max(2.0, self.args.auth_ttl_ms / 1000.0)
        auth_refresh_s = max(2.0, min(ttl_s * 0.5, ttl_s - 2.0))
        if (now - self.last_hb) * 1000.0 >= self.args.heartbeat_ms:
            self._drain_ack_cmd(ACK_CMD_HEARTBEAT)
            self._send(f"CTRL HB {self.args.heartbeat_timeout_ms}")
            ack = self._wait_for_ack(ACK_CMD_HEARTBEAT)
            if ack.status != ACK_OK:
                raise RuntimeError(f"CTRL HB rejected status={ack.status}")
            self.last_hb = now

        if (now - self.last_auth) >= auth_refresh_s:
            self._drain_ack_cmd(ACK_CMD_AUTH)
            self._send(f"CTRL AUTH grant {self.args.auth_ttl_ms}")
            ack = self._wait_for_ack(ACK_CMD_AUTH)
            if ack.status != ACK_OK:
                raise RuntimeError(f"CTRL AUTH grant rejected status={ack.status}")
            self.last_auth = now

        with self.live_lock:
            cp_phase = self.live.cp_phase
            b1_since_ts = self.live.b1_since_ts
            slac_session = self.live.slac_session
            slac_matched = self.live.slac_matched
            slac_fsm = self.live.slac_fsm
            hlc_active = self.live.hlc_active

        waiting_for_slac = (not slac_matched) and (not hlc_active)
        at_b1 = cp_phase == "B1"
        in_digital_comm_window = cp_phase in ("B2", "C", "D")
        if (
            self.remote_start_sent
            and waiting_for_slac
            and (now - self.last_slac_start) >= 2.0
            and ((not slac_session) or slac_fsm == 1)
        ):
            print(
                f"[CTRL] re-arm remote start cp={cp_phase} slac_session={int(slac_session)} "
                f"matched={int(slac_matched)} fsm={slac_fsm}",
                flush=True,
            )
            self.remote_start_sent = False
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
            if self.last_relay_command is not True or not self._relay_is_closed() or (now - self.last_relay_refresh) >= relay_refresh_s:
                self._set_relay(True, self.args.relay_hold_ms, require_ack=False)
                self.last_relay_refresh = now
                self.last_relay_command = True
        else:
            if self.last_relay_command is not False or not self._relay_is_open():
                self._set_relay(False, 0, require_ack=False)
                self.last_relay_refresh = now
                self.last_relay_command = False

    def _tick_feedback(self, now: float, stage: str, target_v: float, force: bool = False) -> bool:
        should_send, marker = self._feedback_due(now, stage, force)
        if not should_send:
            return self.last_feedback_ready if stage == self.last_feedback_stage else False
        telemetry = self._aggregate_feedback_telemetry()
        if telemetry is None:
            self._drain_ack_cmd(ACK_CMD_FEEDBACK)
            self._send("CTRL FEEDBACK 0 0 0 0 0 0 0")
            ack = self._wait_for_ack(ACK_CMD_FEEDBACK)
            if ack.status != ACK_OK:
                raise RuntimeError(f"CTRL FEEDBACK invalid rejected status={ack.status}")
            self.last_feedback_cmd = now
            self._record_feedback_marker(stage, marker)
            self.last_feedback_ready = False
            self.last_feedback_stage = stage
            return False

        present_v, present_i = telemetry
        ready = False
        if stage == "precharge":
            ready = self._precharge_feedback_ready(target_v, present_v, present_i)
            self._send(
                f"CTRL FEEDBACK 1 {1 if ready else 0} {present_v:.1f} {present_i:.1f} 0 0 0"
            )
        elif stage in ("power_delivery", "current_demand"):
            ready = self._delivery_feedback_ready(present_v, present_i)
            self._send(
                f"CTRL FEEDBACK 1 {1 if ready else 0} {present_v:.1f} {present_i:.1f} "
                f"0 0 0"
            )
        else:
            self._drain_ack_cmd(ACK_CMD_FEEDBACK)
            self._send("CTRL FEEDBACK 1 0 0 0 0 0 0")
        ack = self._wait_for_ack(ACK_CMD_FEEDBACK)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL FEEDBACK rejected status={ack.status}")
        self.last_feedback_cmd = now
        self._record_feedback_marker(stage, marker)
        self.last_feedback_ready = ready
        self.last_feedback_stage = stage
        return ready

    def _plan_marker(self, stage: str) -> int:
        return self._feedback_request_marker(stage)

    def _tick_cleanup_contract(self, now: float) -> None:
        if (now - self.last_hb) * 1000.0 >= self.args.heartbeat_ms:
            self._drain_ack_cmd(ACK_CMD_HEARTBEAT)
            self._send(f"CTRL HB {self.args.heartbeat_timeout_ms}")
            try:
                ack = self._wait_for_ack(ACK_CMD_HEARTBEAT, timeout_s=2.0)
                if ack.status == ACK_OK:
                    self.last_hb = now
                else:
                    self._note_once(f"cleanup heartbeat rejected status={ack.status}")
            except Exception as exc:
                self._note_once(f"cleanup heartbeat degraded: {exc}")

        if (now - self.last_feedback_cmd) >= self.args.feedback_interval_s:
            self._drain_ack_cmd(ACK_CMD_FEEDBACK)
            self._send("CTRL FEEDBACK 1 0 0 0 0 0 0 1")
            try:
                ack = self._wait_for_ack(ACK_CMD_FEEDBACK, timeout_s=2.0)
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
        display_target_v = self.current_plan.voltage_v if self.current_plan.enabled else 0.0
        display_target_i = self.current_plan.current_a if self.current_plan.enabled else 0.0
        print(
            f"[PROGRESS] phase={self.phase} stage={stage or '-'} relay1={int(self._relay_is_closed())} "
            f"targetV={display_target_v:.1f} targetI={display_target_i:.1f} "
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

    def _cleanup(self) -> None:
        self._set_phase("stopping")
        try:
            self._drain_ack_cmd(ACK_CMD_STOP)
            self._send(f"CTRL STOP soft {int(self.args.stop_timeout_s * 1000.0)}")
            ack = self._wait_for_ack(ACK_CMD_STOP, timeout_s=3.0)
            if ack.status not in (ACK_OK, 4):
                self.notes.append(f"CTRL STOP soft rejected status={ack.status}; forcing hard stop")
                raise RuntimeError("soft stop rejected")
        except Exception:
            self._drain_ack_cmd(ACK_CMD_STOP)
            self._send("CTRL STOP hard 3000")
            try:
                self._wait_for_ack(ACK_CMD_STOP, timeout_s=3.0)
            except Exception:
                self.notes.append("hard stop ack missing during cleanup")

        self.current_plan = ControlPlan(False, 0.0, 0.0)
        self.desired_relay = False
        self._publish_module_plan(force_refresh=True)

        stop_deadline = time.time() + self.args.stop_timeout_s
        self.last_feedback_cmd = 0.0
        while time.time() < stop_deadline:
            now = time.time()
            self._check_module_worker()
            self._tick_relay(now)
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

        self._drain_ack_cmd(ACK_CMD_AUTH)
        self._send(f"CTRL AUTH deny {self.args.auth_ttl_ms}")
        try:
            self._wait_for_ack(ACK_CMD_AUTH, timeout_s=2.0)
        except Exception:
            pass
        self._drain_ack_cmd(ACK_CMD_SLAC)
        self._send("CTRL SLAC disarm 3000")
        try:
            self._wait_for_ack(ACK_CMD_SLAC, timeout_s=2.0)
        except Exception:
            pass
        self._drain_ack_cmd(ACK_CMD_FEEDBACK)
        self._send("CTRL FEEDBACK 1 0 0 0 0 0 0")
        try:
            self._wait_for_ack(ACK_CMD_FEEDBACK, timeout_s=2.0)
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
                "relay1": self.live.relay_states.get(1),
            },
            "module": {
                "address": self.home_module.address if self.home_module else None,
                "group": self.home_module.group if self.home_module else None,
                "status": snapshot.status_text,
                "voltage_v": snapshot.voltage_v,
                "current_a": snapshot.current_a,
                "current_limit_point": snapshot.current_limit_point,
                "input_power_w": snapshot.input_power_w,
                "input_mode": snapshot.input_mode,
                "rated_output_current_reg_a": snapshot.rated_output_current_reg_a,
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
            if self.args.module_soft_reset:
                self.can.soft_reset(self.home_module)
                self.can.refresh(self.home_module, response_window_s=0.5)
            with self.module_lock:
                self._snapshot_module_locked()
            print(
                f"[MODULE] addr={self.home_module.address} grp={self.home_module.group} "
                f"{self.module_snapshot.status_text}",
                flush=True,
            )
            self._start_module_worker()

            self._set_phase("wait_for_session")
            start_deadline = time.time() + self.args.session_start_timeout_s
            while time.time() < start_deadline:
                if self.vehicle_stop_requested:
                    raise RuntimeError(f"vehicle requested stop before CurrentDemand: {self.vehicle_stop_reason}")

                self._check_module_worker()
                stage, target_v, _requested_i = self._update_plan_from_stage()
                stage_changed = stage != self.last_stage_name
                plan_marker = self._plan_marker(stage)
                force_apply = stage_changed or (
                    stage in ("precharge", "power_delivery", "current_demand") and plan_marker != self.last_plan_marker
                )
                self.last_stage_name = stage
                self.last_plan_marker = plan_marker if stage in ("precharge", "power_delivery", "current_demand") else -1
                self._publish_module_plan(force_refresh=stage_changed, force_apply=force_apply)
                now = time.time()
                self._tick_contract(now)
                self._tick_relay(now)
                ready = self._tick_feedback(now, stage, target_v, force=stage_changed)
                self._tick_progress(now, stage, ready)

                if self._charge_success_reached(stage, ready):
                    result = "pass"
                    print(
                        f"[PASS] charge proven with module 1 only "
                        f"Vpk={self.charge_proof_peak_v:.1f} Ipk={self.charge_proof_peak_i:.1f} "
                        f"Ppk={self.charge_proof_peak_power_w:.0f}",
                        flush=True,
                    )
                    break
                time.sleep(0.02)

            if result != "pass":
                self.failures.append("session never reached proven CurrentDemand charging")
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
    parser.add_argument("--home-addr", type=int, default=1)
    parser.add_argument("--home-group", type=int, default=1)
    parser.add_argument("--can-iface", default="can0")
    parser.add_argument("--discovery-timeout-s", type=float, default=1.5)
    parser.add_argument("--esp-reset-before-start", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--esp-reset-pulse-s", type=float, default=DEFAULT_ESP_RESET_PULSE_S)
    parser.add_argument("--esp-reset-wait-s", type=float, default=DEFAULT_ESP_RESET_WAIT_S)
    parser.add_argument("--module-soft-reset", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--precharge-current-a", type=float, default=2.0)
    parser.add_argument("--voltage-hold-current-a", type=float, default=2.0)
    parser.add_argument("--current-demand-hold-s", type=float, default=1.0)
    parser.add_argument("--session-start-timeout-s", type=float, default=240.0)
    parser.add_argument("--b1-start-delay-s", type=float, default=0.6)
    parser.add_argument("--control-interval-s", type=float, default=DEFAULT_CONTROL_INTERVAL_S)
    parser.add_argument("--feedback-interval-s", type=float, default=0.05)
    parser.add_argument("--telemetry-interval-s", type=float, default=0.05)
    parser.add_argument("--telemetry-response-s", type=float, default=0.01)
    parser.add_argument("--status-interval-s", type=float, default=2.0)
    parser.add_argument("--heartbeat-ms", type=int, default=10000)
    parser.add_argument("--heartbeat-timeout-ms", type=int, default=3000)
    parser.add_argument("--auth-ttl-ms", type=int, default=30000)
    parser.add_argument("--arm-ms", type=int, default=45000)
    parser.add_argument("--relay-hold-ms", type=int, default=30000)
    parser.add_argument("--relay-refresh-s", type=float, default=10.0)
    parser.add_argument("--charge-success-min-voltage-v", type=float, default=100.0)
    parser.add_argument("--charge-success-min-current-a", type=float, default=10.0)
    parser.add_argument("--charge-success-min-power-w", type=float, default=1000.0)
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
