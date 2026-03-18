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
ACK_CMD_RELAY = 0x17
ACK_CMD_STOP = 0x18
ACK_CMD_HEARTBEAT = 0x10
ACK_CMD_AUTH = 0x11
ACK_CMD_SLAC = 0x12
ACK_CMD_FEEDBACK = 0x1A
DEFAULT_ESP_RESET_PULSE_S = 0.2
DEFAULT_ESP_RESET_WAIT_S = 14.0


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


def module_current_value(module: Optional[mxr.ModuleInfo]) -> float:
    if not module or module.current_a is None or not math.isfinite(module.current_a):
        return 0.0
    return max(0.0, module.current_a)


def module_voltage_value(module: Optional[mxr.ModuleInfo]) -> float:
    if not module or module.voltage_v is None or not math.isfinite(module.voltage_v):
        return 0.0
    return max(0.0, module.voltage_v)


MXR_REG_SET_OUTPUT_VOLTAGE_UPPER_LIMIT = 0x0023
MXR_POWER_LIMIT_RATIO_MAX = 1.0


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
            wait_s = 0.04 - (time.time() - self.last_write_ts)
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
        module_rated_power_kw: float,
        module_max_voltage_v: float,
        module_voltage_headroom_v: float,
        module_max_current_a: float,
    ) -> None:
        self.iface = iface
        self.log_file = log_file
        self.abs_current_scale = abs_current_scale
        self.module_rated_power_kw = max(0.0, module_rated_power_kw)
        self.module_max_voltage_v = max(0.0, module_max_voltage_v)
        self.module_voltage_headroom_v = max(0.0, module_voltage_headroom_v)
        self.module_max_current_a = max(0.0, module_max_current_a)
        self.sock = None
        self.log_fp = None
        self.started: set[tuple[int, int]] = set()
        self.last_safety_signature: dict[tuple[int, int], tuple[int, int]] = {}
        self.last_on_attempt_at: dict[tuple[int, int], float] = {}
        self.last_input_mode: dict[tuple[int, int], int] = {}

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

    def soft_reset(self, module: mxr.ModuleInfo, input_mode: Optional[int] = None) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        desired_mode = input_mode if input_mode in {1, 2, 3} else module.input_mode
        if desired_mode not in {1, 2, 3}:
            desired_mode = 3
        key = (module.address, module.group)
        self.last_input_mode[key] = desired_mode
        self.started.discard(key)
        mxr.send_set_int(self.sock, module.address, module.group, mxr.REG_SET_CURRENT_ABS, 0)
        time.sleep(0.02)
        self._log(f"RESET addr={module.address} grp={module.group} input_mode={desired_mode}")
        mxr.soft_reset_defaults(self.sock, module, input_mode=desired_mode)
        self.last_safety_signature.pop(key, None)
        self.last_on_attempt_at.pop(key, None)
        self.refresh(module, response_window_s=0.4)

    def safe_output_request(self, module: mxr.ModuleInfo, voltage_v: float, current_a: float) -> tuple[float, float]:
        safe_voltage_v = max(0.0, voltage_v)
        if self.module_max_voltage_v > 0.0:
            safe_voltage_v = min(safe_voltage_v, self.module_max_voltage_v)

        safe_current_a = max(0.0, current_a)
        if self.module_rated_power_kw > 0.0 and safe_voltage_v > 10.0:
            safe_current_a = min(safe_current_a, (self.module_rated_power_kw * 1000.0) / safe_voltage_v)
        if self.module_max_current_a > 0.0:
            safe_current_a = min(safe_current_a, self.module_max_current_a)

        return safe_voltage_v, safe_current_a

    def _apply_safety_limits(self, module: mxr.ModuleInfo, requested_voltage_v: float, requested_current_a: float) -> tuple[float, float]:
        if not self.sock:
            raise RuntimeError("CAN socket not open")

        safe_voltage_v, safe_current_a = self.safe_output_request(module, requested_voltage_v, requested_current_a)
        voltage_upper_limit_v = safe_voltage_v + self.module_voltage_headroom_v
        if self.module_max_voltage_v > 0.0:
            voltage_upper_limit_v = min(voltage_upper_limit_v, self.module_max_voltage_v)
        voltage_upper_limit_v = max(safe_voltage_v, voltage_upper_limit_v)

        current_limit_ratio_sig = -1
        rated_current_a = module.rated_current_a if module.rated_current_a and module.rated_current_a > 0.0 else None
        if rated_current_a is not None:
            current_limit_ratio = max(0.0, min(MXR_POWER_LIMIT_RATIO_MAX, safe_current_a / rated_current_a))
            current_limit_ratio_sig = int(round(current_limit_ratio * 1000.0))
        else:
            current_limit_ratio = None

        key = (module.address, module.group)
        safety_signature = (
            int(round(voltage_upper_limit_v * 10.0)),
            current_limit_ratio_sig,
        )
        if self.last_safety_signature.get(key) != safety_signature:
            if safe_voltage_v + 0.05 < requested_voltage_v or safe_current_a + 0.05 < requested_current_a:
                self._log(
                    f"CLAMP addr={module.address} grp={module.group} "
                    f"voltage_v={requested_voltage_v:.1f}->{safe_voltage_v:.1f} "
                    f"current_a={requested_current_a:.2f}->{safe_current_a:.2f}"
                )
            self._log(
                f"SAFE addr={module.address} grp={module.group} v_cmd={safe_voltage_v:.1f} v_upper={voltage_upper_limit_v:.1f} "
                f"i_cmd={safe_current_a:.2f} "
                f"i_limit_ratio={(f'{current_limit_ratio:.3f}' if current_limit_ratio is not None else 'n/a')}"
            )
            mxr.send_set_float(
                self.sock,
                module.address,
                module.group,
                MXR_REG_SET_OUTPUT_VOLTAGE_UPPER_LIMIT,
                voltage_upper_limit_v,
            )
            time.sleep(0.005)
            if current_limit_ratio is not None:
                mxr.send_set_float(self.sock, module.address, module.group, mxr.REG_SET_CURRENT_LIMIT_RATIO, current_limit_ratio)
                time.sleep(0.005)
            self.last_safety_signature[key] = safety_signature

        return safe_voltage_v, safe_current_a

    def set_output(self, module: mxr.ModuleInfo, voltage_v: float, current_a: float) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        safe_voltage_v, safe_current_a = self._apply_safety_limits(module, voltage_v, current_a)
        current_raw = int(round(max(0.0, safe_current_a) * self.abs_current_scale))
        key = (module.address, module.group)
        now = time.monotonic()
        if key not in self.started:
            self._log(
                f"START addr={module.address} grp={module.group} voltage_v={safe_voltage_v:.1f} current_a={safe_current_a:.2f} raw=0x{current_raw:08X}"
            )
            time.sleep(0.01)
            mxr.send_set_int(self.sock, module.address, module.group, mxr.REG_ONOFF, 0x00000000)
            time.sleep(0.01)
            mxr.send_set_float(self.sock, module.address, module.group, mxr.REG_SET_VOLTAGE, safe_voltage_v)
            time.sleep(0.01)
            mxr.send_set_int(self.sock, module.address, module.group, mxr.REG_SET_CURRENT_ABS, current_raw)
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
            mxr.send_set_int(self.sock, module.address, module.group, mxr.REG_ONOFF, 0x00000000)
            time.sleep(0.01)
            self.last_on_attempt_at[key] = now
        mxr.send_set_float(self.sock, module.address, module.group, mxr.REG_SET_VOLTAGE, safe_voltage_v)
        time.sleep(0.005)
        mxr.send_set_int(self.sock, module.address, module.group, mxr.REG_SET_CURRENT_ABS, current_raw)

    def stop_output(self, module: mxr.ModuleInfo) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        self._log(f"STOP addr={module.address} grp={module.group}")
        mxr.send_set_int(self.sock, module.address, module.group, mxr.REG_SET_CURRENT_ABS, 0)
        time.sleep(0.01)
        mxr.module_off(self.sock, module)
        self.started.discard((module.address, module.group))
        self.last_safety_signature.pop((module.address, module.group), None)
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
    errors: list[str] = field(default_factory=list)


@dataclass
class ControlPlan:
    enabled: bool = False
    voltage_v: float = 0.0
    current_a: float = 0.0


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
            args.module_rated_power_kw,
            args.module_max_voltage_v,
            args.module_voltage_headroom_v,
            args.module_max_current_a,
        )

        self.home_module: Optional[mxr.ModuleInfo] = None
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
        self.last_status_poll = 0.0
        self.last_telemetry_poll = 0.0
        self.last_progress = 0.0
        self.last_hb = 0.0
        self.last_auth = 0.0
        self.last_slac_start = 0.0
        self.last_status_rx = 0.0
        self.remote_start_sent = False

        self.current_demand_started_at: Optional[float] = None
        self.current_demand_ready_since: Optional[float] = None
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

    def _wait_for_status(self, timeout_s: float = 2.0) -> PlcStatus:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if self.status_queue:
                return self.status_queue.popleft()
            if self.link.reader_error:
                raise RuntimeError(f"serial reader failed: {self.link.reader_error}")
            time.sleep(0.02)
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
            time.sleep(0.02)
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
        self._send(f"CTRL RELAY {mask} {state_mask} {max(0, hold_ms)}")
        if require_ack:
            ack = self._wait_for_ack(ACK_CMD_RELAY)
            if ack.status != ACK_OK:
                raise RuntimeError(
                    f"Relay{self.args.power_relay} rejected status={ack.status} "
                    f"detail0={ack.detail0} detail1={ack.detail1}"
                )

    def _send_slac_start(self, now: float) -> None:
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
        self._send(f"CTRL HB {self.args.heartbeat_timeout_ms}")
        ack = self._wait_for_ack(ACK_CMD_HEARTBEAT)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL HB rejected status={ack.status}")
        self._send("CTRL STOP clear 3000")
        ack = self._wait_for_ack(ACK_CMD_STOP)
        if ack.status != ACK_OK and ack.status != 4:
            raise RuntimeError(f"CTRL STOP clear rejected status={ack.status}")
        self._send(f"CTRL AUTH deny {self.args.auth_ttl_ms}")
        ack = self._wait_for_ack(ACK_CMD_AUTH)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL AUTH deny rejected status={ack.status}")
        self._send("CTRL SLAC disarm 3000")
        ack = self._wait_for_ack(ACK_CMD_SLAC)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL SLAC disarm rejected status={ack.status}")
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

    def _target_voltage(self) -> float:
        with self.live_lock:
            if self.live.target_v > 0.0:
                return self.live.target_v
            if self.live.latched_target_v > 0.0:
                return self.live.latched_target_v
        return self.args.target_voltage_v

    def _requested_current(self) -> float:
        with self.live_lock:
            if self.live.target_i > 0.0:
                return self.live.target_i
            if self.live.latched_target_i > 0.0:
                return self.live.latched_target_i
        return 0.0

    def _aggregate_feedback_telemetry(self) -> Optional[tuple[float, float]]:
        if self.home_module is None:
            return None
        if not (self.desired_relay or self._relay_is_closed()):
            return 0.0, 0.0
        return (
            module_voltage_value(self.home_module),
            module_current_value(self.home_module),
        )

    def _effective_stage_current(self, stage: str, requested_i: float) -> float:
        if stage == "precharge":
            return max(self.args.precharge_current_a, requested_i)
        if requested_i > 0.05:
            return requested_i
        return self.args.voltage_hold_current_a if self._target_voltage() > 10.0 else 0.0

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

    def _tick_contract(self, now: float) -> None:
        if (now - self.last_hb) * 1000.0 >= self.args.heartbeat_ms:
            self._send(f"CTRL HB {self.args.heartbeat_timeout_ms}")
            ack = self._wait_for_ack(ACK_CMD_HEARTBEAT)
            if ack.status != ACK_OK:
                raise RuntimeError(f"CTRL HB rejected status={ack.status}")
            self.last_hb = now

        if (now - self.last_auth) >= 1.0:
            self._send(f"CTRL AUTH grant {self.args.auth_ttl_ms}")
            ack = self._wait_for_ack(ACK_CMD_AUTH)
            if ack.status != ACK_OK:
                raise RuntimeError(f"CTRL AUTH grant rejected status={ack.status}")
            self.last_auth = now

        if (now - self.last_status_poll) >= self.args.status_interval_s:
            try:
                self._query_status(
                    timeout_s=min(1.0, self.args.status_interval_s),
                    allow_stale=True,
                    stale_max_age_s=max(3.0, self.args.status_interval_s * 2.5),
                )
            except Exception as exc:
                self._note_once(f"status poll degraded during {self.phase}: {exc}")
            self.last_status_poll = now

        with self.live_lock:
            cp_phase = self.live.cp_phase
            b1_since_ts = self.live.b1_since_ts
            slac_matched = self.live.slac_matched
            hlc_active = self.live.hlc_active

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
        if self.desired_relay:
            if self.last_relay_command is not True or not self._relay_is_closed() or (now - self.last_relay_refresh) >= self.args.relay_refresh_s:
                self._set_relay(True, self.args.relay_hold_ms, require_ack=False)
                self.last_relay_refresh = now
                self.last_relay_command = True
        else:
            if self.last_relay_command is not False or not self._relay_is_open():
                self._set_relay(False, 0, require_ack=False)
                self.last_relay_refresh = now
                self.last_relay_command = False

    def _tick_module_output(self, now: float) -> None:
        if self.home_module is None:
            return
        safe_v, safe_i = self.can.safe_output_request(self.home_module, self.current_plan.voltage_v, self.current_plan.current_a)
        signature = (self.current_plan.enabled, int(round(safe_v * 10.0)), int(round(safe_i * 100.0)))
        if self.current_plan.enabled and (
            self.last_output_signature != signature or (now - self.last_set_refresh) >= self.args.control_interval_s
        ):
            self.can.set_output(self.home_module, safe_v, safe_i)
            self.plan_active = True
            self.last_output_signature = signature
            self.last_set_refresh = now
        elif (not self.current_plan.enabled) and self.plan_active:
            self.can.stop_output(self.home_module)
            self.plan_active = False
            self.last_output_signature = None

    def _tick_telemetry(self, now: float) -> None:
        if (now - self.last_telemetry_poll) < self.args.telemetry_interval_s:
            return
        should_poll = self.home_module is not None and (
            self.current_plan.enabled
            or self.plan_active
            or self.desired_relay
            or self._relay_is_closed()
            or self.phase in ("wait_for_session", "stopping")
        )
        if should_poll and self.home_module is not None:
            self.can.refresh(self.home_module, response_window_s=self.args.telemetry_response_s)
        self.last_telemetry_poll = now

    def _tick_feedback(self, now: float, stage: str, target_v: float, requested_i: float) -> bool:
        if (now - self.last_feedback_cmd) < self.args.feedback_interval_s:
            return False
        telemetry = self._aggregate_feedback_telemetry()
        if telemetry is None:
            self._send("CTRL FEEDBACK 0 0 0 0 0 0 0")
            ack = self._wait_for_ack(ACK_CMD_FEEDBACK)
            if ack.status != ACK_OK:
                raise RuntimeError(f"CTRL FEEDBACK invalid rejected status={ack.status}")
            self.last_feedback_cmd = now
            return False

        present_v, present_i = telemetry
        ready = False
        if stage == "precharge":
            relay_closed = self._relay_is_closed() or self.desired_relay
            voltage_ready = target_v > 10.0 and abs(present_v - target_v) <= self.args.feedback_voltage_tolerance_v
            current_floor = max(0.5, self.args.precharge_current_a * self.args.precharge_ready_min_current_ratio)
            current_ready = present_i >= current_floor
            if self.args.allow_zero_load_ready:
                current_ready = current_ready or present_v >= self.args.precharge_ready_min_voltage_v
            bench_ready = relay_closed and present_v >= self.args.precharge_ready_min_voltage_v and current_ready
            ready = voltage_ready or bench_ready
            self._send(
                f"CTRL FEEDBACK 1 {1 if ready else 0} {present_v:.1f} {present_i:.1f} 0 0 0"
            )
        elif stage in ("power_delivery", "current_demand"):
            relay_closed = self._relay_is_closed()
            current_ready = requested_i <= 0.05 or present_i >= self.args.current_demand_ready_min_current_a
            if self.args.allow_zero_load_ready:
                current_ready = current_ready or present_v >= self.args.current_demand_ready_min_voltage_v
            ready = (
                target_v > 10.0
                and relay_closed
                and present_v >= self.args.current_demand_ready_min_voltage_v
                and current_ready
            )
            safe_v, safe_i = self.can.safe_output_request(self.home_module, self.current_plan.voltage_v, self.current_plan.current_a) if self.home_module else (0.0, 0.0)
            current_limited = requested_i > (safe_i + 0.5)
            voltage_limited = target_v > (safe_v + 0.5)
            power_limited = False
            self._send(
                f"CTRL FEEDBACK 1 {1 if ready else 0} {present_v:.1f} {present_i:.1f} "
                f"{1 if current_limited else 0} {1 if voltage_limited else 0} {1 if power_limited else 0}"
            )
        else:
            self._send("CTRL FEEDBACK 1 0 0 0 0 0 0")
        ack = self._wait_for_ack(ACK_CMD_FEEDBACK)
        if ack.status != ACK_OK:
            raise RuntimeError(f"CTRL FEEDBACK rejected status={ack.status}")
        self.last_feedback_cmd = now
        return ready

    def _tick_cleanup_contract(self, now: float) -> None:
        if (now - self.last_hb) * 1000.0 >= self.args.heartbeat_ms:
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
        present_v = module_voltage_value(self.home_module)
        present_i = module_current_value(self.home_module)
        print(
            f"[PROGRESS] phase={self.phase} stage={stage or '-'} relay1={int(self._relay_is_closed())} "
            f"targetV={self._target_voltage():.1f} targetI={self._requested_current():.1f} "
            f"presentV={present_v:.1f} presentI={present_i:.1f} ready={int(ready)} "
            f"cp={self.live.cp_phase} slac={int(self.live.slac_matched)} hlc={int(self.live.hlc_ready)}",
            flush=True,
        )
        self.last_progress = now

    def _current_demand_ready(self, stage: str, ready: bool) -> bool:
        if stage != "current_demand" or not ready:
            self.current_demand_ready_since = None
            return False
        if self.current_demand_started_at is None:
            self.current_demand_started_at = time.time()
        if self.current_demand_ready_since is None:
            self.current_demand_ready_since = time.time()
        return (time.time() - self.current_demand_ready_since) >= self.args.current_demand_hold_s

    def _cleanup(self) -> None:
        self._set_phase("stopping")
        try:
            self._send(f"CTRL STOP soft {int(self.args.stop_timeout_s * 1000.0)}")
            ack = self._wait_for_ack(ACK_CMD_STOP, timeout_s=3.0)
            if ack.status not in (ACK_OK, 4):
                self.notes.append(f"CTRL STOP soft rejected status={ack.status}; forcing hard stop")
                raise RuntimeError("soft stop rejected")
        except Exception:
            self._send("CTRL STOP hard 3000")
            try:
                self._wait_for_ack(ACK_CMD_STOP, timeout_s=3.0)
            except Exception:
                self.notes.append("hard stop ack missing during cleanup")

        self.current_plan = ControlPlan(False, 0.0, 0.0)
        self.desired_relay = False
        if self.home_module is not None:
            try:
                self.can.stop_output(self.home_module)
            except Exception as exc:
                self.notes.append(f"module stop failed during cleanup: {exc}")

        stop_deadline = time.time() + self.args.stop_timeout_s
        self.last_feedback_cmd = 0.0
        while time.time() < stop_deadline:
            now = time.time()
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

        self._send(f"CTRL AUTH deny {self.args.auth_ttl_ms}")
        try:
            self._wait_for_ack(ACK_CMD_AUTH, timeout_s=2.0)
        except Exception:
            pass
        self._send("CTRL SLAC disarm 3000")
        try:
            self._wait_for_ack(ACK_CMD_SLAC, timeout_s=2.0)
        except Exception:
            pass
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
        payload = {
            "result": result,
            "phase": self.phase,
            "events": self.events,
            "notes": self.notes,
            "failures": self.failures,
            "current_demand_started_at": self.current_demand_started_at,
            "current_demand_ready_since": self.current_demand_ready_since,
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
                "rated_current_a": self.home_module.rated_current_a if self.home_module else None,
                "status": module_status_text(self.home_module) if self.home_module else None,
                "voltage_v": self.home_module.voltage_v if self.home_module else None,
                "current_a": self.home_module.current_a if self.home_module else None,
                "off": module_is_off(self.home_module) if self.home_module else None,
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
            print(
                f"[MODULE] addr={self.home_module.address} grp={self.home_module.group} "
                f"rated={self.home_module.rated_current_a}A {module_status_text(self.home_module)}",
                flush=True,
            )

            self._set_phase("wait_for_session")
            start_deadline = time.time() + self.args.session_start_timeout_s
            while time.time() < start_deadline:
                if self.vehicle_stop_requested:
                    raise RuntimeError(f"vehicle requested stop before CurrentDemand: {self.vehicle_stop_reason}")

                stage, target_v, requested_i = self._update_plan_from_stage()
                now = time.time()
                self._tick_contract(now)
                self._tick_relay(now)
                self._tick_module_output(now)
                self._tick_telemetry(now)
                ready = self._tick_feedback(now, stage, target_v, requested_i)
                self._tick_progress(now, stage, ready)

                if self._current_demand_ready(stage, ready):
                    result = "pass"
                    print("[PASS] sustained CurrentDemand reached with module 1 only", flush=True)
                    break
                time.sleep(0.02)

            if result != "pass":
                self.failures.append("session never reached sustained CurrentDemand")
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
    parser.add_argument("--input-mode", type=int, choices=(1, 2, 3), default=None)
    parser.add_argument("--can-iface", default="can0")
    parser.add_argument("--discovery-timeout-s", type=float, default=1.5)
    parser.add_argument("--esp-reset-before-start", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--esp-reset-pulse-s", type=float, default=DEFAULT_ESP_RESET_PULSE_S)
    parser.add_argument("--esp-reset-wait-s", type=float, default=DEFAULT_ESP_RESET_WAIT_S)
    parser.add_argument("--target-voltage-v", type=float, default=500.0)
    parser.add_argument("--precharge-current-a", type=float, default=2.0)
    parser.add_argument("--voltage-hold-current-a", type=float, default=2.0)
    parser.add_argument("--allow-zero-load-ready", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--current-demand-hold-s", type=float, default=15.0)
    parser.add_argument("--session-start-timeout-s", type=float, default=240.0)
    parser.add_argument("--b1-start-delay-s", type=float, default=0.6)
    parser.add_argument("--control-interval-s", type=float, default=0.10)
    parser.add_argument("--feedback-interval-s", type=float, default=0.10)
    parser.add_argument("--telemetry-interval-s", type=float, default=0.10)
    parser.add_argument("--telemetry-response-s", type=float, default=0.10)
    parser.add_argument("--status-interval-s", type=float, default=2.0)
    parser.add_argument("--heartbeat-ms", type=int, default=800)
    parser.add_argument("--heartbeat-timeout-ms", type=int, default=3000)
    parser.add_argument("--auth-ttl-ms", type=int, default=6000)
    parser.add_argument("--arm-ms", type=int, default=45000)
    parser.add_argument("--relay-hold-ms", type=int, default=5000)
    parser.add_argument("--relay-refresh-s", type=float, default=0.6)
    parser.add_argument("--feedback-voltage-tolerance-v", type=float, default=15.0)
    parser.add_argument("--precharge-ready-min-voltage-v", type=float, default=20.0)
    parser.add_argument("--precharge-ready-min-current-ratio", type=float, default=0.75)
    parser.add_argument("--current-demand-ready-min-voltage-v", type=float, default=25.0)
    parser.add_argument("--current-demand-ready-min-current-a", type=float, default=1.5)
    parser.add_argument("--absolute-current-scale", type=float, default=1024.0)
    parser.add_argument("--module-rated-power-kw", type=float, default=30.0)
    parser.add_argument("--module-max-voltage-v", type=float, default=1000.0)
    parser.add_argument("--module-voltage-headroom-v", type=float, default=15.0)
    parser.add_argument("--module-max-current-a", type=float, default=100.0)
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
