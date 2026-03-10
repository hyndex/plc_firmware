#!/usr/bin/env python3
"""
Mode-2 UART-router plus direct Maxwell module transition test.

Architecture under test:
  - Controller -> PLC over UART for relay control/status only.
  - Controller -> Modules over CAN directly using the Maxwell V1.50 protocol.
  - PLC1 Relay1 is the main load contactor.
  - PLC2 Relay2 is the donor bus contactor for module addr=3/group=2.

Test sequence:
  1. Put both PLCs into mode 2 and verify `can_stack=0 module_mgr=0`.
  2. Discover modules 0x01/group1 and 0x03/group2 directly on CAN.
  3. Soft-reset both modules into known-good defaults.
  4. Close PLC1 Relay1 and run the home module at 500 V for the home window.
  5. Precharge the donor module in isolation to the same voltage.
  6. Close PLC2 Relay2 only after the donor voltage is matched.
  7. Rebalance current to the shared targets and hold the shared window.
 8. Raise the home-module target first, then drain donor current to near-zero.
 9. Open PLC2 Relay2, turn donor off, and hold the final home-only window.
10. Stop both modules, open relays, and write a summary.

Default timing profile:
  - 40 s home-only
  - 40 s shared
  - 40 s post-release home-only
This keeps the standard validation loop at 2 minutes total unless overridden.

The script logs both UART and CAN-side behavior. It fails if the attached bus
voltage drops below the configured threshold after transition grace windows.
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

ACK_RE = re.compile(
    r"\[SERCTRL\] ACK cmd=0x([0-9A-Fa-f]+)\s+seq=(\d+)\s+status=(\d+)\s+detail0=(\d+)\s+detail1=(\d+)"
)
STATUS_RE = re.compile(
    r"\[SERCTRL\] STATUS mode=(\d+)\(([^)]+)\)\s+plc_id=(\d+)\s+connector_id=(\d+)\s+"
    r"controller_id=(\d+)\s+module_addr=0x([0-9A-Fa-f]+)\s+local_group=(\d+)\s+module_id=([^\s]+)\s+"
    r"can_stack=(\d)\s+module_mgr=(\d)\s+cp=([A-Z0-9]+)\s+duty=(\d+)\s+hb=(\d+)\s+auth=(\d+)\s+"
    r"allow_slac=(\d+)\s+allow_energy=(\d+)\s+armed=(\d+)\s+start=(\d+)\s+"
    r"relay1=(\d+)\s+relay2=(\d+)\s+relay3=(\d+)\s+alloc_sz=(\d+)"
)
RELAY_EVT_RE = re.compile(r"\[RELAY\] Relay(\d+) -> (OPEN|CLOSED)")


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


def module_voltage_ok(module: mxr.ModuleInfo, min_voltage_v: float) -> bool:
    if module.voltage_v is None or not math.isfinite(module.voltage_v):
        return False
    return module.voltage_v >= min_voltage_v and not module_is_off(module)


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


@dataclass
class PlcState:
    name: str
    last_status: Optional[PlcStatus] = None
    relay_states: dict[int, bool] = field(default_factory=dict)
    issues: list[str] = field(default_factory=list)


@dataclass
class PhaseMetrics:
    name: str
    samples: int = 0
    min_bus_voltage_v: float = 1.0e9
    max_bus_voltage_v: float = 0.0
    min_home_voltage_v: float = 1.0e9
    max_home_voltage_v: float = 0.0
    min_donor_voltage_v: float = 1.0e9
    max_donor_voltage_v: float = 0.0
    min_total_current_a: float = 1.0e9
    max_total_current_a: float = 0.0
    low_voltage_events: int = 0
    notes: list[str] = field(default_factory=list)

    def observe(self, bus_v: float, home_v: float, donor_v: float, total_i: float) -> None:
        self.samples += 1
        self.min_bus_voltage_v = min(self.min_bus_voltage_v, bus_v)
        self.max_bus_voltage_v = max(self.max_bus_voltage_v, bus_v)
        self.min_home_voltage_v = min(self.min_home_voltage_v, home_v)
        self.max_home_voltage_v = max(self.max_home_voltage_v, home_v)
        self.min_donor_voltage_v = min(self.min_donor_voltage_v, donor_v)
        self.max_donor_voltage_v = max(self.max_donor_voltage_v, donor_v)
        self.min_total_current_a = min(self.min_total_current_a, total_i)
        self.max_total_current_a = max(self.max_total_current_a, total_i)

    def to_json(self) -> dict[str, object]:
        payload = asdict(self)
        if self.samples == 0:
            payload["min_bus_voltage_v"] = None
            payload["min_home_voltage_v"] = None
            payload["min_donor_voltage_v"] = None
            payload["min_total_current_a"] = None
        return payload


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
        self.last_write_ts = 0.0
        self.pending_status_line = ""

    def open(self) -> None:
        ensure_parent(self.log_file)
        self.log_fp = open(self.log_file, "w", encoding="utf-8", buffering=1)
        kwargs = {
            "port": self.port,
            "baudrate": self.baud,
            "timeout": 0.05,
            "write_timeout": 0.5,
        }
        try:
            self.ser = serial.Serial(exclusive=True, **kwargs)
        except TypeError:
            self.ser = serial.Serial(**kwargs)
        self.ser.reset_input_buffer()
        self.ser.reset_output_buffer()
        self.stop_event.clear()
        self.reader_error = None
        self.reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self.reader_thread.start()

    def close(self) -> None:
        self.stop_event.set()
        if self.reader_thread and self.reader_thread.is_alive():
            self.reader_thread.join(timeout=2.0)
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
            self.log_fp.write(line + "\n")

    def command(self, cmd: str) -> None:
        if not self.ser:
            raise RuntimeError("serial port not open")
        with self.write_lock:
            wait_s = 0.04 - (time.time() - self.last_write_ts)
            if wait_s > 0:
                time.sleep(wait_s)
            payload = (cmd.strip() + "\n").encode("utf-8", errors="ignore")
            self.ser.write(payload)
            self.ser.flush()
            self.last_write_ts = time.time()
            self._log(f"{ts()} > {cmd}")

    def _reader_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                raw = self.ser.readline() if self.ser else b""
            except Exception as exc:
                self.reader_error = str(exc)
                self._log(f"{ts()} [HOST] read failed: {exc}")
                break
            if not raw:
                continue
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            self._log(f"{ts()} {line}")
            if self.pending_status_line:
                if line and not line.startswith("["):
                    combined = self.pending_status_line + line
                    self.pending_status_line = ""
                    self.parse_line(combined)
                    continue
                self.parse_line(self.pending_status_line)
                self.pending_status_line = ""
            if line.startswith("[SERCTRL] STATUS") and "stop_done=" not in line:
                self.pending_status_line = line
                continue
            self.parse_line(line)


class DirectMaxwellCan:
    def __init__(self, iface: str, log_file: str, abs_current_scale: float) -> None:
        self.iface = iface
        self.log_file = log_file
        self.abs_current_scale = abs_current_scale
        self.sock = None
        self.log_fp = None
        self.started: set[tuple[int, int]] = set()

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

    def scan_expected(self, home_addr: int, home_group: int, donor_addr: int, donor_group: int) -> tuple[mxr.ModuleInfo, mxr.ModuleInfo]:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        mods = mxr.scan_modules(
            self.sock,
            groups=sorted({home_group, donor_group}),
            addr_start=min(home_addr, donor_addr),
            addr_end=max(home_addr, donor_addr),
            send_gap_s=0.001,
            listen_s=0.4,
        )
        found = {(mod.address, mod.group): mod for mod in mods}
        home = found.get((home_addr, home_group))
        donor = found.get((donor_addr, donor_group))
        if home is None or donor is None:
            raise RuntimeError(
                f"module scan incomplete: found={sorted(found.keys())} expected={(home_addr, home_group)} and {(donor_addr, donor_group)}"
            )
        self._log(
            f"SCAN found home addr={home.address} grp={home.group}; donor addr={donor.address} grp={donor.group}"
        )
        return home, donor

    def refresh(self, module: mxr.ModuleInfo, response_window_s: float) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        mxr.refresh_module_details(self.sock, module, response_window_s=response_window_s)
        self._log(
            f"TEL addr={module.address} grp={module.group} V={module.voltage_v} I={module.current_a} "
            f"Ilim={module.current_limit_point} Pin={module.input_power_w} {module_status_text(module)}"
        )

    def soft_reset(self, module: mxr.ModuleInfo, input_mode: int = 3) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        self._log(f"RESET addr={module.address} grp={module.group} input_mode={input_mode}")
        mxr.soft_reset_defaults(self.sock, module, input_mode=input_mode)
        self.refresh(module, response_window_s=0.4)

    def set_output(self, module: mxr.ModuleInfo, voltage_v: float, current_a: float) -> None:
        if not self.sock:
            raise RuntimeError("CAN socket not open")
        current_raw = int(round(max(0.0, current_a) * self.abs_current_scale))
        key = (module.address, module.group)
        if key not in self.started:
            self._log(
                f"START addr={module.address} grp={module.group} voltage_v={voltage_v:.1f} current_a={current_a:.2f} raw=0x{current_raw:08X}"
            )
            mxr.send_set_float(self.sock, module.address, module.group, mxr.REG_SET_VOLTAGE, voltage_v)
            time.sleep(0.01)
            mxr.send_set_int(self.sock, module.address, module.group, mxr.REG_SET_CURRENT_ABS, current_raw)
            time.sleep(0.01)
            mxr.send_set_int(self.sock, module.address, module.group, mxr.REG_ONOFF, 0x00000000)
            self.started.add(key)
            return
        self._log(
            f"SET addr={module.address} grp={module.group} voltage_v={voltage_v:.1f} current_a={current_a:.2f} raw=0x{current_raw:08X}"
        )
        mxr.send_set_float(self.sock, module.address, module.group, mxr.REG_SET_VOLTAGE, voltage_v)
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


class ModuleTransitionRunner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.plc1 = PlcState("plc1")
        self.plc2 = PlcState("plc2")
        self.plc1_lock = threading.RLock()
        self.plc2_lock = threading.RLock()
        self.status_queues: dict[str, Deque[PlcStatus]] = {"plc1": deque(), "plc2": deque()}
        self.ack_queues: dict[str, Deque[Ack]] = {"plc1": deque(), "plc2": deque()}

        self.plc1_link = SerialLink(args.plc1_port, args.baud, args.plc1_log, self._parse_plc1)
        self.plc2_link = SerialLink(args.plc2_port, args.baud, args.plc2_log, self._parse_plc2)
        self.can = DirectMaxwellCan(args.can_iface, args.can_log, args.absolute_current_scale)

        self.home_module: Optional[mxr.ModuleInfo] = None
        self.donor_module: Optional[mxr.ModuleInfo] = None

        self.phase = "bootstrap"
        self.phase_started_at = time.time()
        self.phase_metrics: dict[str, PhaseMetrics] = {}
        self.failures: list[str] = []
        self.events: list[str] = []
        self.notes: list[str] = []
        self.current_plan: dict[str, tuple[bool, float, float]] = {
            "home": (False, 0.0, 0.0),
            "donor": (False, 0.0, 0.0),
        }
        self.desired_relays: dict[tuple[str, int], bool] = {
            ("plc1", 1): False,
            ("plc2", 2): False,
        }
        self.last_relay_refresh: dict[tuple[str, int], float] = {}
        self.last_status_poll = 0.0
        self.last_telemetry_poll = 0.0
        self.last_set_refresh = {"home": 0.0, "donor": 0.0}
        self.last_progress = 0.0
        self.capability_clamps: list[str] = []
        self.capability_warnings: list[str] = []
        self.status_miss_counts = {"plc1": 0, "plc2": 0}

    def _state(self, plc_name: str) -> tuple[PlcState, threading.RLock]:
        if plc_name == "plc2":
            return self.plc2, self.plc2_lock
        return self.plc1, self.plc1_lock

    def _parse_status(self, line: str) -> Optional[PlcStatus]:
        m = STATUS_RE.search(line)
        if not m:
            return None
        return PlcStatus(
            mode_id=int(m.group(1)),
            mode_name=m.group(2),
            plc_id=int(m.group(3)),
            connector_id=int(m.group(4)),
            controller_id=int(m.group(5)),
            module_addr=int(m.group(6), 16),
            local_group=int(m.group(7)),
            module_id=m.group(8),
            can_stack=int(m.group(9)),
            module_mgr=int(m.group(10)),
            cp=m.group(11),
            duty=int(m.group(12)),
            relay1=int(m.group(19)),
            relay2=int(m.group(20)),
            relay3=int(m.group(21)),
            alloc_sz=int(m.group(22)),
        )

    def _parse_line_common(self, plc_name: str, line: str) -> None:
        state, lock = self._state(plc_name)
        status = self._parse_status(line)
        if status is not None:
            with lock:
                state.last_status = status
                state.relay_states[1] = status.relay1 != 0
                state.relay_states[2] = status.relay2 != 0
                state.relay_states[3] = status.relay3 != 0
            self.status_queues[plc_name].append(status)
            return
        m_ack = ACK_RE.search(line)
        if m_ack:
            self.ack_queues[plc_name].append(
                Ack(
                    cmd_hex=int(m_ack.group(1), 16),
                    seq=int(m_ack.group(2)),
                    status=int(m_ack.group(3)),
                    detail0=int(m_ack.group(4)),
                    detail1=int(m_ack.group(5)),
                )
            )
            return
        m_relay = RELAY_EVT_RE.search(line)
        if m_relay:
            relay_idx = int(m_relay.group(1))
            closed = m_relay.group(2) == "CLOSED"
            with lock:
                state.relay_states[relay_idx] = closed

    def _parse_plc1(self, line: str) -> None:
        self._parse_line_common("plc1", line)

    def _parse_plc2(self, line: str) -> None:
        self._parse_line_common("plc2", line)

    def _send(self, plc_name: str, cmd: str) -> None:
        if plc_name == "plc2":
            self.plc2_link.command(cmd)
        else:
            self.plc1_link.command(cmd)

    def _wait_for_status(self, plc_name: str, timeout_s: float = 2.0) -> PlcStatus:
        deadline = time.time() + timeout_s
        queue = self.status_queues[plc_name]
        while time.time() < deadline:
            if queue:
                return queue.popleft()
            link = self.plc2_link if plc_name == "plc2" else self.plc1_link
            if link.reader_error:
                raise RuntimeError(f"{plc_name} serial reader failed: {link.reader_error}")
            time.sleep(0.02)
        raise RuntimeError(f"timeout waiting for {plc_name} status")

    def _wait_for_ack(self, plc_name: str, cmd_hex: int, timeout_s: float = 2.0) -> Ack:
        deadline = time.time() + timeout_s
        queue = self.ack_queues[plc_name]
        while time.time() < deadline:
            while queue:
                ack = queue.popleft()
                if ack.cmd_hex == cmd_hex:
                    return ack
            link = self.plc2_link if plc_name == "plc2" else self.plc1_link
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
                    f"{plc_name} Relay{relay_idx} command rejected status={ack.status} "
                    f"detail0={ack.detail0} detail1={ack.detail1}"
                )

    def _relay_is_closed(self, plc_name: str, relay_idx: int) -> bool:
        state, lock = self._state(plc_name)
        with lock:
            return state.relay_states.get(relay_idx) is True

    def _bootstrap_plc(self, plc_name: str, plc_id: int) -> PlcStatus:
        self._send(plc_name, "CTRL STATUS")
        status = self._wait_for_status(plc_name)
        if status.mode_id != 2 or status.controller_id != self.args.controller_id:
            self._send(plc_name, f"CTRL MODE 2 {plc_id} {self.args.controller_id}")
            time.sleep(0.3)
        self._send(plc_name, "CTRL STOP clear 3000")
        ack = self._wait_for_ack(plc_name, ACK_CMD_STOP)
        if ack.status != ACK_OK:
            raise RuntimeError(f"{plc_name} STOP clear rejected status={ack.status}")
        self._set_relay(plc_name, 1, False, 0)
        self._set_relay(plc_name, 2, False, 0)
        self._set_relay(plc_name, 3, False, 0)
        status = self._query_status(plc_name)
        if status.mode_id != 2:
            raise RuntimeError(f"{plc_name} did not enter mode 2")
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
            return requested_a
        effective = min(requested_a, rated)
        if effective + 0.05 < requested_a:
            note = f"{label} clipped from {requested_a:.1f} A to rated {effective:.1f} A"
            self.capability_clamps.append(note)
            print(f"[WARN] {note}", flush=True)
        return effective

    def _set_phase(self, name: str) -> None:
        self.phase = name
        self.phase_started_at = time.time()
        self.phase_metrics.setdefault(name, PhaseMetrics(name=name))
        self.events.append(f"{ts()} phase={name}")
        print(f"[PHASE] {name}", flush=True)

    def _attached_modules(self) -> list[mxr.ModuleInfo]:
        mods: list[mxr.ModuleInfo] = []
        if self.home_module and self._relay_is_closed("plc1", 1):
            mods.append(self.home_module)
        if self.donor_module and self._relay_is_closed("plc2", 2):
            mods.append(self.donor_module)
        return mods

    def _bus_voltage_estimate(self) -> float:
        attached = self._attached_modules()
        voltages = [m.voltage_v for m in attached if m.voltage_v is not None and math.isfinite(m.voltage_v)]
        if not voltages:
            return 0.0
        return min(voltages)

    def _bus_current_estimate(self) -> float:
        total = 0.0
        for module in self._attached_modules():
            if module.current_a is not None and math.isfinite(module.current_a):
                total += max(0.0, module.current_a)
        return total

    def _record_metrics(self) -> None:
        metrics = self.phase_metrics.setdefault(self.phase, PhaseMetrics(name=self.phase))
        home_v = self.home_module.voltage_v if self.home_module and self.home_module.voltage_v is not None else 0.0
        donor_v = self.donor_module.voltage_v if self.donor_module and self.donor_module.voltage_v is not None else 0.0
        metrics.observe(
            bus_v=self._bus_voltage_estimate(),
            home_v=home_v,
            donor_v=donor_v,
            total_i=self._bus_current_estimate(),
        )
        if time.time() - self.phase_started_at < self.args.transition_grace_s:
            return
        threshold = 0.0
        if self.phase in ("home_1", "home_2"):
            threshold = self.args.min_home_bus_voltage_v
        elif self.phase == "shared":
            threshold = self.args.min_shared_bus_voltage_v
        if threshold > 0.0 and self._attached_modules():
            bus_v = self._bus_voltage_estimate()
            if bus_v > 0.0 and bus_v < threshold:
                metrics.low_voltage_events += 1
                msg = f"{self.phase} low bus voltage {bus_v:.1f} V < {threshold:.1f} V"
                if metrics.low_voltage_events == 1:
                    self.failures.append(msg)
                metrics.notes.append(msg)

    def _tick(self) -> None:
        now = time.time()

        for plc_name, relay_idx in (("plc1", 1), ("plc2", 2)):
            desired = self.desired_relays[(plc_name, relay_idx)]
            last = self.last_relay_refresh.get((plc_name, relay_idx), 0.0)
            if desired:
                if (now - last) >= self.args.relay_refresh_s:
                    self._set_relay(plc_name, relay_idx, True, self.args.relay_hold_ms, require_ack=False)
                    self.last_relay_refresh[(plc_name, relay_idx)] = now
            else:
                if self._relay_is_closed(plc_name, relay_idx) and (now - last) >= 0.8:
                    self._set_relay(plc_name, relay_idx, False, 0, require_ack=False)
                    self.last_relay_refresh[(plc_name, relay_idx)] = now

        if self.home_module and self.current_plan["home"][0] and (now - self.last_set_refresh["home"]) >= self.args.control_interval_s:
            _, v, i = self.current_plan["home"]
            self.can.set_output(self.home_module, v, i)
            self.last_set_refresh["home"] = now
        if self.donor_module and self.current_plan["donor"][0] and (now - self.last_set_refresh["donor"]) >= self.args.control_interval_s:
            _, v, i = self.current_plan["donor"]
            self.can.set_output(self.donor_module, v, i)
            self.last_set_refresh["donor"] = now

        if (now - self.last_status_poll) >= self.args.status_interval_s:
            self._poll_status_best_effort("plc1")
            self._poll_status_best_effort("plc2")
            self.last_status_poll = now

        if (now - self.last_telemetry_poll) >= self.args.telemetry_interval_s:
            if self.home_module:
                self.can.refresh(self.home_module, response_window_s=self.args.telemetry_response_s)
            if self.donor_module:
                self.can.refresh(self.donor_module, response_window_s=self.args.telemetry_response_s)
            self._record_metrics()
            self.last_telemetry_poll = now

        if (now - self.last_progress) >= 5.0:
            home_v = self.home_module.voltage_v if self.home_module and self.home_module.voltage_v is not None else -1.0
            home_i = self.home_module.current_a if self.home_module and self.home_module.current_a is not None else -1.0
            donor_v = self.donor_module.voltage_v if self.donor_module and self.donor_module.voltage_v is not None else -1.0
            donor_i = self.donor_module.current_a if self.donor_module and self.donor_module.current_a is not None else -1.0
            print(
                f"[PROGRESS] phase={self.phase} relay1={int(self._relay_is_closed('plc1', 1))} "
                f"relay2={int(self._relay_is_closed('plc2', 2))} busV={self._bus_voltage_estimate():.1f} "
                f"busI={self._bus_current_estimate():.1f} homeV={home_v:.1f} homeI={home_i:.1f} "
                f"donorV={donor_v:.1f} donorI={donor_i:.1f}",
                flush=True,
            )
            self.last_progress = now

    def _wait_until(self, timeout_s: float, predicate, description: str) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if predicate():
                return
            self._tick()
            time.sleep(0.05)
        raise RuntimeError(description)

    def _module_matched(self, module: mxr.ModuleInfo, target_v: float, tolerance_v: float) -> bool:
        if module.voltage_v is None or not math.isfinite(module.voltage_v):
            return False
        return (abs(module.voltage_v - target_v) <= tolerance_v) and not module_is_off(module)

    def _donor_current_low(self) -> bool:
        if not self.donor_module or self.donor_module.current_a is None or not math.isfinite(self.donor_module.current_a):
            return False
        return abs(self.donor_module.current_a) <= self.args.release_donor_current_a

    def _hold_phase(self, name: str, duration_s: float) -> None:
        self._set_phase(name)
        end_at = time.time() + duration_s
        while time.time() < end_at:
            self._tick()
            time.sleep(0.05)

    def _ramp_plan(
        self,
        name: str,
        duration_s: float,
        home_start_a: float,
        home_end_a: float,
        donor_start_a: float,
        donor_end_a: float,
    ) -> None:
        self._set_phase(name)
        start = time.time()
        if duration_s <= 0.0:
            self.current_plan["home"] = (True, self.args.voltage_v, home_end_a)
            self.current_plan["donor"] = (donor_end_a > 0.0, self.args.voltage_v, donor_end_a)
            return
        while True:
            now = time.time()
            frac = min(1.0, max(0.0, (now - start) / duration_s))
            home_i = home_start_a + ((home_end_a - home_start_a) * frac)
            donor_i = donor_start_a + ((donor_end_a - donor_start_a) * frac)
            self.current_plan["home"] = (True, self.args.voltage_v, max(0.0, home_i))
            self.current_plan["donor"] = (max(0.0, donor_i) > 0.0, self.args.voltage_v, max(0.0, donor_i))
            self._tick()
            if frac >= 1.0:
                break
            time.sleep(0.05)

    def _shutdown_modules(self) -> None:
        if self.home_module:
            for _ in range(2):
                self.can.stop_output(self.home_module)
                time.sleep(0.05)
        if self.donor_module:
            for _ in range(2):
                self.can.stop_output(self.donor_module)
                time.sleep(0.05)
        self.current_plan["home"] = (False, 0.0, 0.0)
        self.current_plan["donor"] = (False, 0.0, 0.0)

    def _open_relays(self) -> None:
        self.desired_relays[("plc1", 1)] = False
        self.desired_relays[("plc2", 2)] = False
        self._set_relay("plc1", 1, False, 0)
        self._set_relay("plc2", 2, False, 0)

    def _write_summary(self, result: str) -> None:
        payload = {
            "result": result,
            "phase": self.phase,
            "events": self.events,
            "notes": self.notes,
            "capability_clamps": self.capability_clamps,
            "capability_warnings": self.capability_warnings,
            "failures": self.failures,
            "home_module": {
                "address": self.home_module.address if self.home_module else None,
                "group": self.home_module.group if self.home_module else None,
                "rated_current_a": self.home_module.rated_current_a if self.home_module else None,
                "status": module_status_text(self.home_module) if self.home_module else None,
                "voltage_v": self.home_module.voltage_v if self.home_module else None,
                "current_a": self.home_module.current_a if self.home_module else None,
            },
            "donor_module": {
                "address": self.donor_module.address if self.donor_module else None,
                "group": self.donor_module.group if self.donor_module else None,
                "rated_current_a": self.donor_module.rated_current_a if self.donor_module else None,
                "status": module_status_text(self.donor_module) if self.donor_module else None,
                "voltage_v": self.donor_module.voltage_v if self.donor_module else None,
                "current_a": self.donor_module.current_a if self.donor_module else None,
            },
            "phase_metrics": {name: metrics.to_json() for name, metrics in self.phase_metrics.items()},
        }
        ensure_parent(self.args.summary_file)
        with open(self.args.summary_file, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, indent=2)

    def run(self) -> int:
        result = "fail"
        try:
            self.plc1_link.open()
            self.plc2_link.open()
            self.can.open()
            print(f"PLC1_LOG={self.args.plc1_log}", flush=True)
            print(f"PLC2_LOG={self.args.plc2_log}", flush=True)
            print(f"CAN_LOG={self.args.can_log}", flush=True)
            print(f"SUMMARY_FILE={self.args.summary_file}", flush=True)

            st1 = self._bootstrap_plc("plc1", self.args.plc1_id)
            st2 = self._bootstrap_plc("plc2", self.args.plc2_id)
            print(
                f"[BOOT] plc1 mode={st1.mode_name} can_stack={st1.can_stack} module_mgr={st1.module_mgr}; "
                f"plc2 mode={st2.mode_name} can_stack={st2.can_stack} module_mgr={st2.module_mgr}",
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

            home_current_a = self._effective_current(self.args.home_current_a, self.home_module.rated_current_a, "home")
            donor_shared_current_a = self._effective_current(
                self.args.donor_shared_current_a, self.donor_module.rated_current_a, "donor_shared"
            )
            home_shared_current_a = self._effective_current(
                self.args.home_shared_current_a, self.home_module.rated_current_a, "home_shared"
            )
            donor_bootstrap_current_a = min(self.args.donor_bootstrap_current_a, donor_shared_current_a)
            donor_release_hold_a = min(self.args.donor_hold_current_a, donor_shared_current_a)

            self.desired_relays[("plc1", 1)] = True
            self.desired_relays[("plc2", 2)] = False
            self.current_plan["home"] = (True, self.args.voltage_v, home_current_a)
            self.current_plan["donor"] = (False, 0.0, 0.0)

            self._set_phase("home_1_startup")
            self._wait_until(
                self.args.startup_timeout_s,
                lambda: self._relay_is_closed("plc1", 1),
                "PLC1 Relay1 did not close",
            )
            self._wait_until(
                self.args.startup_timeout_s,
                lambda: self.home_module is not None and module_voltage_ok(self.home_module, self.args.min_home_module_voltage_v),
                "home module failed to reach target voltage",
            )
            self._hold_phase("home_1", self.args.home_duration_s)

            self._set_phase("shared_prepare")
            self.current_plan["donor"] = (True, self.args.voltage_v, donor_bootstrap_current_a)
            self._wait_until(
                self.args.startup_timeout_s,
                lambda: self.donor_module is not None and self._module_matched(
                    self.donor_module, self.args.voltage_v, self.args.match_tolerance_v
                ),
                "donor module failed isolated precharge match",
            )
            self.desired_relays[("plc2", 2)] = True
            self._wait_until(
                self.args.attach_timeout_s,
                lambda: self._relay_is_closed("plc2", 2),
                "PLC2 Relay2 did not close",
            )
            self._wait_until(
                self.args.attach_timeout_s,
                lambda: self._bus_voltage_estimate() >= self.args.min_shared_bus_voltage_v,
                "shared bus did not reach minimum voltage after attach",
            )
            self._ramp_plan(
                "shared_ramp",
                self.args.share_ramp_s,
                home_start_a=home_current_a,
                home_end_a=home_shared_current_a,
                donor_start_a=donor_bootstrap_current_a,
                donor_end_a=donor_shared_current_a,
            )
            self._hold_phase("shared", self.args.shared_duration_s)

            self._set_phase("release_prepare")
            self._ramp_plan(
                "release_ramp",
                self.args.release_ramp_s,
                home_start_a=home_shared_current_a,
                home_end_a=home_current_a,
                donor_start_a=donor_shared_current_a,
                donor_end_a=donor_release_hold_a,
            )
            self._wait_until(
                self.args.release_timeout_s,
                lambda: self._bus_voltage_estimate() >= self.args.min_home_bus_voltage_v,
                "home module failed to recover bus voltage before donor release",
            )
            self._wait_until(
                self.args.release_timeout_s,
                self._donor_current_low,
                "donor current did not drain before bus release",
            )
            self.desired_relays[("plc2", 2)] = False
            self._wait_until(
                self.args.release_timeout_s,
                lambda: not self._relay_is_closed("plc2", 2),
                "PLC2 Relay2 did not open",
            )
            if self.donor_module:
                self.can.stop_output(self.donor_module)
                time.sleep(0.05)
                self.can.stop_output(self.donor_module)
            self.current_plan["donor"] = (False, 0.0, 0.0)

            self._hold_phase("home_2", self.args.post_release_duration_s)

            self._set_phase("stop")
            self._shutdown_modules()
            self._open_relays()
            settle_deadline = time.time() + self.args.shutdown_settle_s
            while time.time() < settle_deadline:
                self._tick()
                time.sleep(0.05)

            result = "pass" if not self.failures else "fail"
            if result == "pass":
                print("[PASS] direct module transition completed without bus-voltage collapse", flush=True)
            else:
                print(f"[FAIL] failures={self.failures}", flush=True)
            return 0 if result == "pass" else 1
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
                self._open_relays()
            except Exception:
                pass
            self.plc1_link.close()
            self.plc2_link.close()
            self.can.close()
            self._write_summary(result)
            print(f"SUMMARY_FILE_DONE={self.args.summary_file}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mode-2 UART-router plus direct Maxwell module transition test")
    parser.add_argument("--plc1-port", default="/dev/ttyACM0")
    parser.add_argument("--plc2-port", default="/dev/ttyACM1")
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument("--can-iface", default="can0")
    parser.add_argument("--plc1-id", type=int, default=1)
    parser.add_argument("--plc2-id", type=int, default=2)
    parser.add_argument("--controller-id", type=int, default=1)
    parser.add_argument("--input-mode", type=int, default=3)
    parser.add_argument("--voltage-v", type=float, default=500.0)
    parser.add_argument("--home-current-a", type=float, default=40.0)
    parser.add_argument("--home-shared-current-a", type=float, default=20.0)
    parser.add_argument("--donor-shared-current-a", type=float, default=20.0)
    parser.add_argument("--donor-bootstrap-current-a", type=float, default=5.0)
    parser.add_argument("--donor-hold-current-a", type=float, default=0.5)
    parser.add_argument("--home-duration-s", type=int, default=40)
    parser.add_argument("--shared-duration-s", type=int, default=40)
    parser.add_argument("--post-release-duration-s", type=int, default=40)
    parser.add_argument("--startup-timeout-s", type=float, default=30.0)
    parser.add_argument("--attach-timeout-s", type=float, default=20.0)
    parser.add_argument("--release-timeout-s", type=float, default=20.0)
    parser.add_argument("--shutdown-settle-s", type=float, default=3.0)
    parser.add_argument("--transition-grace-s", type=float, default=3.0)
    parser.add_argument("--share-ramp-s", type=float, default=3.0)
    parser.add_argument("--release-ramp-s", type=float, default=3.0)
    parser.add_argument("--control-interval-s", type=float, default=0.5)
    parser.add_argument("--telemetry-interval-s", type=float, default=1.0)
    parser.add_argument("--telemetry-response-s", type=float, default=0.25)
    parser.add_argument("--status-interval-s", type=float, default=10.0)
    parser.add_argument("--max-status-misses", type=int, default=5)
    parser.add_argument("--relay-hold-ms", type=int, default=1500)
    parser.add_argument("--relay-refresh-s", type=float, default=0.6)
    parser.add_argument("--home-addr", type=int, default=1)
    parser.add_argument("--home-group", type=int, default=1)
    parser.add_argument("--donor-addr", type=int, default=3)
    parser.add_argument("--donor-group", type=int, default=2)
    parser.add_argument("--default-rated-current-a", type=float, default=30.0)
    parser.add_argument("--absolute-current-scale", type=float, default=1024.0)
    parser.add_argument("--enforce-rated-current", action="store_true")
    parser.add_argument("--match-tolerance-v", type=float, default=15.0)
    parser.add_argument("--min-home-module-voltage-v", type=float, default=450.0)
    parser.add_argument("--min-home-bus-voltage-v", type=float, default=450.0)
    parser.add_argument("--min-shared-bus-voltage-v", type=float, default=450.0)
    parser.add_argument("--release-donor-current-a", type=float, default=1.0)
    parser.add_argument("--plc1-log", default="")
    parser.add_argument("--plc2-log", default="")
    parser.add_argument("--can-log", default="")
    parser.add_argument("--summary-file", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    args.plc1_log = args.plc1_log or f"/home/jpi/Desktop/EVSE/plc_firmware/logs/module_transition_direct_{stamp}_plc1.log"
    args.plc2_log = args.plc2_log or f"/home/jpi/Desktop/EVSE/plc_firmware/logs/module_transition_direct_{stamp}_plc2.log"
    args.can_log = args.can_log or f"/home/jpi/Desktop/EVSE/plc_firmware/logs/module_transition_direct_{stamp}_can.log"
    args.summary_file = args.summary_file or f"/home/jpi/Desktop/EVSE/plc_firmware/logs/module_transition_direct_{stamp}_summary.json"
    runner = ModuleTransitionRunner(args)
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
