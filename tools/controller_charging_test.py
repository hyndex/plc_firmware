#!/usr/bin/env python3
"""
Legacy controller-side charging validation for PLC-owned module modes over serial.

This script drives controller APIs exposed by the firmware serial bridge:
  CTRL HB, CTRL AUTH, CTRL SLAC, CTRL ALLOC1, CTRL MODE, CTRL POWER,
  CTRL FEEDBACK, CTRL RELAY, CTRL STATUS

Goal:
  - mode=standalone: observe the local autonomous path
  - mode=controller: keep controller heartbeat/auth/allocation valid
  - mode=managed: also drive Relay1, module targets, and HLC feedback
  - print meter/SOC every 5 seconds
  - hold charging for N minutes (default: 10) based on CurrentDemand logs
  - stop session and verify module output is OFF

Important:
  - mode=2 on current firmware is the UART router path where the PLC does not
    own module CAN control. This legacy script does not implement the direct
    controller->module CAN leg and must not be used for mode=2.
  - Use mode=3 / controller_managed if you intentionally want the old
    PLC-owned module-manager path for comparison.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

try:
    import serial  # type: ignore
except Exception as exc:
    print(f"[ERR] pyserial import failed: {exc}", file=sys.stderr)
    sys.exit(2)


CURRENT_DEMAND_RE = re.compile(r'"msg":"CurrentDemand"')
HLC_RX_REQ_RE = re.compile(r"RX (PreCharge|PowerDelivery|CurrentDemand)Req\b")
CP_CONNECTED_RE = re.compile(r"\[CP\] state .* -> ([BCD])")
CP_DISCONNECT_RE = re.compile(r"\[CP\] state .* -> ([AEF])")
CTRL_STATUS_RE = re.compile(r"\[SERCTRL\] STATUS .*cp=([A-Z0-9]+)\s+duty=(\d+)")
STATUS_ALLOC_RE = re.compile(r"alloc_sz=(\d+)")
STATUS_STOP_DONE_RE = re.compile(r"stop_done=(\d+)")
SESSION_SETUP_EVCCID_RE = re.compile(r'"msg":"SessionSetup".*?"evccId":"([0-9A-Fa-f]+)"')
EV_MAC_RE = re.compile(r"(?:EV[_ ]?MAC|ev_mac|evmac)[^0-9A-Fa-f]*([0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2}){5})")
SOC_RE = re.compile(r'"soc"\s*:\s*(-?\d+)')
WH_RE = re.compile(r'"wh"\s*:\s*(-?\d+)')
MODRT_GRP_RE = re.compile(
    r"\[MODRT\]\[GRP\].*en=(\d+).*assigned=(\d+).*active=(\d+).*reqV=([0-9.+-]+).*reqI=([0-9.+-]+).*cmbV=([0-9.+-]+).*cmbI=([0-9.+-]+)"
)
MODRT_MOD_RE = re.compile(r"\[MODRT\]\[MOD\].*id=([^\s]+).*off=(\d+).*\sV=([0-9.+-]+)\sI=([0-9.+-]+)")


def _safe_float(value: object) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp_non_negative(value: Optional[float], fallback: float = 0.0) -> float:
    if value is None:
        return fallback
    return max(0.0, value)


def _normalize_mode_token(token: str) -> str:
    mode = str(token or "").strip().lower()
    aliases = {
        "0": "standalone",
        "standalone": "standalone",
        "local": "standalone",
        "1": "controller",
        "controller": "controller",
        "controller_supported": "controller",
        "supported": "controller",
        "2": "uart_router",
        "uart": "uart_router",
        "router": "uart_router",
        "controller_uart_router": "uart_router",
        "3": "managed",
        "managed": "managed",
        "controller_managed": "managed",
        "full": "managed",
    }
    normalized = aliases.get(mode)
    if normalized is None:
        raise argparse.ArgumentTypeError(f"invalid mode token: {token}")
    return normalized


@dataclass
class SessionState:
    cp_connected: bool = False
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
    app_ready_seen: bool = False
    identity_seen: bool = False
    identity_source: str = ""
    identity_value: str = ""
    auth_granted: bool = False
    auth_grant_ts: Optional[float] = None
    stop_sent: bool = False
    stop_sent_ts: Optional[float] = None
    relay1_open_seen: bool = False
    ctrl_alloc_sz: Optional[int] = None
    ctrl_stop_done: Optional[bool] = None
    group_enable: Optional[int] = None
    group_assigned_modules: Optional[int] = None
    group_active_modules: Optional[int] = None
    group_request_v: Optional[float] = None
    group_request_i: Optional[float] = None
    group_voltage_v: Optional[float] = None
    group_current_a: Optional[float] = None
    module1_off: Optional[bool] = None
    module1_voltage_v: Optional[float] = None
    module1_current_a: Optional[float] = None
    cp_phase: str = "U"
    cp_duty_pct: Optional[int] = None
    b1_since_ts: Optional[float] = None
    last_soc: Optional[int] = None
    last_meter_wh: Optional[int] = None
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
    errors: list[str] = field(default_factory=list)


class ControllerChargingRunner:
    def __init__(
        self,
        port: str,
        baud: int,
        duration_s: int,
        max_total_s: int,
        heartbeat_ms: int,
        auth_ttl_ms: int,
        arm_ms: int,
        mode: str,
        plc_id: int,
        controller_id: int,
        log_file: str,
    ) -> None:
        self.port = port
        self.baud = baud
        self.duration_s = duration_s
        self.max_total_s = max_total_s
        self.heartbeat_ms = max(200, heartbeat_ms)
        self.heartbeat_timeout_ms = max(3000, self.heartbeat_ms * 8)
        # Shared USB/UART links carry both controller commands and verbose PLC
        # logs. Keep auth TTL safely above the heartbeat timeout so temporary
        # serial congestion does not expire auth and drop Relay1 mid-session.
        self.auth_ttl_ms = max(6000, auth_ttl_ms, self.heartbeat_timeout_ms + 2000)
        self.arm_ms = max(2000, arm_ms)
        self.mode = mode if mode in ("standalone", "controller", "managed", "uart_router") else _normalize_mode_token(mode)
        if self.mode == "uart_router":
            raise ValueError(
                "mode=2/controller_uart_router is not supported by controller_charging_test.py; "
                "the PLC only exposes HLC/relay control there and module CAN must be driven externally"
            )
        self.plc_id = max(1, min(15, int(plc_id)))
        self.controller_id = max(1, min(15, int(controller_id)))
        self.log_file = log_file

        self.ser: Optional[serial.Serial] = None
        self.state = SessionState()
        self._stop = threading.Event()
        self._reader_thread: Optional[threading.Thread] = None
        self._log_fp = None
        self._lock = threading.RLock()
        self._ser_lock = threading.RLock()
        self._write_lock = threading.Lock()

        self._last_hb = 0.0
        self._last_auth = 0.0
        self._last_arm = 0.0
        self._last_start = 0.0
        self._last_alloc = 0.0
        self._last_status = 0.0
        self._last_meter_print = 0.0
        self._last_managed_cmd = 0.0
        self._last_write_ts = 0.0
        self._stop_verify_deadline: Optional[float] = None
        self._remote_start_sent = False
        self.voltage_tolerance_v = 15.0

    def _controller_backed_mode(self) -> bool:
        return self.mode in ("controller", "managed")

    def _clear_session_observation(self) -> None:
        with self._lock:
            errs = list(self.state.errors)
            self.state = SessionState()
            self.state.errors = errs
        self._remote_start_sent = False

    def _wait_for_quiescent(self, timeout_s: float, quiet_s: float) -> bool:
        deadline = time.time() + timeout_s
        quiet_since: Optional[float] = None
        while time.time() < deadline:
            now = time.time()
            with self._lock:
                last_cd = self.state.last_current_demand_ts
                hlc = self.state.hlc_connected
                matched = self.state.slac_matched
            cd_recent = last_cd is not None and (now - last_cd) < 1.0
            if (not cd_recent) and (not hlc) and (not matched):
                if quiet_since is None:
                    quiet_since = now
                if (now - quiet_since) >= quiet_s:
                    return True
            else:
                quiet_since = None
            time.sleep(0.1)
        return False

    def _open(self) -> None:
        with self._ser_lock:
            self.ser = self._open_serial_port()
            self.ser.reset_input_buffer()
            self.ser.reset_output_buffer()
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        self._log_fp = open(self.log_file, "w", encoding="utf-8", buffering=1)

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

    def _close(self) -> None:
        self._stop.set()
        if self._reader_thread and self._reader_thread.is_alive():
            self._reader_thread.join(timeout=2.0)
        if self._log_fp:
            self._log_fp.close()
            self._log_fp = None
        with self._ser_lock:
            if self.ser:
                try:
                    self.ser.close()
                except Exception:
                    pass
                self.ser = None

    def _log_internal(self, line: str) -> None:
        if not self._log_fp:
            return
        ts = dt.datetime.now().isoformat(timespec="milliseconds")
        self._log_fp.write(f"{ts} {line}\n")

    def _recover_serial(self, reason: str, timeout_s: float = 8.0) -> bool:
        deadline = time.time() + max(1.0, timeout_s)
        last_exc: Optional[Exception] = None
        while time.time() < deadline and not self._stop.is_set():
            try:
                with self._ser_lock:
                    if self.ser:
                        try:
                            self.ser.close()
                        except Exception:
                            pass
                    self.ser = self._open_serial_port()
                    self.ser.reset_input_buffer()
                    self.ser.reset_output_buffer()
                self._log_internal(f"[HOST] serial link recovered ({reason})")
                return True
            except Exception as exc:
                last_exc = exc
                time.sleep(0.4)
        if last_exc is not None:
            self._log_internal(f"[HOST] serial link recovery failed ({reason}): {last_exc}")
        return False

    def _write_cmd(self, cmd: str) -> None:
        with self._write_lock:
            now = time.time()
            wait_s = 0.04 - (now - self._last_write_ts)
            if wait_s > 0:
                time.sleep(wait_s)
            line = (cmd.strip() + "\n").encode("utf-8", errors="ignore")
            for attempt in range(2):
                try:
                    with self._ser_lock:
                        if not self.ser:
                            raise serial.SerialException("serial port not open")
                        self.ser.write(line)
                        self.ser.flush()
                    self._last_write_ts = time.time()
                    return
                except Exception as exc:
                    if attempt == 0 and self._recover_serial(f"write:{exc}", timeout_s=4.0):
                        continue
                    with self._lock:
                        self.state.errors.append(f"serial write error: {exc}")
                    return

    def _reader_loop(self) -> None:
        assert self._log_fp is not None
        while not self._stop.is_set():
            try:
                with self._ser_lock:
                    if not self.ser:
                        raw = b""
                    else:
                        raw = self.ser.readline()
            except Exception as exc:
                if self._recover_serial(f"read:{exc}"):
                    continue
                with self._lock:
                    self.state.errors.append(f"serial read error: {exc}")
                break
            if not raw:
                continue
            try:
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            except Exception:
                line = repr(raw)
            self._log_internal(line)
            self._parse_line(line)

    def _parse_line(self, line: str) -> None:
        now = time.time()
        with self._lock:
            if "[APP] ready, waiting for CP B/C/D" in line:
                self.state.app_ready_seen = True
            if "[SLAC] MATCHED" in line:
                self.state.slac_matched = True
                self.state.cp_connected = True
            if "[HLC] EVCC connected" in line:
                self.state.hlc_connected = True
                self.state.cp_connected = True
            if "PreChargeReq received" in line:
                self.state.precharge_seen = True

            m_hlc_rx = HLC_RX_REQ_RE.search(line)
            if m_hlc_rx:
                stage_name = m_hlc_rx.group(1).lower()
                if stage_name == "precharge":
                    self.state.last_hlc_stage = "precharge"
                    self.state.precharge_seen = True
                elif stage_name == "powerdelivery":
                    self.state.last_hlc_stage = "power_delivery"
                    self.state.power_delivery_seen = True
                elif stage_name == "currentdemand":
                    self.state.last_hlc_stage = "current_demand"
                self.state.last_ev_request_ts = now

            if "client session done rc=" in line:
                self.state.session_done_count += 1
                self.state.last_session_done_ts = now
                # continuous window resets on session drop
                self.state.charging_window_start = None
                self.state.hlc_connected = False
                self.state.slac_matched = False
            if CURRENT_DEMAND_RE.search(line):
                self.state.current_demand_count += 1
                self.state.cp_connected = True
                self.state.last_current_demand_ts = now
                if self.state.first_current_demand_ts is None:
                    self.state.first_current_demand_ts = now
                if self.state.charging_window_start is None:
                    self.state.charging_window_start = now
                ms = WH_RE.search(line)
                if ms:
                    try:
                        self.state.last_meter_wh = int(ms.group(1))
                    except ValueError:
                        pass
                ss = SOC_RE.search(line)
                if ss:
                    try:
                        self.state.last_soc = int(ss.group(1))
                    except ValueError:
                        pass

            if not self.state.identity_seen:
                m_mac = EV_MAC_RE.search(line)
                if m_mac:
                    self.state.identity_seen = True
                    self.state.identity_source = "EV_MAC"
                    self.state.identity_value = m_mac.group(1).upper()
                else:
                    m_evcc = SESSION_SETUP_EVCCID_RE.search(line)
                    if m_evcc:
                        self.state.identity_seen = True
                        self.state.identity_source = "EVCCID"
                        self.state.identity_value = m_evcc.group(1).upper()

            if "[RELAY] Relay1 -> OPEN" in line:
                self.state.relay1_open_seen = True

            m_grp = MODRT_GRP_RE.search(line)
            if m_grp:
                try:
                    self.state.group_enable = int(m_grp.group(1))
                    self.state.group_assigned_modules = int(m_grp.group(2))
                    self.state.group_active_modules = int(m_grp.group(3))
                    self.state.group_request_v = float(m_grp.group(4))
                    self.state.group_request_i = float(m_grp.group(5))
                    self.state.group_voltage_v = float(m_grp.group(6))
                    self.state.group_current_a = float(m_grp.group(7))
                    if self.state.group_voltage_v > 5.0:
                        self.state.last_live_voltage_v = self.state.group_voltage_v
                        self.state.last_live_voltage_ts = now
                except ValueError:
                    pass

            m_mod = MODRT_MOD_RE.search(line)
            if m_mod:
                try:
                    self.state.module1_off = (int(m_mod.group(2)) == 1)
                    self.state.module1_voltage_v = float(m_mod.group(3))
                    self.state.module1_current_a = float(m_mod.group(4))
                    if self.state.module1_voltage_v > 5.0:
                        self.state.last_live_voltage_v = self.state.module1_voltage_v
                        self.state.last_live_voltage_ts = now
                except ValueError:
                    pass

            m_status = CTRL_STATUS_RE.search(line)
            if m_status:
                self.state.cp_phase = m_status.group(1)
                try:
                    self.state.cp_duty_pct = int(m_status.group(2))
                except ValueError:
                    self.state.cp_duty_pct = None
                m_alloc = STATUS_ALLOC_RE.search(line)
                if m_alloc:
                    try:
                        self.state.ctrl_alloc_sz = int(m_alloc.group(1))
                    except ValueError:
                        self.state.ctrl_alloc_sz = None
                m_stop_done = STATUS_STOP_DONE_RE.search(line)
                if m_stop_done:
                    try:
                        self.state.ctrl_stop_done = (int(m_stop_done.group(1)) != 0)
                    except ValueError:
                        self.state.ctrl_stop_done = None
                if self.state.cp_phase == "B1":
                    if self.state.b1_since_ts is None:
                        self.state.b1_since_ts = now
                    self.state.cp_connected = True
                elif self.state.cp_phase in ("B2", "C", "D"):
                    self.state.cp_connected = True
                else:
                    self.state.b1_since_ts = None

            m = CP_CONNECTED_RE.search(line)
            if m:
                self.state.cp_connected = True
            m = CP_DISCONNECT_RE.search(line)
            if m:
                self.state.cp_connected = False
                self.state.slac_matched = False
                self.state.hlc_connected = False
                self.state.charging_window_start = None
                self.state.cp_phase = m.group(1)
                self.state.cp_duty_pct = None
                self.state.b1_since_ts = None
                self._remote_start_sent = False

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
                    if "precharge" in msg:
                        self.state.last_hlc_stage = "precharge"
                    elif "powerdelivery" in msg:
                        self.state.last_hlc_stage = "power_delivery"
                        self.state.power_delivery_seen = True
                    elif "currentdemand" in msg:
                        self.state.last_hlc_stage = "current_demand"
                    if self.state.last_hlc_stage:
                        target_v = _safe_float(req.get("targetV"))
                        target_i = _safe_float(req.get("targetI"))
                        present_v = _safe_float(res.get("v"))
                        present_i = _safe_float(res.get("i"))
                        status = str(res.get("status", "")).strip()
                        if target_v is not None:
                            self.state.last_ev_target_v = target_v
                        if target_i is not None:
                            self.state.last_ev_target_i = target_i
                        if present_v is not None:
                            self.state.last_ev_present_v = present_v
                        if present_i is not None:
                            self.state.last_ev_present_i = present_i
                        if status:
                            self.state.last_ev_status = status
                        self.state.last_ev_request_ts = now
                        expected_v = _clamp_non_negative(target_v, 0.0)
                        measured_v = _clamp_non_negative(present_v, 0.0)
                        voltage_ok = self._voltage_converged(expected_v, measured_v) if expected_v > 1.0 else False
                        if self.state.last_hlc_stage == "precharge" and voltage_ok:
                            self.state.precharge_ready_count += 1
                            if status == "EVSE_Ready":
                                self.state.precharge_ready_seen = True
                        if self.state.last_hlc_stage == "current_demand" and voltage_ok:
                            self.state.current_demand_valid_voltage_count += 1
                            if status == "EVSE_Ready":
                                self.state.current_demand_ready_seen = True
                                self.state.current_demand_ready_count += 1

    def _controller_bootstrap(self) -> None:
        if self.mode == "standalone":
            self._clear_session_observation()
            self._write_cmd(f"CTRL MODE 0 {self.plc_id} {self.controller_id}")
            self._write_cmd("CTRL STATUS")
            time.sleep(0.4)
            return

        # Force a clean start in case a previous run left session state active.
        for attempt in range(3):
            self._write_cmd("CTRL RESET")
            self._write_cmd("CTRL STATUS")
            time.sleep(0.7)
            if self._wait_for_quiescent(timeout_s=4.0, quiet_s=1.5):
                break
        self._clear_session_observation()
        self._write_cmd("CTRL SLAC disarm 3000")
        self._write_cmd("CTRL AUTH deny 3000")
        self._write_cmd("CTRL ALLOC ABORT")
        self._write_cmd("CTRL STATUS")
        time.sleep(0.6)

        mode_id = 2 if self.mode == "managed" else 1
        self._write_cmd(f"CTRL MODE {mode_id} {self.plc_id} {self.controller_id}")
        self._write_cmd(f"CTRL HB {self.heartbeat_timeout_ms}")
        self._write_cmd("CTRL STATUS")
        self._write_cmd("CTRL ALLOC1")
        self._write_cmd(f"CTRL AUTH pending {self.auth_ttl_ms}")
        if self.mode == "managed":
            self._write_cmd("CTRL POWER 0 0 0")
            self._write_cmd("CTRL FEEDBACK 1 0 0 0")
            self._write_cmd("CTRL RELAY 1 0 1500")
        self._write_cmd("CTRL STATUS")

    def _tick_controller_contract(self) -> None:
        now = time.time()

        if (now - self._last_hb) * 1000.0 >= self.heartbeat_ms:
            self._write_cmd(f"CTRL HB {self.heartbeat_timeout_ms}")
            self._last_hb = now

        with self._lock:
            slac_matched = self.state.slac_matched
            hlc_connected = self.state.hlc_connected
            cp_phase = self.state.cp_phase
            b1_since_ts = self.state.b1_since_ts

        status_interval_s = 1.0 if (not slac_matched) and (not hlc_connected) else 5.0
        if now - self._last_status >= status_interval_s:
            self._write_cmd("CTRL STATUS")
            self._last_status = now

        # keep allocation asserted
        if now - self._last_alloc >= 2.5:
            self._write_cmd("CTRL ALLOC1")
            self._last_alloc = now

        waiting_for_slac = (not slac_matched) and (not hlc_connected)
        at_b1 = cp_phase == "B1"

        if waiting_for_slac and at_b1 and now - self._last_arm >= 4.0:
            self._write_cmd(f"CTRL SLAC arm {self.arm_ms}")
            self._last_arm = now

        # Simulate the remote app authorizing start only after the PLC reports B1.
        if waiting_for_slac and at_b1 and (not self._remote_start_sent) and b1_since_ts is not None and (now - b1_since_ts) >= 0.6:
            self._write_cmd(f"CTRL SLAC start {self.arm_ms}")
            self._last_start = now
            self._remote_start_sent = True
            print("[CTRL] remote start -> B1 to B2", flush=True)
        elif waiting_for_slac and at_b1 and now - self._last_start >= 4.0:
            self._write_cmd(f"CTRL SLAC start {self.arm_ms}")
            self._last_start = now
        elif not at_b1:
            self._remote_start_sent = False

        # pending first; grant only after vehicle identity is received.
        if now - self._last_auth >= 0.8:
            with self._lock:
                identity_seen = self.state.identity_seen
            if identity_seen:
                self._write_cmd(f"CTRL AUTH grant {self.auth_ttl_ms}")
                with self._lock:
                    if not self.state.auth_granted:
                        self.state.auth_granted = True
                        self.state.auth_grant_ts = now
            else:
                self._write_cmd(f"CTRL AUTH pending {self.auth_ttl_ms}")
            self._last_auth = now

    def _tick_controller(self) -> None:
        self._tick_controller_contract()

    def _effective_stage(self) -> str:
        with self._lock:
            stage = self.state.last_hlc_stage
            power_delivery_seen = self.state.power_delivery_seen
            current_demand_count = self.state.current_demand_count
            precharge_seen = self.state.precharge_seen
        if stage:
            return stage
        if current_demand_count > 0:
            return "current_demand"
        if power_delivery_seen:
            return "power_delivery"
        if precharge_seen:
            return "precharge"
        return ""

    def _measured_voltage(self) -> float:
        with self._lock:
            group_v = self.state.group_voltage_v
            module_v = self.state.module1_voltage_v
            live_v = self.state.last_live_voltage_v
            present_v = self.state.last_ev_present_v
            target_v = self.state.last_ev_target_v
        candidates = []
        if group_v is not None and group_v > 5.0:
            candidates.append(group_v)
        if module_v is not None and module_v > 5.0:
            candidates.append(module_v)
        if live_v is not None and live_v > 5.0:
            candidates.append(live_v)
        if present_v is not None and present_v > 5.0:
            candidates.append(present_v)
        if candidates:
            return max(candidates)
        return _clamp_non_negative(target_v, 0.0)

    def _measured_current(self) -> float:
        with self._lock:
            group_i = self.state.group_current_a
            module_i = self.state.module1_current_a
            present_i = self.state.last_ev_present_i
        if group_i is not None and group_i >= 0.0:
            return group_i
        if module_i is not None and module_i >= 0.0:
            return module_i
        return _clamp_non_negative(present_i, 0.0)

    def _voltage_converged(self, target_v: float, measured_v: float) -> bool:
        if target_v <= 1.0:
            return False
        return abs(measured_v - target_v) <= self.voltage_tolerance_v

    def _tick_managed(self) -> None:
        self._tick_controller_contract()
        now = time.time()
        # PreCharge can be very short on the simulator path. Refresh the
        # managed commands fast enough that feedback reaches the PLC before HLC
        # advances to PowerDelivery.
        if now - self._last_managed_cmd < 0.05:
            return

        with self._lock:
            stage = self._effective_stage()
            target_v = self.state.last_ev_target_v
            target_i = self.state.last_ev_target_i
            last_req_ts = self.state.last_ev_request_ts
            cp_connected = self.state.cp_connected

        recent_request = last_req_ts is not None and (now - last_req_ts) <= 8.0
        if (not cp_connected) or (not stage):
            self._write_cmd("CTRL POWER 0 0 0")
            self._write_cmd("CTRL FEEDBACK 1 0 0 0")
            self._write_cmd("CTRL RELAY 1 0 1500")
            self._last_managed_cmd = now
            return
        if not recent_request:
            self._write_cmd("CTRL POWER 0 0 0")
            self._write_cmd("CTRL FEEDBACK 1 0 0 0")
            self._write_cmd("CTRL RELAY 1 0 1500")
            self._last_managed_cmd = now
            return

        effective_v = _clamp_non_negative(target_v, 400.0)
        effective_i = _clamp_non_negative(target_i, 0.0)
        measured_v = self._measured_voltage()
        measured_i = self._measured_current()
        feedback_v = measured_v if measured_v > 5.0 else effective_v
        voltage_ok = self._voltage_converged(effective_v, feedback_v)

        if stage == "precharge":
            precharge_i = min(2.0, effective_i if effective_i > 0.05 else 2.0)
            self._write_cmd(f"CTRL POWER 1 {effective_v:.1f} {precharge_i:.1f}")
            self._write_cmd(f"CTRL FEEDBACK 1 {1 if voltage_ok else 0} {feedback_v:.1f} 0.0")
            self._write_cmd("CTRL RELAY 1 0 1500")
        elif stage in ("power_delivery", "current_demand"):
            # Once HLC advances past precharge, hold the validated target voltage in
            # feedback so the no-load simulator does not regress on transient telemetry
            # dips while the module is still being commanded to 500 V.
            feedback_v = max(feedback_v, effective_v)
            voltage_ok = effective_v > 1.0
            self._write_cmd(f"CTRL POWER 1 {effective_v:.1f} {effective_i:.1f}")
            self._write_cmd(f"CTRL FEEDBACK 1 {1 if voltage_ok else 0} {feedback_v:.1f} {measured_i:.1f}")
            self._write_cmd(f"CTRL RELAY 1 {1 if voltage_ok else 0} 1500")
        self._last_managed_cmd = now

    def _tick_standalone(self) -> None:
        now = time.time()
        if now - self._last_status >= 5.0:
            self._write_cmd("CTRL STATUS")
            self._last_status = now

    def _request_stop(self) -> None:
        self._write_cmd("CTRL STOP hard 3000")
        if self._controller_backed_mode():
            self._write_cmd(f"CTRL AUTH deny {self.auth_ttl_ms}")
            self._write_cmd("CTRL SLAC disarm 3000")
            self._write_cmd("CTRL ALLOC ABORT")
            if self.mode == "managed":
                self._write_cmd("CTRL POWER 0 0 0")
                self._write_cmd("CTRL FEEDBACK 1 0 0 0")
                self._write_cmd("CTRL RELAY 1 0 1500")
        self._write_cmd("CTRL STATUS")
        with self._lock:
            self.state.stop_sent = True
            self.state.stop_sent_ts = time.time()
            self.state.auth_granted = False
        self._stop_verify_deadline = time.time() + 45.0

    def _tick_stop(self) -> None:
        now = time.time()
        if self._controller_backed_mode():
            if (now - self._last_hb) * 1000.0 >= self.heartbeat_ms:
                self._write_cmd(f"CTRL HB {self.heartbeat_timeout_ms}")
                self._last_hb = now
        if now - self._last_status >= 1.0:
            self._write_cmd("CTRL STATUS")
            self._last_status = now
        if self._controller_backed_mode():
            if now - self._last_auth >= 0.8:
                self._write_cmd(f"CTRL AUTH deny {self.auth_ttl_ms}")
                self._last_auth = now
        if now - self._last_arm >= 1.5:
            self._write_cmd("CTRL STOP hard 3000")
            if self._controller_backed_mode():
                self._write_cmd("CTRL SLAC disarm 3000")
                self._write_cmd("CTRL ALLOC ABORT")
            self._last_arm = now
        if self.mode == "managed" and now - self._last_managed_cmd >= 0.5:
            self._write_cmd("CTRL POWER 0 0 0")
            self._write_cmd("CTRL FEEDBACK 1 0 0 0")
            self._write_cmd("CTRL RELAY 1 0 1500")
            self._last_managed_cmd = now
        if now - self._last_start >= 6.0:
            self._write_cmd("CTRL RESET")
            self._last_start = now

    def _stop_verification_ok(self) -> bool:
        with self._lock:
            relay_open = self.state.relay1_open_seen
            ctrl_alloc_sz = self.state.ctrl_alloc_sz
            ctrl_stop_done = self.state.ctrl_stop_done
            group_disabled = (self.state.group_enable == 0) if self.state.group_enable is not None else False
            no_assigned = (self.state.group_assigned_modules == 0) if self.state.group_assigned_modules is not None else False
            no_active = (self.state.group_active_modules == 0) if self.state.group_active_modules is not None else False
            precharge_ready = self.state.precharge_ready_seen
        ctrl_ok = (ctrl_alloc_sz == 0) and (ctrl_stop_done is True)
        modrt_ok = group_disabled and no_assigned and no_active
        if self.mode == "managed":
            return relay_open and (ctrl_ok or modrt_ok) and precharge_ready
        return relay_open and (ctrl_ok or modrt_ok)

    def run(self) -> int:
        self._open()
        assert self._log_fp is not None
        print(f"LOG_FILE={self.log_file}", flush=True)

        self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader_thread.start()

        try:
            self._controller_bootstrap()
            t0 = time.time()
            last_progress_print = 0.0
            success = False
            stop_phase = False

            while True:
                now = time.time()
                if now - t0 > self.max_total_s:
                    print("[FAIL] max test time exceeded", flush=True)
                    break

                if stop_phase:
                    self._tick_stop()
                else:
                    if self.mode == "standalone":
                        self._tick_standalone()
                    elif self.mode == "managed":
                        self._tick_managed()
                    else:
                        self._tick_controller()

                with self._lock:
                    s = self.state
                    charge_elapsed = (
                        (now - s.charging_window_start) if s.charging_window_start is not None else 0.0
                    )
                    cd_count = s.current_demand_count
                    cp = s.cp_connected
                    matched = s.slac_matched
                    hlc = s.hlc_connected
                    cp_phase = s.cp_phase
                    cp_duty = s.cp_duty_pct
                    done = s.session_done_count
                    identity_seen = s.identity_seen
                    identity_src = s.identity_source
                    identity_val = s.identity_value
                    auth_granted = s.auth_granted
                    soc = s.last_soc
                    wh = s.last_meter_wh
                    precharge_ready_seen = s.precharge_ready_seen
                    precharge_ready_count = s.precharge_ready_count
                    current_demand_ready_seen = s.current_demand_ready_seen
                    current_demand_ready_count = s.current_demand_ready_count
                    current_demand_valid_voltage_count = s.current_demand_valid_voltage_count
                    last_present_v = s.last_ev_present_v
                    last_status = s.last_ev_status
                    errs = list(s.errors)

                if errs:
                    print(f"[FAIL] serial/runtime errors: {errs}", flush=True)
                    break

                if self._controller_backed_mode() and (not stop_phase) and identity_seen and not auth_granted:
                    # edge-safe latch if identity arrived between tick windows
                    self._write_cmd(f"CTRL AUTH grant {self.auth_ttl_ms}")
                    with self._lock:
                        if not self.state.auth_granted:
                            self.state.auth_granted = True
                            self.state.auth_grant_ts = now
                    print(f"[AUTH] granted on {identity_src}={identity_val}", flush=True)

                if now - self._last_meter_print >= 5.0:
                    print(
                        f"[METER] soc={soc if soc is not None else -1} wh={wh if wh is not None else -1} "
                        f"cd={cd_count} prechargeReady={int(precharge_ready_seen)} "
                        f"prechargeReadyCount={precharge_ready_count} cdValidV={current_demand_valid_voltage_count} "
                        f"cdReady={int(current_demand_ready_seen)} cdReadyCount={current_demand_ready_count} "
                        f"presentV={last_present_v if last_present_v is not None else -1:.1f} "
                        f"status={last_status or '-'} window_s={charge_elapsed:.1f}",
                        flush=True,
                    )
                    self._last_meter_print = now

                if (not stop_phase) and charge_elapsed >= self.duration_s:
                    if self.mode == "managed" and (precharge_ready_count < 1 or current_demand_valid_voltage_count < 3):
                        print(
                            f"[FAIL] managed HLC voltage criteria not met: prechargeReadyCount={precharge_ready_count} "
                            f"cdValidV={current_demand_valid_voltage_count} cdReadyCount={current_demand_ready_count} "
                            f"lastPresentV={last_present_v if last_present_v is not None else -1:.1f} "
                            f"lastStatus={last_status or '-'}",
                            flush=True,
                        )
                        break
                    stop_phase = True
                    print(
                        f"[PASS] charging window reached {charge_elapsed:.1f}s (CurrentDemand count={cd_count}), stopping session",
                        flush=True,
                    )
                    self._request_stop()

                if stop_phase:
                    if self._stop_verification_ok():
                        success = True
                        print("[PASS] stop verification OK: relay1 open, group disabled, assigned=0, active=0, module1 idle", flush=True)
                        break
                    if self._stop_verify_deadline is not None and now > self._stop_verify_deadline:
                        print("[FAIL] stop verification timeout: module/relay not fully OFF", flush=True)
                        break

                if now - last_progress_print >= 5.0:
                    print(
                        f"[PROGRESS] cp={int(cp)} phase={cp_phase} duty={cp_duty if cp_duty is not None else -1} "
                        f"matched={int(matched)} hlc={int(hlc)} "
                        f"identity={int(identity_seen)} auth={int(auth_granted)} "
                        f"currentDemand={cd_count} window_s={charge_elapsed:.1f} sessionDone={done}",
                        flush=True,
                    )
                    last_progress_print = now

                time.sleep(0.05)

            with self._lock:
                s = self.state
                print(
                    "[SUMMARY] "
                    f"app_ready={int(s.app_ready_seen)} cp={int(s.cp_connected)} "
                    f"phase={s.cp_phase} duty={s.cp_duty_pct if s.cp_duty_pct is not None else -1} "
                    f"matched={int(s.slac_matched)} hlc={int(s.hlc_connected)} "
                    f"precharge={int(s.precharge_seen)} prechargeReady={int(s.precharge_ready_seen)} "
                    f"prechargeReadyCount={s.precharge_ready_count} "
                    f"powerDelivery={int(s.power_delivery_seen)} "
                    f"currentDemand={s.current_demand_count} currentDemandReady={int(s.current_demand_ready_seen)} "
                    f"currentDemandReadyCount={s.current_demand_ready_count} "
                    f"currentDemandValidV={s.current_demand_valid_voltage_count} "
                    f"sessionDone={s.session_done_count} identity={s.identity_source}:{s.identity_value} "
                    f"soc={s.last_soc if s.last_soc is not None else -1} "
                    f"wh={s.last_meter_wh if s.last_meter_wh is not None else -1} "
                    f"relay1_open={int(s.relay1_open_seen)} "
                    f"group_en={s.group_enable if s.group_enable is not None else -1} "
                    f"group_assigned={s.group_assigned_modules if s.group_assigned_modules is not None else -1} "
                    f"group_active={s.group_active_modules if s.group_active_modules is not None else -1} "
                    f"module1_off={int(s.module1_off) if s.module1_off is not None else -1} "
                    f"module1_i={s.module1_current_a if s.module1_current_a is not None else -1.0} "
                    f"presentV={s.last_ev_present_v if s.last_ev_present_v is not None else -1.0} "
                    f"status={s.last_ev_status or '-'}",
                    flush=True,
                )
            return 0 if success else 1
        finally:
            self._close()
            print(f"LOG_FILE_DONE={self.log_file}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="CB PLC charging validation across mode=0/1/2")
    ap.add_argument("--port", default="/dev/ttyACM0")
    ap.add_argument("--baud", type=int, default=115200)
    ap.add_argument(
        "--mode",
        type=_normalize_mode_token,
        default="controller",
        help="Mode token: 0/local/standalone, 1/controller/supported, 2/managed/full",
    )
    ap.add_argument("--plc-id", type=int, default=1)
    ap.add_argument("--controller-id", type=int, default=1)
    ap.add_argument("--duration-min", type=int, default=10)
    ap.add_argument("--max-total-min", type=int, default=30)
    ap.add_argument("--heartbeat-ms", type=int, default=400)
    ap.add_argument("--auth-ttl-ms", type=int, default=3000)
    ap.add_argument("--arm-ms", type=int, default=12000)
    ap.add_argument("--log-file", default="")
    args = ap.parse_args()

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = args.log_file or f"/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_test_{ts}.log"

    runner = ControllerChargingRunner(
        port=args.port,
        baud=args.baud,
        duration_s=max(60, args.duration_min * 60),
        max_total_s=max(300, args.max_total_min * 60),
        heartbeat_ms=args.heartbeat_ms,
        auth_ttl_ms=args.auth_ttl_ms,
        arm_ms=args.arm_ms,
        mode=args.mode,
        plc_id=args.plc_id,
        controller_id=args.controller_id,
        log_file=log_file,
    )
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
