#!/usr/bin/env python3
"""
Controller-side charging validation for CB PLC firmware over serial.

This script drives controller APIs exposed by firmware serial bridge:
  CTRL HB, CTRL AUTH, CTRL SLAC, CTRL ALLOC1, CTRL MODE, CTRL STATUS

Goal:
  - keep controller heartbeat/auth valid
  - allocate module 1
  - arm/start SLAC when gun connected
  - grant auth only after vehicle identity arrives (EV MAC or EVCCID)
  - print meter/SOC every 5 seconds
  - hold charging for N minutes (default: 10) based on CurrentDemand logs
  - stop session and verify module is OFF
"""

from __future__ import annotations

import argparse
import datetime as dt
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
CP_CONNECTED_RE = re.compile(r"\[CP\] state .* -> ([BCD])")
CP_DISCONNECT_RE = re.compile(r"\[CP\] state .* -> ([AEF])")
CTRL_STATUS_RE = re.compile(r"\[SERCTRL\] STATUS .*cp=([A-Z0-9]+)\s+duty=(\d+)")
SESSION_SETUP_EVCCID_RE = re.compile(r'"msg":"SessionSetup".*?"evccId":"([0-9A-Fa-f]+)"')
EV_MAC_RE = re.compile(r"(?:EV[_ ]?MAC|ev_mac|evmac)[^0-9A-Fa-f]*([0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2}){5})")
SOC_RE = re.compile(r'"soc"\s*:\s*(-?\d+)')
WH_RE = re.compile(r'"wh"\s*:\s*(-?\d+)')
MODRT_GRP_RE = re.compile(r"\[MODRT\]\[GRP\].*en=(\d+).*assigned=(\d+).*active=(\d+)")
MODRT_MOD_RE = re.compile(r"\[MODRT\]\[MOD\].*id=([^\s]+).*off=(\d+).*\sI=([0-9.+-]+)")


@dataclass
class SessionState:
    cp_connected: bool = False
    slac_matched: bool = False
    hlc_connected: bool = False
    precharge_seen: bool = False
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
    group_enable: Optional[int] = None
    group_assigned_modules: Optional[int] = None
    group_active_modules: Optional[int] = None
    module1_off: Optional[bool] = None
    module1_current_a: Optional[float] = None
    cp_phase: str = "U"
    cp_duty_pct: Optional[int] = None
    b1_since_ts: Optional[float] = None
    last_soc: Optional[int] = None
    last_meter_wh: Optional[int] = None
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
        log_file: str,
    ) -> None:
        self.port = port
        self.baud = baud
        self.duration_s = duration_s
        self.max_total_s = max_total_s
        self.heartbeat_ms = max(200, heartbeat_ms)
        self.heartbeat_timeout_ms = max(3000, self.heartbeat_ms * 8)
        self.auth_ttl_ms = max(1000, auth_ttl_ms)
        self.arm_ms = max(2000, arm_ms)
        self.mode = mode.lower().strip()
        if self.mode not in ("controller", "standalone"):
            self.mode = "controller"
        self.log_file = log_file

        self.ser: Optional[serial.Serial] = None
        self.state = SessionState()
        self._stop = threading.Event()
        self._reader_thread: Optional[threading.Thread] = None
        self._log_fp = None
        self._lock = threading.Lock()

        self._last_hb = 0.0
        self._last_auth = 0.0
        self._last_arm = 0.0
        self._last_start = 0.0
        self._last_alloc = 0.0
        self._last_status = 0.0
        self._last_meter_print = 0.0
        self._stop_verify_deadline: Optional[float] = None
        self._remote_start_sent = False

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
        self.ser = serial.Serial(self.port, self.baud, timeout=0.05)
        self.ser.reset_input_buffer()
        self.ser.reset_output_buffer()
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        self._log_fp = open(self.log_file, "w", encoding="utf-8", buffering=1)

    def _close(self) -> None:
        self._stop.set()
        if self._reader_thread and self._reader_thread.is_alive():
            self._reader_thread.join(timeout=2.0)
        if self._log_fp:
            self._log_fp.close()
            self._log_fp = None
        if self.ser:
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None

    def _write_cmd(self, cmd: str) -> None:
        if not self.ser:
            return
        line = (cmd.strip() + "\n").encode("utf-8", errors="ignore")
        self.ser.write(line)

    def _reader_loop(self) -> None:
        assert self.ser is not None
        assert self._log_fp is not None
        while not self._stop.is_set():
            try:
                raw = self.ser.readline()
            except Exception as exc:
                with self._lock:
                    self.state.errors.append(f"serial read error: {exc}")
                break
            if not raw:
                continue
            try:
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            except Exception:
                line = repr(raw)
            ts = dt.datetime.now().isoformat(timespec="milliseconds")
            self._log_fp.write(f"{ts} {line}\n")
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
                except ValueError:
                    pass

            m_mod = MODRT_MOD_RE.search(line)
            if m_mod and m_mod.group(1) == "MXR_1":
                try:
                    self.state.module1_off = (int(m_mod.group(2)) == 1)
                    self.state.module1_current_a = float(m_mod.group(3))
                except ValueError:
                    pass

            m_status = CTRL_STATUS_RE.search(line)
            if m_status:
                self.state.cp_phase = m_status.group(1)
                try:
                    self.state.cp_duty_pct = int(m_status.group(2))
                except ValueError:
                    self.state.cp_duty_pct = None
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

    def _controller_bootstrap(self) -> None:
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

        if self.mode == "standalone":
            # Standalone mode: no external controller driving auth/SLAC.
            self._write_cmd("CTRL MODE 1 1 1")
            self._write_cmd("CTRL STATUS")
            time.sleep(0.4)
            self._write_cmd("CTRL SAVE")
            self._write_cmd("CTRL STATUS")
            return

        # Controller-dependent mode.
        self._write_cmd("CTRL MODE 0 1 1")
        self._write_cmd(f"CTRL HB {self.heartbeat_timeout_ms}")
        self._write_cmd("CTRL STATUS")
        self._write_cmd("CTRL ALLOC1")
        self._write_cmd(f"CTRL AUTH pending {self.auth_ttl_ms}")
        self._write_cmd("CTRL STATUS")

    def _tick_controller(self) -> None:
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

    def _tick_standalone(self) -> None:
        now = time.time()
        if now - self._last_status >= 5.0:
            self._write_cmd("CTRL STATUS")
            self._last_status = now

    def _request_stop(self) -> None:
        self._write_cmd("CTRL STOP hard 3000")
        if self.mode != "standalone":
            self._write_cmd(f"CTRL AUTH deny {self.auth_ttl_ms}")
            self._write_cmd("CTRL SLAC disarm 3000")
        self._write_cmd("CTRL STATUS")
        with self._lock:
            self.state.stop_sent = True
            self.state.stop_sent_ts = time.time()
            self.state.auth_granted = False
        self._stop_verify_deadline = time.time() + 45.0

    def _tick_stop(self) -> None:
        now = time.time()
        if self.mode != "standalone":
            if (now - self._last_hb) * 1000.0 >= self.heartbeat_ms:
                self._write_cmd(f"CTRL HB {self.heartbeat_timeout_ms}")
                self._last_hb = now
        if now - self._last_status >= 1.0:
            self._write_cmd("CTRL STATUS")
            self._last_status = now
        if self.mode != "standalone":
            if now - self._last_auth >= 0.8:
                self._write_cmd(f"CTRL AUTH deny {self.auth_ttl_ms}")
                self._last_auth = now
        if now - self._last_arm >= 1.5:
            self._write_cmd("CTRL STOP hard 3000")
            if self.mode != "standalone":
                self._write_cmd("CTRL SLAC disarm 3000")
            self._last_arm = now
        if now - self._last_start >= 6.0:
            self._write_cmd("CTRL RESET")
            self._last_start = now

    def _stop_verification_ok(self) -> bool:
        with self._lock:
            relay_open = self.state.relay1_open_seen
            group_disabled = (self.state.group_enable == 0) if self.state.group_enable is not None else False
            no_assigned = (self.state.group_assigned_modules == 0) if self.state.group_assigned_modules is not None else False
            no_active = (self.state.group_active_modules == 0) if self.state.group_active_modules is not None else False
            mod_off = (self.state.module1_off is True)
            mod_i = self.state.module1_current_a
            mod_i_ok = (mod_i is not None and abs(mod_i) <= 0.2)
            # Some module firmwares do not assert an explicit "off" bit even when OFF command is applied.
            mod_off_or_idle = mod_off or (mod_i_ok and no_active and no_assigned)
        return relay_open and group_disabled and no_assigned and no_active and mod_off_or_idle

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
                    errs = list(s.errors)

                if errs:
                    print(f"[FAIL] serial/runtime errors: {errs}", flush=True)
                    break

                if (self.mode != "standalone") and (not stop_phase) and identity_seen and not auth_granted:
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
                        f"cd={cd_count} window_s={charge_elapsed:.1f}",
                        flush=True,
                    )
                    self._last_meter_print = now

                if (not stop_phase) and charge_elapsed >= self.duration_s:
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
                    f"precharge={int(s.precharge_seen)} currentDemand={s.current_demand_count} "
                    f"sessionDone={s.session_done_count} identity={s.identity_source}:{s.identity_value} "
                    f"soc={s.last_soc if s.last_soc is not None else -1} "
                    f"wh={s.last_meter_wh if s.last_meter_wh is not None else -1} "
                    f"relay1_open={int(s.relay1_open_seen)} "
                    f"group_en={s.group_enable if s.group_enable is not None else -1} "
                    f"group_assigned={s.group_assigned_modules if s.group_assigned_modules is not None else -1} "
                    f"group_active={s.group_active_modules if s.group_active_modules is not None else -1} "
                    f"module1_off={int(s.module1_off) if s.module1_off is not None else -1} "
                    f"module1_i={s.module1_current_a if s.module1_current_a is not None else -1.0}",
                    flush=True,
                )
            return 0 if success else 1
        finally:
            self._close()
            print(f"LOG_FILE_DONE={self.log_file}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Controller-driven 10-minute charging validation")
    ap.add_argument("--port", default="/dev/ttyACM0")
    ap.add_argument("--baud", type=int, default=115200)
    ap.add_argument("--mode", choices=["controller", "standalone"], default="controller")
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
        log_file=log_file,
    )
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
