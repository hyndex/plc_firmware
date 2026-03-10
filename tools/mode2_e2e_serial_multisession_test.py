#!/usr/bin/env python3
"""
UART-controlled mode 2 multisession validation.

This keeps the EV physically connected, runs multiple HLC charging sessions
back-to-back over the serial controller bridge, and fails if CP ever drops to
state A after the test begins.

This harness is legacy-only. Current mode 2 is the UART-router split and does
not allow PLC-owned module allocation/power control.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
import threading
import time

from controller_charging_test import ControllerChargingRunner, SessionState


LEGACY_MODE2_ERROR = (
    "tools/mode2_e2e_serial_multisession_test.py is obsolete for the current "
    "mode-2 UART-router architecture. Use a direct controller->module CAN "
    "application plus the PLC UART router stream instead."
)


CP_TRANSITION_RE = re.compile(r"\[CP\] state .* -> ([A-F])")


class MultiSessionSerialRunner(ControllerChargingRunner):
    def __init__(
        self,
        port: str,
        baud: int,
        duration_s: int,
        max_total_s: int,
        heartbeat_ms: int,
        auth_ttl_ms: int,
        arm_ms: int,
        plc_id: int,
        controller_id: int,
        log_file: str,
        sessions: int,
        gap_s: float,
        retry_gap_s: float,
        stable_b1_s: float,
    ) -> None:
        super().__init__(
            port=port,
            baud=baud,
            duration_s=duration_s,
            max_total_s=max_total_s,
            heartbeat_ms=heartbeat_ms,
            auth_ttl_ms=auth_ttl_ms,
            arm_ms=arm_ms,
            mode="managed",
            plc_id=plc_id,
            controller_id=controller_id,
            log_file=log_file,
        )
        self.sessions = max(1, sessions)
        self.gap_s = max(1.0, gap_s)
        self.retry_gap_s = max(self.gap_s, retry_gap_s)
        self.stable_b1_s = max(1.0, stable_b1_s)
        self.cp_a_count = 0
        self.cp_f_count = 0
        self._cp_monitor_enabled = False
        self._session_cp_a_start = 0
        self._session_cp_f_start = 0
        self._session_attempt_started_at = 0.0
        self._session_retry_count = 0
        self._gap_ready_since = 0.0
        self.session_reports: list[dict[str, object]] = []

    def _parse_line(self, line: str) -> None:
        super()._parse_line(line)
        m = CP_TRANSITION_RE.search(line)
        if not m or not self._cp_monitor_enabled:
            return
        target = m.group(1)
        if target == "A":
            self.cp_a_count += 1
            with self._lock:
                self.state.errors.append("cp transitioned to A while EV should remain connected")
        elif target == "F":
            self.cp_f_count += 1

    def _clear_session_observation_preserve_link(self) -> None:
        with self._lock:
            prev = self.state
            next_state = SessionState(
                cp_connected=prev.cp_connected,
                cp_phase=prev.cp_phase,
                cp_duty_pct=prev.cp_duty_pct,
                b1_since_ts=prev.b1_since_ts,
                app_ready_seen=prev.app_ready_seen,
            )
            next_state.errors = list(prev.errors)
            self.state = next_state
        self._remote_start_sent = False

    def _bootstrap_without_reset(self) -> None:
        self._clear_session_observation_preserve_link()
        self._write_cmd("CTRL STOP hard 3000")
        self._write_cmd("CTRL SLAC disarm 3000")
        self._write_cmd(f"CTRL AUTH deny {self.auth_ttl_ms}")
        self._write_cmd("CTRL ALLOC ABORT")
        self._write_cmd("CTRL POWER 0 0 0")
        self._write_cmd("CTRL FEEDBACK 1 0 0 0")
        self._write_cmd("CTRL RELAY 1 0 1500")
        self._write_cmd(f"CTRL MODE 2 {self.plc_id} {self.controller_id}")
        self._write_cmd(f"CTRL HB {self.heartbeat_timeout_ms}")
        self._write_cmd("CTRL STOP clear 3000")
        self._write_cmd("CTRL STATUS")
        time.sleep(1.0)
        self._last_hb = 0.0
        self._last_auth = 0.0
        self._last_arm = 0.0
        self._last_start = 0.0
        self._last_alloc = 0.0
        self._last_status = 0.0
        self._last_managed_cmd = 0.0
        self._gap_ready_since = 0.0
        self._stop_verify_deadline = None
        self._cp_monitor_enabled = True

    def _begin_session(self, index: int) -> None:
        self._session_cp_a_start = self.cp_a_count
        self._session_cp_f_start = self.cp_f_count
        self._session_attempt_started_at = time.time()
        self._clear_session_observation_preserve_link()
        self._stop_verify_deadline = None
        self._remote_start_sent = False
        self._last_hb = 0.0
        self._last_auth = 0.0
        self._last_arm = 0.0
        self._last_start = 0.0
        self._last_alloc = 0.0
        self._last_status = 0.0
        self._last_managed_cmd = 0.0
        self._gap_ready_since = 0.0
        self._write_cmd(f"CTRL MODE 2 {self.plc_id} {self.controller_id}")
        self._write_cmd(f"CTRL HB {self.heartbeat_timeout_ms}")
        self._write_cmd("CTRL STOP clear 3000")
        self._write_cmd("CTRL STATUS")
        self._write_cmd("CTRL ALLOC1")
        self._write_cmd(f"CTRL AUTH pending {self.auth_ttl_ms}")
        self._write_cmd("CTRL POWER 0 0 0")
        self._write_cmd("CTRL FEEDBACK 1 0 0 0")
        self._write_cmd("CTRL RELAY 1 0 1500")
        print(f"[SESSION {index}] begin: waiting for B1 -> B2 -> HLC", flush=True)

    def _recover_pending_session(self, index: int, reason: str) -> None:
        self._session_retry_count += 1
        print(
            f"[SESSION {index}] recovery {self._session_retry_count}: {reason}; "
            "returning to 100% duty and retrying same session",
            flush=True,
        )
        self._write_cmd("CTRL STOP hard 3000")
        self._write_cmd("CTRL SLAC abort 3000")
        self._write_cmd("CTRL SLAC disarm 3000")
        self._write_cmd(f"CTRL AUTH deny {self.auth_ttl_ms}")
        self._write_cmd("CTRL ALLOC ABORT")
        self._write_cmd("CTRL POWER 0 0 0")
        self._write_cmd("CTRL FEEDBACK 1 0 0 0")
        self._write_cmd("CTRL RELAY 1 0 1500")
        self._stop_verify_deadline = None
        self._remote_start_sent = False
        self._last_hb = 0.0
        self._last_auth = 0.0
        self._last_arm = 0.0
        self._last_start = 0.0
        self._last_alloc = 0.0
        self._last_status = 0.0
        self._last_managed_cmd = 0.0
        self._gap_ready_since = 0.0

    def _tick_stop(self) -> None:
        now = time.time()
        if (now - self._last_hb) * 1000.0 >= self.heartbeat_ms:
            self._write_cmd(f"CTRL HB {self.heartbeat_timeout_ms}")
            self._last_hb = now
        if now - self._last_status >= 1.0:
            self._write_cmd("CTRL STATUS")
            self._last_status = now
        if now - self._last_auth >= 0.6:
            self._write_cmd(f"CTRL AUTH deny {self.auth_ttl_ms}")
            self._last_auth = now
        if now - self._last_arm >= 1.0:
            self._write_cmd("CTRL STOP hard 3000")
            self._write_cmd("CTRL SLAC disarm 3000")
            self._write_cmd("CTRL ALLOC ABORT")
            self._last_arm = now
        if now - self._last_managed_cmd >= 0.4:
            self._write_cmd("CTRL POWER 0 0 0")
            self._write_cmd("CTRL FEEDBACK 1 0 0 0")
            self._write_cmd("CTRL RELAY 1 0 1500")
            self._last_managed_cmd = now

    def _tick_gap(self) -> None:
        now = time.time()
        if (now - self._last_hb) * 1000.0 >= self.heartbeat_ms:
            self._write_cmd(f"CTRL HB {self.heartbeat_timeout_ms}")
            self._last_hb = now
        if now - self._last_status >= 1.0:
            self._write_cmd("CTRL STATUS")
            self._last_status = now
        if now - self._last_auth >= 0.8:
            self._write_cmd(f"CTRL AUTH deny {self.auth_ttl_ms}")
            self._last_auth = now
        if now - self._last_arm >= 2.0:
            self._write_cmd("CTRL SLAC disarm 3000")
            self._write_cmd("CTRL ALLOC ABORT")
            self._last_arm = now
        if now - self._last_managed_cmd >= 0.5:
            self._write_cmd("CTRL POWER 0 0 0")
            self._write_cmd("CTRL FEEDBACK 1 0 0 0")
            self._write_cmd("CTRL RELAY 1 0 1500")
            self._last_managed_cmd = now

    def _idle_connected_ready(self) -> bool:
        with self._lock:
            cp_connected = self.state.cp_connected
            cp_phase = self.state.cp_phase
            cp_duty = self.state.cp_duty_pct
            hlc = self.state.hlc_connected
            matched = self.state.slac_matched
        if hlc or matched:
            return False
        if cp_phase not in ("B", "B1"):
            return False
        if cp_duty is not None and cp_duty < 95:
            return False
        return cp_connected

    def _stop_verification_ok(self) -> bool:
        with self._lock:
            relay_open = self.state.relay1_open_seen
            ctrl_alloc_sz = self.state.ctrl_alloc_sz
            ctrl_stop_done = self.state.ctrl_stop_done
            group_disabled = (self.state.group_enable == 0) if self.state.group_enable is not None else False
            no_assigned = (self.state.group_assigned_modules == 0) if self.state.group_assigned_modules is not None else False
            no_active = (self.state.group_active_modules == 0) if self.state.group_active_modules is not None else False
        ctrl_ok = (ctrl_alloc_sz == 0) and (ctrl_stop_done is True)
        modrt_ok = group_disabled and no_assigned and no_active
        return relay_open and (ctrl_ok or modrt_ok)

    def _record_session(self, index: int) -> None:
        with self._lock:
            s = self.state
            report = {
                "index": index,
                "precharge_seen": s.precharge_seen,
                "power_delivery_seen": s.power_delivery_seen,
                "precharge_ready_count": s.precharge_ready_count,
                "current_demand_count": s.current_demand_count,
                "current_demand_valid_v": s.current_demand_valid_voltage_count,
                "current_demand_ready_count": s.current_demand_ready_count,
                "present_v": s.last_ev_present_v,
                "status": s.last_ev_status,
                "cp_phase": s.cp_phase,
                "cp_duty": s.cp_duty_pct,
                "cp_a_delta": self.cp_a_count - self._session_cp_a_start,
                "cp_f_delta": self.cp_f_count - self._session_cp_f_start,
            }
        self.session_reports.append(report)

    def run(self) -> int:
        self._open()
        assert self._log_fp is not None
        print(f"LOG_FILE={self.log_file}", flush=True)

        self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader_thread.start()

        try:
            self._bootstrap_without_reset()
            t0 = time.time()
            last_progress_print = 0.0
            success = False
            session_index = 1
            phase = "charge"
            gap_until = 0.0
            retry_same_session = False
            self._begin_session(session_index)

            while True:
                now = time.time()
                if now - t0 > self.max_total_s:
                    print("[FAIL] max multisession test time exceeded", flush=True)
                    break

                if phase == "charge":
                    self._tick_managed()
                elif phase == "stop":
                    self._tick_stop()
                elif phase == "gap":
                    self._tick_gap()

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
                    auth_granted = s.auth_granted
                    precharge_ready_count = s.precharge_ready_count
                    current_demand_valid_voltage_count = s.current_demand_valid_voltage_count
                    current_demand_ready_count = s.current_demand_ready_count
                    precharge_seen = s.precharge_seen
                    power_delivery_seen = s.power_delivery_seen
                    last_present_v = s.last_ev_present_v
                    last_status = s.last_ev_status
                    errs = list(s.errors)

                if errs:
                    print(f"[FAIL] serial/runtime errors: {errs}", flush=True)
                    break

                if now - last_progress_print >= 5.0:
                    print(
                        f"[PROGRESS] session={session_index}/{self.sessions} phase={phase} "
                        f"cp={int(cp)} cpPhase={cp_phase} duty={cp_duty if cp_duty is not None else -1} "
                        f"matched={int(matched)} hlc={int(hlc)} identity={int(identity_seen)} auth={int(auth_granted)} "
                        f"cd={cd_count} prechargeReady={precharge_ready_count} "
                        f"cdValidV={current_demand_valid_voltage_count} cdReady={current_demand_ready_count} "
                        f"presentV={last_present_v if last_present_v is not None else -1:.1f} "
                        f"status={last_status or '-'} window_s={charge_elapsed:.1f} cpA={self.cp_a_count} cpF={self.cp_f_count}",
                        flush=True,
                    )
                    last_progress_print = now

                if phase == "charge":
                    if (
                        cd_count == 0
                        and charge_elapsed <= 0.0
                        and (now - self._session_attempt_started_at) >= 45.0
                    ):
                        if self._session_retry_count >= 4:
                            print(
                                f"[FAIL] session {session_index} never reached HLC current demand after "
                                f"{self._session_retry_count} retries",
                                flush=True,
                            )
                            break
                        self._recover_pending_session(session_index, "no progress to CurrentDemand within 45s")
                        gap_until = now + self.retry_gap_s
                        retry_same_session = True
                        phase = "gap"
                        continue
                    if charge_elapsed >= self.duration_s:
                        if (not precharge_seen) or (not power_delivery_seen) or current_demand_valid_voltage_count < 3:
                            print(
                                f"[FAIL] session {session_index} stage/voltage criteria not met: "
                                f"prechargeSeen={int(precharge_seen)} "
                                f"powerDeliverySeen={int(power_delivery_seen)} "
                                f"prechargeReadyCount={precharge_ready_count} "
                                f"cdValidV={current_demand_valid_voltage_count} "
                                f"cdReadyCount={current_demand_ready_count} "
                                f"lastPresentV={last_present_v if last_present_v is not None else -1:.1f} "
                                f"lastStatus={last_status or '-'}",
                                flush=True,
                            )
                            break
                        print(
                            f"[SESSION {session_index}] charging window reached {charge_elapsed:.1f}s "
                            f"(CurrentDemand count={cd_count}), stopping session",
                            flush=True,
                        )
                        self._request_stop()
                        phase = "stop"
                elif phase == "stop":
                    if self._stop_verification_ok():
                        self._record_session(session_index)
                        print(
                            f"[SESSION {session_index}] stop verification OK: relay1 open and session idle",
                            flush=True,
                        )
                        if session_index >= self.sessions:
                            success = True
                            break
                        gap_until = now + self.gap_s
                        retry_same_session = False
                        phase = "gap"
                        print(
                            f"[SESSION {session_index}] gap start: hold 100% duty for {self.gap_s:.1f}s before next session",
                            flush=True,
                        )
                    elif self._stop_verify_deadline is not None and now > self._stop_verify_deadline:
                        print(f"[FAIL] session {session_index} stop verification timeout", flush=True)
                        break
                else:
                    ready = self._idle_connected_ready()
                    if ready:
                        if self._gap_ready_since <= 0.0:
                            self._gap_ready_since = now
                    else:
                        self._gap_ready_since = 0.0
                    ready_long_enough = (
                        self._gap_ready_since > 0.0 and (now - self._gap_ready_since) >= self.stable_b1_s
                    )
                    if now >= gap_until and ready_long_enough:
                        if retry_same_session:
                            self._begin_session(session_index)
                            phase = "charge"
                            continue
                        session_index += 1
                        self._session_retry_count = 0
                        self._begin_session(session_index)
                        phase = "charge"

                time.sleep(0.05)

            print(
                f"[SUMMARY] sessions={self.sessions} completed={len(self.session_reports)} "
                f"cpA={self.cp_a_count} cpF={self.cp_f_count}",
                flush=True,
            )
            for report in self.session_reports:
                print(
                    f"[SESSION_SUMMARY] idx={report['index']} prechargeSeen={int(report['precharge_seen'])} "
                    f"powerDeliverySeen={int(report['power_delivery_seen'])} "
                    f"prechargeReadyCount={report['precharge_ready_count']} "
                    f"currentDemandCount={report['current_demand_count']} cdValidV={report['current_demand_valid_v']} "
                    f"cdReadyCount={report['current_demand_ready_count']} presentV={report['present_v']} "
                    f"status={report['status']} cpPhase={report['cp_phase']} duty={report['cp_duty']} "
                    f"cpA={report['cp_a_delta']} cpF={report['cp_f_delta']}",
                    flush=True,
                )
            return 0 if success else 1
        finally:
            self._close()
            print(f"LOG_FILE_DONE={self.log_file}", flush=True)


def main() -> int:
    print(f"[ERR] {LEGACY_MODE2_ERROR}", file=sys.stderr)
    return 2
    ap = argparse.ArgumentParser(description="UART mode 2 multisession validation with EV kept connected")
    ap.add_argument("--port", default="/dev/ttyACM0")
    ap.add_argument("--baud", type=int, default=115200)
    ap.add_argument("--plc-id", type=int, default=1)
    ap.add_argument("--controller-id", type=int, default=1)
    ap.add_argument("--sessions", type=int, default=3)
    ap.add_argument("--duration-min", type=int, default=5)
    ap.add_argument("--gap-s", type=float, default=8.0)
    ap.add_argument("--retry-gap-s", type=float, default=15.0)
    ap.add_argument("--stable-b1-s", type=float, default=6.0)
    ap.add_argument("--max-total-min", type=int, default=25)
    ap.add_argument("--heartbeat-ms", type=int, default=400)
    ap.add_argument("--auth-ttl-ms", type=int, default=3000)
    ap.add_argument("--arm-ms", type=int, default=12000)
    ap.add_argument("--log-file", default="")
    args = ap.parse_args()

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = args.log_file or f"/home/jpi/Desktop/EVSE/plc_firmware/logs/mode2_serial_multisession_{ts}.log"

    runner = MultiSessionSerialRunner(
        port=args.port,
        baud=args.baud,
        duration_s=max(60, args.duration_min * 60),
        max_total_s=max(600, args.max_total_min * 60),
        heartbeat_ms=args.heartbeat_ms,
        auth_ttl_ms=args.auth_ttl_ms,
        arm_ms=args.arm_ms,
        plc_id=args.plc_id,
        controller_id=args.controller_id,
        log_file=log_file,
        sessions=args.sessions,
        gap_s=args.gap_s,
        retry_gap_s=args.retry_gap_s,
        stable_b1_s=args.stable_b1_s,
    )
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
