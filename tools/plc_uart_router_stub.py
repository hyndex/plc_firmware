#!/usr/bin/env python3
"""
Mode 1 / external_controller serial stub for the PLC control plane.

This script only speaks to the PLC over UART:
  - heartbeat
  - auth
  - SLAC arm/start
  - relay control
  - HLC feedback
  - status polling

It does not send ALLOC or module power ownership to the PLC. In mode 1 the
controller is expected to drive modules directly over CAN outside this process.
"""

from __future__ import annotations

import argparse
import re
import sys
import threading
import time
from dataclasses import dataclass
from typing import Optional

try:
    import serial  # type: ignore
except Exception as exc:
    print(f"[ERR] pyserial import failed: {exc}", file=sys.stderr)
    sys.exit(2)


BMS_RE = re.compile(
    r"\[SERCTRL\] EVT BMS stage=(\d+)\(([^)]+)\) valid=(\d+) delivery_ready=(\d+) "
    r"target_v=([0-9.+-]+) target_i=([0-9.+-]+)"
)
CP_RE = re.compile(r"\[SERCTRL\] EVT CP cp=([A-Z0-9]+) duty=(\d+).*allow_slac=(\d+).*allow_energy=(\d+)")
STATUS_RE = re.compile(r"\[SERCTRL\] STATUS .*mode=(\d+)\(([^)]+)\).*relay1=(\d).*relay2=(\d).*relay3=(\d)")


@dataclass
class Snapshot:
    stage_id: int = 0
    stage_name: str = "None"
    target_v: float = 0.0
    target_i: float = 0.0
    bms_valid: bool = False
    delivery_ready: bool = False
    cp: str = "U"
    duty_pct: int = -1
    allow_slac: bool = False
    allow_energy: bool = False
    mode_id: int = -1
    mode_name: str = ""
    relay1: int = -1
    relay2: int = -1
    relay3: int = -1


class Runner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.snapshot = Snapshot()
        self._ser: Optional[serial.Serial] = None
        self._stop = threading.Event()
        self._reader: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._last_write_ts = 0.0
        self._slac_started = False

    def _write(self, cmd: str) -> None:
        if not self._ser:
            raise RuntimeError("serial port not open")
        now = time.time()
        wait_s = 0.04 - (now - self._last_write_ts)
        if wait_s > 0:
            time.sleep(wait_s)
        self._ser.write((cmd.strip() + "\n").encode("utf-8", errors="ignore"))
        self._ser.flush()
        self._last_write_ts = time.time()

    def _reader_loop(self) -> None:
        assert self._ser is not None
        while not self._stop.is_set():
            raw = self._ser.readline()
            if not raw:
                continue
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            print(line, flush=True)
            self._parse_line(line)

    def _parse_line(self, line: str) -> None:
        with self._lock:
            m = BMS_RE.search(line)
            if m:
                self.snapshot.stage_id = int(m.group(1))
                self.snapshot.stage_name = m.group(2)
                self.snapshot.bms_valid = m.group(3) == "1"
                self.snapshot.delivery_ready = m.group(4) == "1"
                self.snapshot.target_v = float(m.group(5))
                self.snapshot.target_i = float(m.group(6))
                return
            m = CP_RE.search(line)
            if m:
                self.snapshot.cp = m.group(1)
                self.snapshot.duty_pct = int(m.group(2))
                self.snapshot.allow_slac = m.group(3) == "1"
                self.snapshot.allow_energy = m.group(4) == "1"
                return
            m = STATUS_RE.search(line)
            if m:
                self.snapshot.mode_id = int(m.group(1))
                self.snapshot.mode_name = m.group(2)
                self.snapshot.relay1 = int(m.group(3))
                self.snapshot.relay2 = int(m.group(4))
                self.snapshot.relay3 = int(m.group(5))

    def run(self) -> int:
        self._ser = serial.Serial(self.args.port, self.args.baud, timeout=0.1, write_timeout=0.5)
        self._ser.reset_input_buffer()
        self._ser.reset_output_buffer()
        self._reader = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader.start()

        try:
            self._write(f"CTRL MODE 1 {self.args.plc_id} {self.args.controller_id}")
            self._write("CTRL STATUS")

            start = time.time()
            last_hb = 0.0
            last_auth = 0.0
            last_slac = 0.0
            last_feedback = 0.0
            last_relay = 0.0
            last_status = 0.0
            last_summary = 0.0

            while (time.time() - start) < self.args.duration_s:
                now = time.time()
                if now - last_hb >= (self.args.heartbeat_ms / 1000.0):
                    self._write(f"CTRL HB {self.args.heartbeat_timeout_ms}")
                    last_hb = now
                if now - last_auth >= 1.0:
                    self._write(f"CTRL AUTH {self.args.auth_state} {self.args.auth_ttl_ms}")
                    last_auth = now
                if self.args.auto_slac and now - last_slac >= 2.0:
                    self._write(f"CTRL SLAC arm {self.args.slac_arm_ms}")
                    if self.args.start_slac and not self._slac_started:
                        self._write(f"CTRL SLAC start {self.args.slac_arm_ms}")
                        self._slac_started = True
                    last_slac = now
                if self.args.feedback and now - last_feedback >= self.args.feedback_period_s:
                    self._write(
                        f"CTRL FEEDBACK {1 if self.args.feedback_valid else 0} "
                        f"{1 if self.args.feedback_ready else 0} "
                        f"{self.args.present_v:.1f} {self.args.present_i:.1f} "
                        f"{1 if self.args.current_limit else 0} "
                        f"{1 if self.args.voltage_limit else 0} "
                        f"{1 if self.args.power_limit else 0}"
                    )
                    last_feedback = now
                if self.args.relay_enable_mask and now - last_relay >= self.args.relay_period_s:
                    self._write(
                        f"CTRL RELAY {self.args.relay_enable_mask} {self.args.relay_state_mask} {self.args.relay_hold_ms}"
                    )
                    last_relay = now
                if now - last_status >= self.args.status_period_s:
                    self._write("CTRL STATUS")
                    last_status = now
                if now - last_summary >= 5.0:
                    with self._lock:
                        snap = Snapshot(**vars(self.snapshot))
                    print(
                        f"[HOST] stage={snap.stage_id}({snap.stage_name}) bms_valid={int(snap.bms_valid)} "
                        f"target={snap.target_v:.1f}V/{snap.target_i:.1f}A cp={snap.cp} duty={snap.duty_pct} "
                        f"allow_slac={int(snap.allow_slac)} allow_energy={int(snap.allow_energy)} "
                        f"relays={snap.relay1}/{snap.relay2}/{snap.relay3}",
                        flush=True,
                    )
                    last_summary = now
                time.sleep(0.05)
        finally:
            self._stop.set()
            if self._reader and self._reader.is_alive():
                self._reader.join(timeout=1.0)
            if self._ser:
                try:
                    self._ser.close()
                except Exception:
                    pass
        return 0


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Mode 1 PLC external-controller UART control stub")
    ap.add_argument("--port", default="/dev/ttyACM0")
    ap.add_argument("--baud", type=int, default=115200)
    ap.add_argument("--plc-id", type=int, default=1)
    ap.add_argument("--controller-id", type=int, default=1)
    ap.add_argument("--duration-s", type=int, default=60)
    ap.add_argument("--heartbeat-ms", type=int, default=800)
    ap.add_argument("--heartbeat-timeout-ms", type=int, default=3000)
    ap.add_argument("--auth-state", default="grant", choices=("deny", "pending", "grant"))
    ap.add_argument("--auth-ttl-ms", type=int, default=3000)
    ap.add_argument("--auto-slac", action="store_true", default=True)
    ap.add_argument("--start-slac", action="store_true", default=True)
    ap.add_argument("--slac-arm-ms", type=int, default=12000)
    ap.add_argument("--feedback", action="store_true", default=True)
    ap.add_argument("--feedback-valid", action="store_true", default=True)
    ap.add_argument("--feedback-ready", action="store_true", default=False)
    ap.add_argument("--present-v", type=float, default=0.0)
    ap.add_argument("--present-i", type=float, default=0.0)
    ap.add_argument("--current-limit", action="store_true", default=False)
    ap.add_argument("--voltage-limit", action="store_true", default=False)
    ap.add_argument("--power-limit", action="store_true", default=False)
    ap.add_argument("--feedback-period-s", type=float, default=0.25)
    ap.add_argument("--relay-enable-mask", type=int, default=0)
    ap.add_argument("--relay-state-mask", type=int, default=0)
    ap.add_argument("--relay-hold-ms", type=int, default=1500)
    ap.add_argument("--relay-period-s", type=float, default=0.8)
    ap.add_argument("--status-period-s", type=float, default=2.0)
    return ap.parse_args()


def main() -> int:
    return Runner(parse_args()).run()


if __name__ == "__main__":
    raise SystemExit(main())
