#!/usr/bin/env python3
"""
UART-router PLC monitor and keepalive helper.

This is the controller-side serial companion for `mode=2`
(`controller_uart_router`):
- puts the PLC into UART-router mode
- keeps the PLC controller contract alive with `CTRL HB`
- maintains auth state with `CTRL AUTH`
- polls `CTRL STATUS`
- prints the structured `SERCTRL LOCAL` / `SERCTRL EVT ...` stream

It does not drive modules. In this architecture, the external controller owns
module CAN directly and the PLC only exposes HLC/CP/session/relay state over
UART plus accepts relay/auth/SLAC/feedback commands over UART.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from typing import Optional

try:
    import serial  # type: ignore
except Exception as exc:
    print(f"[ERR] pyserial import failed: {exc}", file=sys.stderr)
    sys.exit(2)


class UartRouterMonitor:
    def __init__(
        self,
        port: str,
        baud: int,
        plc_id: int,
        controller_id: int,
        heartbeat_timeout_ms: int,
        auth_state: str,
        auth_ttl_ms: int,
        status_interval_s: float,
        log_file: str,
    ) -> None:
        self.port = port
        self.baud = baud
        self.plc_id = plc_id
        self.controller_id = controller_id
        self.heartbeat_timeout_ms = max(500, heartbeat_timeout_ms)
        self.auth_state = auth_state
        self.auth_ttl_ms = max(500, auth_ttl_ms)
        self.status_interval_s = max(0.5, status_interval_s)
        self.log_file = log_file

        self.ser: Optional[serial.Serial] = None
        self.log_fp = None
        self.last_write_ts = 0.0

    def open(self) -> None:
        parent = os.path.dirname(self.log_file)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.log_fp = open(self.log_file, "w", encoding="utf-8", buffering=1)
        kwargs = {
            "port": self.port,
            "baudrate": self.baud,
            "timeout": 0.1,
            "write_timeout": 0.5,
        }
        try:
            self.ser = serial.Serial(exclusive=True, **kwargs)
        except TypeError:
            self.ser = serial.Serial(**kwargs)
        self.ser.reset_input_buffer()
        self.ser.reset_output_buffer()

    def close(self) -> None:
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

    def _readline(self) -> str:
        if not self.ser:
            raise RuntimeError("serial port not open")
        raw = self.ser.readline()
        if not raw:
            return ""
        line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        self._log(f"{dt.datetime.now().isoformat(timespec='milliseconds')} {line}")
        return line

    def send(self, cmd: str) -> None:
        if not self.ser:
            raise RuntimeError("serial port not open")
        wait_s = 0.04 - (time.time() - self.last_write_ts)
        if wait_s > 0:
            time.sleep(wait_s)
        payload = (cmd.strip() + "\n").encode("utf-8", errors="ignore")
        self.ser.write(payload)
        self.ser.flush()
        self.last_write_ts = time.time()
        self._log(f"{dt.datetime.now().isoformat(timespec='milliseconds')} > {cmd}")
        print(f"[TX] {cmd}", flush=True)

    def bootstrap(self) -> None:
        self.send("CTRL STATUS")
        time.sleep(0.1)
        self.send(f"CTRL MODE 2 {self.plc_id} {self.controller_id}")
        time.sleep(0.2)
        self.send("CTRL STOP clear 3000")
        time.sleep(0.1)
        self.send(f"CTRL HB {self.heartbeat_timeout_ms}")
        time.sleep(0.05)
        self.send(f"CTRL AUTH {self.auth_state} {self.auth_ttl_ms}")
        time.sleep(0.05)
        self.send("CTRL STATUS")

    def run(self) -> int:
        last_hb = 0.0
        last_auth = 0.0
        last_status = 0.0
        try:
            self.open()
            print(f"LOG_FILE={self.log_file}", flush=True)
            self.bootstrap()
            while True:
                now = time.time()
                if (now - last_hb) >= max(0.4, (self.heartbeat_timeout_ms / 1000.0) / 2.0):
                    self.send(f"CTRL HB {self.heartbeat_timeout_ms}")
                    last_hb = now
                if (now - last_auth) >= max(0.5, self.auth_ttl_ms / 3000.0):
                    self.send(f"CTRL AUTH {self.auth_state} {self.auth_ttl_ms}")
                    last_auth = now
                if (now - last_status) >= self.status_interval_s:
                    self.send("CTRL STATUS")
                    last_status = now

                line = self._readline()
                if not line:
                    continue
                if "[SERCTRL]" in line or "[HLC]" in line or "[CP]" in line:
                    print(line, flush=True)
        except KeyboardInterrupt:
            print("[INFO] interrupted", flush=True)
            return 0
        finally:
            self.close()


def parse_args() -> argparse.Namespace:
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    ap = argparse.ArgumentParser(description="UART-router PLC monitor and keepalive helper")
    ap.add_argument("--port", default="/dev/ttyACM0")
    ap.add_argument("--baud", type=int, default=115200)
    ap.add_argument("--plc-id", type=int, default=1)
    ap.add_argument("--controller-id", type=int, default=1)
    ap.add_argument("--heartbeat-timeout-ms", type=int, default=3000)
    ap.add_argument("--auth-state", choices=["deny", "pending", "grant"], default="grant")
    ap.add_argument("--auth-ttl-ms", type=int, default=3000)
    ap.add_argument("--status-interval-s", type=float, default=2.0)
    ap.add_argument(
        "--log-file",
        default=f"/home/jpi/Desktop/EVSE/plc_firmware/logs/uart_router_monitor_{ts}.log",
    )
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    runner = UartRouterMonitor(
        port=args.port,
        baud=args.baud,
        plc_id=args.plc_id,
        controller_id=args.controller_id,
        heartbeat_timeout_ms=args.heartbeat_timeout_ms,
        auth_state=args.auth_state,
        auth_ttl_ms=args.auth_ttl_ms,
        status_interval_s=args.status_interval_s,
        log_file=args.log_file,
    )
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
