#!/usr/bin/env python3
"""
UART-router relay tester.

This script is intentionally narrow:
- switch the target PLC into runtime `mode=1` (`external_controller`)
- close one relay at a time through `CTRL RELAY`
- keep it closed by refreshing the relay hold deadline
- verify state from `CTRL STATUS`
- reopen the relay cleanly on exit

Examples:
  python3 -u tools/relay_manual_test.py --port /dev/ttyACM0 --relay 1
  python3 -u tools/relay_manual_test.py --port /dev/ttyACM1 --relay 2 --hold-s 30
  python3 -u tools/relay_manual_test.py --port /dev/ttyACM0 --relay all --non-interactive --hold-s 5
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import select
import sys
import time
from dataclasses import dataclass
from typing import Optional

try:
    import serial  # type: ignore
except Exception as exc:
    print(f"[ERR] pyserial import failed: {exc}", file=sys.stderr)
    sys.exit(2)


ACK_OK = 0
ACK_CMD_RELAY = 0x17
ACK_CMD_STOP = 0x18

STATUS_RE = re.compile(
    r"\[SERCTRL\] STATUS mode=(\d+)\(([^)]+)\)\s+plc_id=(\d+)\s+connector_id=(\d+)\s+"
    r"controller_id=(\d+)\s+module_addr=0x([0-9A-Fa-f]+)\s+local_group=(\d+)\s+module_id=([^\s]+)\s+"
    r"cp=([A-Z0-9]+)\s+duty=(\d+)\s+hb=(\d+)\s+auth=(\d+)\s+allow_slac=(\d+)\s+allow_energy=(\d+)\s+"
    r"armed=(\d+)\s+start=(\d+)\s+relay1=(\d+)\s+relay2=(\d+)\s+relay3=(\d+)\s+alloc_sz=(\d+)"
)
ACK_RE = re.compile(
    r"\[SERCTRL\] ACK cmd=0x([0-9A-Fa-f]+)\s+seq=(\d+)\s+status=(\d+)\s+detail0=(\d+)\s+detail1=(\d+)"
)
RELAY_EVT_RE = re.compile(r"\[RELAY\] Relay(\d+) -> (OPEN|CLOSED)")


@dataclass
class PlcStatus:
    mode_id: int
    mode_name: str
    plc_id: int
    connector_id: int
    controller_id: int
    cp: str
    duty: int
    relay1: int
    relay2: int
    relay3: int
    alloc_sz: int

    def relay_state(self, relay_idx: int) -> int:
        return getattr(self, f"relay{relay_idx}")


@dataclass
class Ack:
    cmd_hex: int
    seq: int
    status: int
    detail0: int
    detail1: int


class RelayManualTester:
    def __init__(
        self,
        port: str,
        baud: int,
        controller_id: int,
        hold_s: int,
        interactive: bool,
        log_file: str,
        refresh_ms: int,
        status_interval_s: float,
    ) -> None:
        self.port = port
        self.baud = baud
        self.controller_id = controller_id
        self.hold_s = max(0, hold_s)
        self.interactive = interactive
        self.log_file = log_file
        self.refresh_ms = max(500, refresh_ms)
        self.status_interval_s = max(1.0, status_interval_s)

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

    def wait_for_ack(self, cmd_hex: int, timeout_s: float = 2.0) -> Ack:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            line = self._readline()
            if not line:
                continue
            match = ACK_RE.search(line)
            if not match:
                continue
            ack = Ack(
                cmd_hex=int(match.group(1), 16),
                seq=int(match.group(2)),
                status=int(match.group(3)),
                detail0=int(match.group(4)),
                detail1=int(match.group(5)),
            )
            if ack.cmd_hex == cmd_hex:
                return ack
        raise RuntimeError(f"timeout waiting for ACK 0x{cmd_hex:02X}")

    def wait_for_status(self, timeout_s: float = 2.0) -> PlcStatus:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            line = self._readline()
            if not line:
                continue
            match = STATUS_RE.search(line)
            if not match:
                continue
            return PlcStatus(
                mode_id=int(match.group(1)),
                mode_name=match.group(2),
                plc_id=int(match.group(3)),
                connector_id=int(match.group(4)),
                controller_id=int(match.group(5)),
                cp=match.group(9),
                duty=int(match.group(10)),
                relay1=int(match.group(17)),
                relay2=int(match.group(18)),
                relay3=int(match.group(19)),
                alloc_sz=int(match.group(20)),
            )
        raise RuntimeError("timeout waiting for CTRL STATUS")

    def query_status(self) -> PlcStatus:
        self.send("CTRL STATUS")
        status = self.wait_for_status()
        print(
            f"[STATUS] mode={status.mode_name} plc_id={status.plc_id} connector_id={status.connector_id} "
            f"cp={status.cp} duty={status.duty} relay1={status.relay1} relay2={status.relay2} relay3={status.relay3}",
            flush=True,
        )
        return status

    def wait_for_relay_event(self, relay_idx: int, closed: bool, timeout_s: float = 2.0) -> bool:
        deadline = time.time() + timeout_s
        target = "CLOSED" if closed else "OPEN"
        while time.time() < deadline:
            line = self._readline()
            if not line:
                continue
            match = RELAY_EVT_RE.search(line)
            if match and int(match.group(1)) == relay_idx and match.group(2) == target:
                return True
        return False

    def ensure_router_mode(self) -> PlcStatus:
        status = self.query_status()
        if status.mode_id != 1 or status.controller_id != self.controller_id:
            self.send(f"CTRL MODE 1 {status.plc_id} {self.controller_id}")
            time.sleep(0.3)
            status = self.query_status()
        self.send("CTRL STOP clear 3000")
        ack = self.wait_for_ack(ACK_CMD_STOP)
        if ack.status != ACK_OK:
            raise RuntimeError(f"STOP clear rejected status={ack.status}")
        status = self.query_status()
        if status.mode_id != 1:
            raise RuntimeError("PLC did not enter external_controller mode")
        return status

    def set_relay(self, relay_idx: int, closed: bool, hold_ms: int) -> PlcStatus:
        mask = 1 << (relay_idx - 1)
        state_mask = mask if closed else 0
        self.send(f"CTRL RELAY {mask} {state_mask} {max(0, hold_ms)}")
        ack = self.wait_for_ack(ACK_CMD_RELAY)
        if ack.status != ACK_OK:
            raise RuntimeError(
                f"Relay{relay_idx} command rejected status={ack.status} "
                f"(detail0={ack.detail0} detail1={ack.detail1})"
            )
        self.wait_for_relay_event(relay_idx, closed)
        status = self.query_status()
        actual = status.relay_state(relay_idx)
        expected = 1 if closed else 0
        if actual != expected:
            raise RuntimeError(f"Relay{relay_idx} state mismatch expected={expected} actual={actual}")
        return status

    def hold_closed(self, relay_idx: int) -> None:
        deadline = time.time() + self.hold_s
        last_refresh = 0.0
        last_status = 0.0
        last_remaining: Optional[int] = None

        while True:
            now = time.time()
            if now >= deadline and not self.interactive:
                return

            if now - last_refresh >= max(1.0, (self.refresh_ms / 1000.0) * 0.4):
                self.set_relay(relay_idx, True, self.refresh_ms)
                last_refresh = now

            if now - last_status >= self.status_interval_s:
                status = self.query_status()
                if status.relay_state(relay_idx) != 1:
                    raise RuntimeError(f"Relay{relay_idx} reopened during hold window")
                last_status = now

            remaining = max(0, int(deadline - now))
            if remaining != last_remaining and remaining % 5 == 4:
                print(f"[HOLD] Relay{relay_idx} remains closed for {remaining}s", flush=True)
                last_remaining = remaining

            if self.interactive:
                ready, _, _ = select.select([sys.stdin], [], [], 0.5)
                if ready:
                    sys.stdin.readline()
                    return
            else:
                time.sleep(0.4)

    def run_single_relay(self, relay_idx: int) -> None:
        print(f"\n=== Testing Relay{relay_idx} on {self.port} ===", flush=True)
        status = self.ensure_router_mode()
        print(
            f"[READY] mode={status.mode_name} plc_id={status.plc_id} connector_id={status.connector_id} cp={status.cp}",
            flush=True,
        )

        if self.interactive:
            input(f"Press Enter to CLOSE Relay{relay_idx} on {self.port}...")

        status = self.set_relay(relay_idx, True, self.refresh_ms)
        print(
            f"[CLOSED] Relay{relay_idx} on {self.port} is now closed. "
            f"Check continuity now with the multimeter. status cp={status.cp}",
            flush=True,
        )

        if self.interactive:
            print(f"Press Enter after measurement to OPEN Relay{relay_idx} on {self.port}...", flush=True)
        self.hold_closed(relay_idx)

        status = self.set_relay(relay_idx, False, 0)
        print(
            f"[OPEN] Relay{relay_idx} on {self.port} is now open. "
            f"status relay1={status.relay1} relay2={status.relay2} relay3={status.relay3}",
            flush=True,
        )

    def open_all_relays(self) -> None:
        for relay_idx in (1, 2, 3):
            try:
                self.set_relay(relay_idx, False, 0)
            except Exception:
                pass

    def run(self, relay_arg: str) -> int:
        relays = [1, 2, 3] if relay_arg == "all" else [int(relay_arg)]
        self.open()
        print(f"LOG_FILE={self.log_file}", flush=True)
        try:
            for relay_idx in relays:
                self.run_single_relay(relay_idx)
            return 0
        finally:
            self.open_all_relays()
            self.close()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="UART-router relay tester")
    ap.add_argument("--port", required=True, help="serial port for the PLC, for example /dev/ttyACM0")
    ap.add_argument("--baud", type=int, default=115200)
    ap.add_argument("--relay", choices=("1", "2", "3", "all"), required=True)
    ap.add_argument("--controller-id", type=int, default=1)
    ap.add_argument("--hold-s", type=int, default=300)
    ap.add_argument("--refresh-ms", type=int, default=10000, help="relay hold refresh command duration")
    ap.add_argument("--status-interval-s", type=float, default=4.0)
    ap.add_argument("--interactive", action="store_true", default=True)
    ap.add_argument("--non-interactive", dest="interactive", action="store_false")
    ap.add_argument("--log-file", default="")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = args.log_file or (
        f"/home/jpi/Desktop/EVSE/plc_firmware/logs/relay_manual_test_{ts}_{os.path.basename(args.port)}.log"
    )
    tester = RelayManualTester(
        port=args.port,
        baud=args.baud,
        controller_id=args.controller_id,
        hold_s=args.hold_s,
        interactive=args.interactive,
        log_file=log_file,
        refresh_ms=args.refresh_ms,
        status_interval_s=args.status_interval_s,
    )
    try:
        return tester.run(args.relay)
    except KeyboardInterrupt:
        print("\n[STOP] interrupted by user", flush=True)
        return 130
    except Exception as exc:
        print(f"[FAIL] {exc}", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
