#!/usr/bin/env python3
"""
Explicit mode 2 serial entrypoint.

This wraps the existing serial-managed controller runner and fixes the mode to
`managed` so it is used specifically for mode 2 serial-path validation.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys

from controller_charging_test import ControllerChargingRunner


def main() -> int:
    ap = argparse.ArgumentParser(description="Live mode 2 end-to-end validation over the serial controller bridge")
    ap.add_argument("--port", default="/dev/ttyACM0")
    ap.add_argument("--baud", type=int, default=115200)
    ap.add_argument("--plc-id", type=int, default=1)
    ap.add_argument("--controller-id", type=int, default=1)
    ap.add_argument("--duration-min", type=int, default=1)
    ap.add_argument("--max-total-min", type=int, default=8)
    ap.add_argument("--heartbeat-ms", type=int, default=400)
    ap.add_argument("--auth-ttl-ms", type=int, default=3000)
    ap.add_argument("--arm-ms", type=int, default=12000)
    ap.add_argument("--log-file", default="")
    args = ap.parse_args()

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = args.log_file or f"/home/jpi/Desktop/EVSE/plc_firmware/logs/mode2_serial_e2e_{ts}.log"

    runner = ControllerChargingRunner(
        port=args.port,
        baud=args.baud,
        duration_s=max(60, args.duration_min * 60),
        max_total_s=max(180, args.max_total_min * 60),
        heartbeat_ms=args.heartbeat_ms,
        auth_ttl_ms=args.auth_ttl_ms,
        arm_ms=args.arm_ms,
        mode="managed",
        plc_id=args.plc_id,
        controller_id=args.controller_id,
        log_file=log_file,
    )
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
