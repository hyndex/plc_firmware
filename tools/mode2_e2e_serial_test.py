#!/usr/bin/env python3
"""
Explicit mode 2 serial entrypoint.

This entrypoint is intentionally disabled. Current mode 2 is the UART-router
split where the PLC does not own module CAN or allocation. Use the UART router
monitor/stub plus a direct controller->module CAN application instead.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys

def main() -> int:
    print(
        "[ERR] tools/mode2_e2e_serial_test.py is obsolete for the current mode-2 UART-router architecture.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
