#!/usr/bin/env python3
"""
Explicit mode 2 CAN entrypoint.

This entrypoint is intentionally disabled. Current mode 2 is the UART-router
split where the PLC does not own module CAN or allocation. Use a direct
controller->module CAN application plus the PLC UART router stream instead.
"""

from __future__ import annotations

import sys

def main() -> int:
    print(
        "[ERR] tools/mode2_e2e_can_test.py is obsolete for the current mode-2 UART-router architecture.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
