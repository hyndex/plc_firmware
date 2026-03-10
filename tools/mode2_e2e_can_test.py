#!/usr/bin/env python3
"""
Explicit mode 2 CAN entrypoint.

This forwards to the CAN-driven mode 2 harness implementation in
`tools/mode2_e2e_test.py`.
"""

from __future__ import annotations

import sys

from mode2_e2e_test import main


if __name__ == "__main__":
    sys.exit(main())
