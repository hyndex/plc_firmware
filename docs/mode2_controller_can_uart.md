# Legacy Mode Note

This file is kept only as an archival note.

The old split between:

- `mode=2`
- `mode=3`
- `controller_supported`
- `controller_managed`

no longer exists as a distinct runtime implementation.

Current firmware behavior:

- there are two effective runtime modes only:
  - `mode=0` `standalone`
  - `mode=1` `external_controller`
- legacy aliases like `2`, `3`, `controller_managed`, and
  `controller_supported` normalize to `mode=1`
- the live controller path is UART / USB CDC with `[SERCTRL]` events
- the old PLC-managed allocation / power-setpoint / CAN-status path is removed

Use these documents instead:

- [README.md](/home/jpi/Desktop/EVSE/plc_firmware/README.md)
- [DEVELOPER_CONTROL_PLANE_API.md](/home/jpi/Desktop/EVSE/plc_firmware/DEVELOPER_CONTROL_PLANE_API.md)
- [controller_uart_router_mode.md](/home/jpi/Desktop/EVSE/plc_firmware/docs/controller_uart_router_mode.md)
