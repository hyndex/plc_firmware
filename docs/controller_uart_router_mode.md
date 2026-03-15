# Controller UART Bridge Mode

Current controller-managed operation is the runtime mode reported as:

- `mode=1`
- `external_controller`

Legacy aliases such as `mode=2`, `mode=3`, `uart_router`, `controller_managed`, and
`controller_supported` are normalized into this same runtime mode by the firmware.

For the detailed SLAC lifecycle, flash-and-save procedure, harness usage, and
warnings, read `docs/external_controller_slac_handling.md`.

## Architecture

In `external_controller` mode the split is:

- `Controller -> PLC` over UART / USB CDC
- `Controller -> modules` over the controller's external CAN path
- PLC owns `CP`, `SLAC`, `HLC`, session decoding, and serial status export
- Controller owns relay decisions and all module-power decisions

The PLC does not run its own module-manager / module-control stack in this mode.
It reports EV demand and session state, and it executes controller commands.

## Relay Ownership

Relay ownership in this mode is controller-side:

- `CTRL RELAY` commands directly drive `Relay1`, `Relay2`, and `Relay3`
- `CTRL STOP` is an explicit controller stop request for PLC-side session policy
- PLC HLC / CP / SLAC events do not autonomously toggle relays in this mode

Bit mapping for `CTRL RELAY`:

- bit0 -> `Relay1`
- bit1 -> `Relay2`
- bit2 -> `Relay3`

## Supported UART Commands

Supported controller commands are plain text, one line per command:

- `CTRL HB [timeout_ms]`
- `CTRL AUTH <deny|pending|grant> [ttl_ms]`
- `CTRL SLAC <disarm|arm|start|abort> [arm_ms]`
- `CTRL RELAY <enable_mask> <state_mask> [hold_ms]`
- `CTRL FEEDBACK <valid0|1> <ready0|1> <present_v> <present_i> [curr_lim0|1] [volt_lim0|1] [pwr_lim0|1] [stop_notify0|1]`
- `CTRL STOP <soft|hard|clear> [timeout_ms]`
- `CTRL MODE <mode0|1> <plc_id 1..15> [controller_id 1..15]`
- `CTRL OWNERSHIP <connector_id> <module_addr>`
- `CTRL SAVE`
- `CTRL STATUS`
- `CTRL RESET`

Removed from the supported UART surface:

- `CTRL ALLOC ...`
- `CTRL ALLOC1`
- `CTRL POWER ...`

Those belonged to the older PLC-owned-module controller flow and are not part of the
current bridge-mode contract.

## UART Event Stream

The PLC emits a structured serial event stream in this mode:

- `[SERCTRL] LOCAL ...`
- `[SERCTRL] EVT CP ...`
- `[SERCTRL] EVT SLAC ...`
- `[SERCTRL] EVT HLC ...`
- `[SERCTRL] EVT SESSION ...`
- `[SERCTRL] EVT BMS ...`
- `[SERCTRL] IDENTITY ...`
- `[SERCTRL] ACK ...`
- `[SERCTRL] EVT RFID ...`

The controller should primarily consume:

- `EVT BMS` for EV-requested voltage/current and HLC stage
- `EVT HLC` for readiness and feedback visibility
- `EVT SESSION` for relay and stop state
- `EVT CP` / `EVT SLAC` for CP and matching progression

## Important Runtime Behavior

- The PLC still gates PWM / SLAC start from controller auth + SLAC commands.
- The PLC still answers ISO 15118 / DIN requests locally.
- EV-facing present voltage/current and readiness come from `CTRL FEEDBACK`.
- The PLC does not compute or command module setpoints in this mode.
- issue one `CTRL SLAC start` per EV attach, then keep only heartbeat/auth alive
- after that single `start`, the PLC owns SLAC retry / hold / E/F recovery internally
- controller-mode SLAC start is now gated by stable B2/PWM timing, matching standalone more closely
- do not immediately `disarm`, `abort`, or `reset` on the first SLAC match failure; let the PLC hold/retry path run first

## Recommended Validation Harness

For pure controller-mode charging validation, use:

```bash
python3 tools/external_controller_single_module_charge_test.py
```

That harness:

- uses one PLC and one module only
- sends one `CTRL SLAC start` per attach
- lets the PLC own SLAC retry
- avoids donor/share complexity

For repeated runs, pin the serial path explicitly and do not run concurrent copies:

```bash
python3 tools/external_controller_single_module_charge_test.py \
  --plc-port /dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_B8:F8:62:4B:C5:20-if00 \
  --plc-id 1 \
  --controller-id 1
```

## UART Integrity

There is no end-to-end application checksum appended to the ASCII UART command line.

What exists today:

- USB CDC / USB transport already has lower-layer integrity and retries
- after parsing a `CTRL ...` line, the firmware internally injects the command into
  the legacy control-frame parser and generates an internal CRC-8 for that in-memory
  frame

That internal CRC is not an end-to-end UART checksum. It only preserves one common
validation path inside the firmware.

Practical guidance:

- for the current USB CDC deployment, an extra UART checksum is not strictly required
- if this is moved to a raw/noisy UART link later, add explicit framing plus checksum
  or CRC on the wire

## Operator Note

The repo now keeps one supported controller-mode Python harness:

```bash
python3 tools/external_controller_single_module_charge_test.py
```

If you need continuous monitoring or relay-only tooling again later, recreate it
around the current mode-1 contract instead of reviving the removed older scripts.
