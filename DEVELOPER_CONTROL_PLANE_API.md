# CB PLC Firmware Control Plane Guide

Updated: 2026-03-14

This document is the implementation-accurate controller contract for the current
firmware in `src/main.cpp`.

For operator-oriented SLAC handling, flash procedure, retry policy, and bench
usage, also read `docs/external_controller_slac_handling.md`.

## 1. Scope

The live controller path is:

- controller to PLC over UART / USB CDC
- controller to power modules over the controller's own CAN path
- PLC to controller over the `[SERCTRL]` serial event stream

This is not the older controller-CAN control plane. The old CAN-only contract,
allocation API, and PLC-managed power-setpoint path are no longer part of the
supported runtime behavior.

## 2. Runtime Modes

- `mode=0` `standalone`
  - PLC owns module control, EV-facing power replies, and Relay1.
- `mode=1` `external_controller`
  - PLC owns CP, SLAC, HLC, and relay execution.
  - Controller owns relay decisions and all module-power decisions.

Legacy config aliases such as `2`, `3`, `controller`, `managed`,
`controller_managed`, `controller_supported`, `uart`, and `uart_router`
normalize to `mode=1`.

## 3. Authority Model

### Standalone

- CP / SLAC / HLC: PLC
- authorization: PLC
- module setpoints: PLC
- EVSE present voltage/current replies: PLC
- Relay1: PLC

### External Controller

- CP / SLAC / HLC socket lifecycle: PLC
- authorization gating: controller via `CTRL AUTH`
- SLAC arm/start gating: controller via `CTRL SLAC`
- EVSE present voltage/current / ready flags: controller via `CTRL FEEDBACK`
- relay state: controller via `CTRL RELAY`
- module setpoints: controller, outside the PLC firmware

Important rule in `external_controller` mode:

- the PLC must not make independent relay decisions from HLC / CP / SLAC state
- `CTRL STOP` changes PLC-side stop/session policy, but relay state still belongs
  to `CTRL RELAY`

## 4. UART Transport

- physical transport: `Serial` over USB CDC
- baud: `115200`
- framing: one ASCII command per line
- parser entry point: `CTRL ...`

The firmware converts each accepted command line into the internal controller
frame format and passes it through the common validation/handler path.

## 5. Supported Commands

### `CTRL HB [timeout_ms]`

- updates controller heartbeat timeout
- marks heartbeat as seen
- emits `[SERCTRL] ACK ...`

### `CTRL AUTH <deny|pending|grant> [ttl_ms]`

- sets controller authorization state
- drives HLC authorization behavior in controller mode

### `CTRL SLAC <disarm|arm|start|abort> [arm_ms]`

- controls whether PWM / SLAC start is permitted
- `arm` grants a lease but clears the start latch
- `start` grants a lease and latches immediate start permission
- `disarm` and `abort` clear the SLAC contract and safe-stop an active session when required

Operational rule:

- for a waiting borrower session, refresh with `CTRL SLAC start`, not `CTRL SLAC arm`
- avoid immediate `disarm` or `abort` on the first failed match attempt; let the PLC hold/retry path run first

### `CTRL RELAY <enable_mask> <state_mask> [hold_ms]`

- explicit controller relay command
- bit mapping:
  - bit0: `Relay1`
  - bit1: `Relay2`
  - bit2: `Relay3`
- only commands with the corresponding bit set in `enable_mask` are applied
- optional `hold_ms` auto-releases the requested relay after the requested hold

### `CTRL FEEDBACK <valid0|1> <ready0|1> <present_v> <present_i> [curr_lim0|1] [volt_lim0|1] [pwr_lim0|1] [stop_notify0|1]`

- provides the EV-facing power-side values used in controller mode
- drives:
  - ready / not-ready reply state
  - present voltage
  - present current
  - current / voltage / power limit flags
  - optional stop-charging notification

### `CTRL STOP <soft|hard|clear> [timeout_ms]`

- controls PLC-side session-stop policy
- clears auth / SLAC readiness on the PLC side
- does not directly change relay state
- use `CTRL RELAY` when the controller wants relay state to change

### `CTRL MODE <mode0|1> <plc_id 1..15> [controller_id 1..15]`

- updates in-memory mode and addressing
- does not persist until `CTRL SAVE`
- flashing a different build target does not override an already-saved NVS mode until reboot with the new saved config

### `CTRL OWNERSHIP <connector_id> <module_addr>`

- updates in-memory connector/module identity
- does not persist until `CTRL SAVE`
- bench default donor identity is now `connector_id=2`, `module_addr=2`

### `CTRL SAVE`

- persists current runtime config to NVS

### `CTRL STATUS`

- prints a one-line summary of current runtime state

### `CTRL RESET`

- resets controller runtime state and clears session-side controller state

## 6. Serial Responses

### ACK

Every accepted controller command produces:

- `[SERCTRL] ACK cmd=0x.. seq=.. status=.. detail0=.. detail1=..`

Current status codes:

- `0` `ACK_OK`
- `1` `ACK_BAD_CRC`
- `2` `ACK_BAD_VERSION`
- `3` `ACK_BAD_TARGET`
- `4` `ACK_BAD_SEQ`
- `5` `ACK_BAD_STATE`
- `6` `ACK_BAD_VALUE`

### Event Stream

The controller should consume these lines:

- `[SERCTRL] LOCAL ...`
  - PLC identity, current mode, and whether PLC module CAN/control are active
- `[SERCTRL] EVT CP ...`
  - CP state, PWM duty, auth/SLAC/energy gates
- `[SERCTRL] EVT SLAC ...`
  - SLAC session state and failure counters
- `[SERCTRL] EVT HLC ...`
  - HLC readiness, auth state, and feedback visibility
- `[SERCTRL] EVT SESSION ...`
  - relay states, stop-state flags, meter Wh, SOC
- `[SERCTRL] EVT BMS ...`
  - latest EV request stage and target voltage/current
- `[SERCTRL] IDENTITY ...`
  - EV MAC and RFID identity events
- `[SERCTRL] EVT RFID ...`
  - RFID taps in controller mode

`EVT BMS` is the main controller input for charging control. It carries the
latest EV-requested stage plus requested voltage/current.

## 7. Session Flow

### Standalone

1. Stable CP starts SLAC.
2. SLAC match starts HLC.
3. HLC authorization is local.
4. `PreChargeReq` drives local module targets and closes Relay1.
5. `CurrentDemandReq` continues local control.
6. Stop paths open Relay1 and shut power down locally.

### External Controller

1. Stable CP is observed by the PLC.
2. Controller provides heartbeat/auth/SLAC permission over UART.
3. PLC advertises PWM and starts SLAC only when controller gating permits it.
4. SLAC match starts HLC on the PLC.
5. Each EV demand update is exported in `[SERCTRL] EVT BMS`.
6. Controller responds with `CTRL FEEDBACK` and explicit `CTRL RELAY`.
7. PLC answers the EV using controller-fed feedback and executes relay commands.

Current intended retry behavior in `mode=1`:

- keep heartbeat/auth/SLAC contract alive
- let the PLC use the same hold and optional E/F retry shape as standalone
- avoid Python-side or controller-side immediate resets on a single failed match

## 8. Removed Legacy Interfaces

The following are intentionally unsupported in the current codebase:

- `CTRL ALLOC ...`
- `CTRL ALLOC1`
- `CTRL POWER ...`
- PLC heartbeat / CP / SLAC / HLC / power / session / BMS status over CAN
- direct controller-CAN polling for the active controller runtime path

If these appear in older notes or scripts, treat them as historical only.

## 9. UART Integrity

There is no end-to-end application checksum on the ASCII UART command line.

What exists today:

- USB CDC transport provides lower-layer integrity and retries
- the firmware computes an internal CRC-8 after parsing the text command and
  injecting it into the internal controller-frame path

That CRC-8 is internal only. It is not transmitted as part of the UART line.

Recommendation:

- current USB CDC deployment: no extra checksum required
- future raw/noisy UART deployment: add explicit framing plus checksum or CRC
