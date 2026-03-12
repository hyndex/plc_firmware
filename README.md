# CB PLC Firmware (ESP32-S3)

Integrated EVSE firmware built from:

- `cbslac` for SLAC matching over QCA7000
- `jpv2g` for ISO 15118 / DIN 70121 HLC SECC handling
- `cbmodules` for local DC power-module control in standalone mode

This README describes the current firmware behavior end to end.

## Documentation

- `README.md`
  Current runtime architecture and charging flow
- `DEVELOPER_CONTROL_PLANE_API.md`
  Current UART bridge contract and controller-facing events
- `docs/controller_uart_router_mode.md`
  Short controller-mode reference
- `docs/mode2_controller_can_uart.md`
  Legacy-mode archival note

## Runtime Model

Each PLC is one EVSE endpoint. The PLC always owns:

- CP sampling, debounce, and PWM advertisement
- QCA7000 transport and SLAC state machine
- HLC server lifecycle and EV request decoding
- relay output execution
- EV-facing session state

The firmware has two effective runtime modes:

- `mode=0` `standalone`
- `mode=1` `external_controller`

Legacy inputs such as `2`, `3`, `controller`, `managed`, `controller_managed`,
`controller_supported`, `uart`, and `uart_router` are still accepted for config
compatibility, but they normalize to `mode=1`.

## Mode Behavior

### `mode=0` Standalone

The PLC is autonomous:

- CP connect can advertise PWM and start SLAC
- HLC authorization is local
- module targets come directly from HLC requests
- Relay1 is PLC-owned
- module CAN and module-manager stay active on the PLC

End-to-end path:

- `CP -> SLAC -> HLC -> local module control -> Relay1`

### `mode=1` External Controller

The PLC becomes a UART bridge and EV-protocol endpoint:

- `Controller -> PLC` over UART / USB CDC
- `Controller -> modules` over the controller's own CAN path
- PLC keeps CP / SLAC / HLC and exports EV demand back to the controller
- controller owns relay decisions
- controller owns all module-power decisions
- PLC module CAN / module-manager are bypassed in this mode

End-to-end path:

- `CP -> SLAC -> HLC on PLC`
- `EV demand/status -> [SERCTRL] UART events`
- `controller feedback/relay commands -> CTRL ... UART commands`

Important relay rule in `mode=1`:

- the PLC does not autonomously open or close relays because of HLC / CP / SLAC transitions
- relay changes happen only from explicit controller commands such as `CTRL RELAY`
- relay hold expiry also follows the controller-provided relay command timeout

## Boot Sequence

On every boot the firmware:

1. starts serial logging
2. loads runtime config from NVS and normalizes legacy mode aliases
3. refreshes local identity derived from `plc_id` and `module_addr`
4. reads the PLC MAC address
5. opens all relays
6. optionally starts the SW4 setup portal
7. configures CP PWM
8. initializes either:
   - local module manager in `standalone`
   - or controller-bridge mode with module CAN/control disabled in `external_controller`
9. initializes QCA7000 / SLAC
10. enters the fixed loop

Main loop order:

1. serial commands
2. QCA ingress
3. controller RX dispatch
4. controller watchdogs / stop policy
5. controller UART status/event TX
6. CP / SLAC / HLC progression
7. module-manager service only in standalone mode

## End-to-End Charging Flow

### Standalone

1. EV plugs in and CP becomes stable.
2. PLC advertises PWM and starts SLAC.
3. On SLAC match the PLC starts HLC.
4. HLC auth is handled locally.
5. `PreChargeReq` applies local voltage/current targets and closes Relay1.
6. `CurrentDemandReq` keeps updating the local module target.
7. `PowerDelivery stop`, `SessionStop`, disconnect, or HLC teardown shut power down locally.

### External Controller

1. EV plugs in and CP becomes stable.
2. Controller sends `CTRL HB`, `CTRL AUTH`, and `CTRL SLAC`.
3. PLC advertises PWM / starts SLAC only when controller gating allows it.
4. On SLAC match the PLC starts HLC.
5. Each EV demand update is emitted on the `[SERCTRL]` event stream.
6. Controller responds with:
   - `CTRL FEEDBACK ...` for EV-facing ready/present values
   - `CTRL RELAY ...` for Relay1/2/3 state
   - optional `CTRL STOP ...` for session stop policy
7. PLC answers the EV using controller-fed feedback, but does not compute module setpoints itself.

## UART Control Surface

Supported controller commands:

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

Controller-facing status/event stream:

- `[SERCTRL] LOCAL ...`
- `[SERCTRL] EVT CP ...`
- `[SERCTRL] EVT SLAC ...`
- `[SERCTRL] EVT HLC ...`
- `[SERCTRL] EVT SESSION ...`
- `[SERCTRL] EVT BMS ...`
- `[SERCTRL] IDENTITY ...`
- `[SERCTRL] EVT RFID ...`
- `[SERCTRL] ACK ...`

Removed from the supported interface:

- `CTRL ALLOC ...`
- `CTRL ALLOC1`
- `CTRL POWER ...`
- old PLC-to-controller CAN status publications
- old controller-CAN ingress path

## UART Integrity

There is no end-to-end application checksum on the ASCII UART command line.

What exists today:

- USB CDC / USB transport already provides lower-layer integrity and retries
- after parsing a `CTRL ...` line, the firmware injects the command into the
  internal controller-frame parser and computes an internal CRC-8 there

That CRC-8 is internal parser compatibility only. It is not an on-wire UART
checksum.

Practical guidance:

- for the current USB CDC deployment, an extra checksum is not required
- for any future raw or noisy UART link, add explicit framing plus checksum or CRC

## Current Non-Goals

These old paths are no longer part of the supported firmware:

- PLC-managed controller allocation transactions
- PLC-managed controller power setpoints
- PLC-to-controller CAN status uplinks
- separate runtime semantics for legacy `mode=2` or `mode=3`
