# Controller UART Router Mode

`mode=2` is the direct-controller split:

- `Controller -> PLC` over `UART`
- `Controller -> Modules` over external `CAN`
- the PLC stays responsible for `CP`, `SLAC`, `HLC`, and physical relays
- the PLC does not start its local MCP2515/module-manager path in this mode

## What Changes In `mode=2`

- PLC-side module CAN is disabled at boot.
- PLC-side `cbmodules::ModuleManager` is not initialized.
- PLC does not poll controller CAN RX frames.
- PLC does not publish PLC controller-status CAN frames.
- `CTRL ALLOC ...` is rejected.
- `CTRL POWER ...` is rejected.
- `CTRL FEEDBACK ...` remains valid and is what drives EV-facing HLC readiness / present V/I.
- `CTRL RELAY ...` remains valid and directly controls Relay1/2/3.
- `CTRL AUTH ...`, `CTRL SLAC ...`, `CTRL STOP ...`, `CTRL STATUS`, and `CTRL MODE` remain valid.

## UART Event Stream

When the PLC is in `mode=2`, it emits a stable serial event stream:

- `[SERCTRL] LOCAL ...`
  - one-line identity/config summary
  - includes mode, PLC ID, connector ID, controller ID, local module identity, and whether PLC CAN/module-manager are enabled

- `[SERCTRL] EVT CP ...`
  - CP phase/duty
  - controller heartbeat/auth view
  - SLAC-start permission
  - energy permission

- `[SERCTRL] EVT SLAC ...`
  - session-started
  - matched
  - FSM state
  - failure count

- `[SERCTRL] EVT HLC ...`
  - HLC ready/active
  - auth state
  - feedback valid/ready
  - feedback present V/I

- `[SERCTRL] EVT SESSION ...`
  - session/match state
  - Relay1/2/3 state
  - stop state
  - meter Wh
  - EV SOC if known

- `[SERCTRL] EVT BMS ...`
  - HLC stage
  - target voltage/current requested by the EV
  - delivery-ready flag
  - latest controller feedback snapshot

- `[SERCTRL] EVT ID ...`
  - `EVMAC`, `EVCCID`, or `EMAID` when available

## Intended Controller Behavior

The external controller should:

1. Put the PLC into `mode=2`.
2. Keep `CTRL HB` alive.
3. Drive `CTRL AUTH` and `CTRL SLAC`.
4. Read `[SERCTRL] EVT BMS ...` and `[SERCTRL] EVT HLC ...` to learn requested EV targets and session state.
5. Control modules directly over its own CAN path.
6. Feed the live bus result back to the PLC with `CTRL FEEDBACK ...`.
7. Close/open relays with `CTRL RELAY ...`.

## Helper Tool

Use [uart_router_serial_monitor.py](/home/jpi/Desktop/EVSE/plc_firmware/tools/uart_router_serial_monitor.py) as the basic keepalive/status monitor for this mode:

```bash
python3 -u tools/uart_router_serial_monitor.py --port /dev/ttyACM0 --plc-id 1 --controller-id 1
```

It does not control modules. It only keeps the PLC UART contract alive and surfaces the serial event stream.
