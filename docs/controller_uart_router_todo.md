# Controller UART Router TODO

## Stage 1: Architecture Split
- [completed] Confirm current `mode=2` flow still booted PLC-side CAN/module-manager and therefore did not match the required split
- [completed] Define a dedicated UART-router runtime mode (`mode=2`)
- [completed] Keep PLC responsibilities limited to CP/SLAC/HLC/relay handling in that mode

## Stage 2: Firmware Gating
- [completed] Disable PLC-side MCP2515/module-manager startup in `mode=2`
- [completed] Disable PLC controller-plane CAN polling/transmit in `mode=2`
- [completed] Reject `CTRL ALLOC ...` in `mode=2`
- [completed] Reject `CTRL POWER ...` in `mode=2`
- [completed] Keep `CTRL FEEDBACK ...` active in `mode=2`
- [completed] Keep `CTRL RELAY ...` active in `mode=2`

## Stage 3: UART Telemetry
- [completed] Restore a structured serial status/event stream for router mode
- [completed] Publish PLC local identity over serial in router mode
- [completed] Publish EV identity (`EVMAC` / `EVCCID` / `EMAID`) over serial in router mode
- [completed] Publish EV-requested BMS target voltage/current over serial in router mode
- [completed] Publish CP/SLAC/HLC/session status over serial in router mode

## Stage 4: Tooling
- [completed] Retarget the relay tester to `mode=2`
- [completed] Add a standalone UART-router monitor/keepalive script
- [pending] Retire or explicitly deprecate the old mode-2 PLC-owned charging harnesses
- [pending] Add a direct external module-controller example that consumes `SERCTRL EVT ...` and drives modules over CAN

## Stage 5: Verification
- [completed] `py_compile` the updated Python tools
- [completed] Build `controller-plc1`
- [completed] Build `controller-plc2`
- [pending] Flash both PLCs with the router-mode firmware
- [pending] Verify on hardware that boot logs report PLC CAN/module-manager disabled in `mode=2`
- [pending] Verify `CTRL FEEDBACK` alone is sufficient to drive PreCharge / CurrentDemand HLC responses in `mode=2`
- [pending] Verify relay control on hardware in `mode=2`

## Open Gap
- [pending] The repository still does not contain the direct external module CAN controller implementation itself. The PLC side is now separated correctly, but the actual controller-to-module CAN application still needs to be supplied or integrated.
