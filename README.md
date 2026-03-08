# CB PLC Firmware (ESP32-S3)

Integrated EVSE firmware built from:

- `cbslac` (PLC SLAC matching)
- `jpv2g` (ISO15118/DIN HLC SECC)
- `cbmodules` (DC power-module management)

## Documentation

- Full control-plane/API contract and behavior guide:
  - `DEVELOPER_CONTROL_PLANE_API.md`
- Implementation execution tracker:
  - `IMPLEMENTATION_TODO.md`

## Operating Modes

- `StandaloneMode=enabled`
  - PLC runs independently (CP -> SLAC -> HLC -> module control, Relay1 local).
- `StandaloneMode=disabled`
  - PLC depends on external controller for authorization and SLAC start permission.
  - PLC publishes status and EV identity events (RFID, EV MAC, EVCCID/EMAID where available).
  - Relay1 remains PLC-owned; Relay2/Relay3 are externally commandable by policy.

## Current Hardware Profile

- Gun contactor: `Relay 1 (EXP_P00 / PCA9555)` for both precharge and charging.
- No separate precharge contactor: precharge current is limited via module setpoint path.
- Default local module wiring profile: one MXR module per PLC-local CAN bus at module address `0x01`.
- Logical PLC group identity is tied to `plc_id` (`PLC_GROUP_<plc_id>`); the current Maxwell low-level CAN group stays at its working local profile value.
- Hardware base aligned to QCA7000 + MCP2515 ESP32-S3 setup.

## Persisted PLC Identity

Each PLC persists:

- `plc_id`: unique PLC identity on the controller CAN contract
- `connector_id`: stable connector mapping for the site controller
- `controller_id`: only control-plane frames from this controller are accepted
- `local_module_address`: only this local module address is admitted into the module stack

This keeps one firmware image reusable across multiple PLCs while still giving each board a unique runtime identity.

## Active Rework Plan

- The full architecture rework plan is maintained in:
  - `IMPLEMENTATION_TODO.md`
- It includes:
  - external-controller control plane (auth, SLAC arm/start, module assignment),
  - status/identity uplink contract,
  - safety watchdog/fail-safe logic,
  - SW4-gated WiFi manager boot behavior,
  - staged implementation and validation matrix.

## Production Integration

Use the developer guide as the normative contract for:

- CAN frame payloads and ACK semantics
- session call order from controller
- stop/deallocation completion checks
- watchdog/fault reactions and edge-case handling

## Implemented Controller Plane (Current)

- RX from controller:
  - heartbeat watchdog
  - authorization state (pending/granted/denied with TTL)
  - SLAC control (`DISARM/ARM/START_NOW/ABORT`)
  - allocation transaction (`BEGIN/DATA/COMMIT/ABORT`) for module allowlist
  - Relay2/Relay3 command with timeout-off safety
- TX to controller:
  - heartbeat, CP status, SLAC status, HLC/auth status, power status, session status
  - charging telemetry uplink: `V/I` (power status) + `SoC/Wh` (session status)
  - command acknowledgements (CRC/version/seq/state/value validation)
  - segmented identity events (local MAC, PLC/connector identity, EVCCID)

## Safety Notes

- In `StandaloneMode=disabled`, charging power path is gated by external authorization and heartbeat health.
- SLAC start is controller-gated in controller mode.
- Loss of controller heartbeat/auth window enforces safe stop (Relay1 open, modules OFF, HLC/session stop).
- Controller CAN frames are consumed by the PLC control-plane path and are never forwarded into `cbmodules`.
- Module CAN receive is masked to the PLC's own `local_module_address` plus matching ownership-claim traffic for that same local module.

## Build

```bash
pio run -d /home/jpi/Desktop/EVSE/plc_firmware
```

## Flash

```bash
pio run -d /home/jpi/Desktop/EVSE/plc_firmware -t upload --upload-port /dev/ttyACM0
```

## Erase

```bash
pio run -d /home/jpi/Desktop/EVSE/plc_firmware -t erase --upload-port /dev/ttyACM0
```

## Provision Identity

After flashing, provision each board over serial and then reboot it:

```text
CTRL MODE 0 <plc_id> <controller_id>
CTRL OWNERSHIP <connector_id> <module_addr>
CTRL SAVE
```

Example 2-PLC commissioning used on 2026-03-07:

- `/dev/ttyACM0` -> `plc_id=1 connector_id=1 controller_id=1 module_addr=0x01`
- `/dev/ttyACM1` -> `plc_id=2 connector_id=2 controller_id=1 module_addr=0x01`

## Serial log

```bash
stty -F /dev/ttyACM0 115200 raw -echo -echoe -echok -echoctl -echoke
cat /dev/ttyACM0
```
