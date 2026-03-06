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
- Default module wiring profile: MXR module on CAN (e.g., module id `1`), low-bandwidth capable mode.
- Hardware base aligned to QCA7000 + MCP2515 ESP32-S3 setup.

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
  - segmented identity events (local MAC, EVCCID)

## Safety Notes

- In `StandaloneMode=disabled`, charging power path is gated by external authorization and heartbeat health.
- SLAC start is controller-gated in controller mode.
- Loss of controller heartbeat/auth window enforces safe stop (Relay1 open, modules OFF, HLC/session stop).

## Build

```bash
pio run -d /home/jpi/Desktop/EVSE/plc_firmware
```

## Flash

```bash
pio run -d /home/jpi/Desktop/EVSE/plc_firmware -t upload --upload-port /dev/ttyACM0
```

## Serial log

```bash
stty -F /dev/ttyACM0 115200 raw -echo -echoe -echok -echoctl -echoke
cat /dev/ttyACM0
```
