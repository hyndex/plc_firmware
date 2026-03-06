# CB PLC Firmware Control Plane Developer Guide

Updated: 2026-03-06

This document is the implementation-accurate API and behavior contract for:

- Controller <-> PLC CAN control plane
- PLC runtime mode behavior (standalone vs controller mode)
- HLC authorization gating behavior
- Module allocation/deallocation behavior
- Stop/shutdown behavior and safe handover semantics
- Serial command API (developer/test harness)
- SW4 boot setup portal behavior

Source of truth is firmware implementation in `src/main.cpp`.

## 1. Architecture and Authority Model

### 1.1 Runtime Modes

- `standalone_mode = true`
  - PLC is autonomous for SLAC start and energy path decisions.
  - External controller commands are optional from power authorization perspective.
- `standalone_mode = false` (default)
  - Controller is required for session control decisions.
  - SLAC start gating, energy authorization, and module allowlist are controller-driven.
  - Relay1 (gun contactor) remains PLC-owned and switched by PLC logic only.
  - Relay2/Relay3 are externally commandable through control-plane API.

### 1.2 Decision Ownership

- CP state, SLAC FSM, HLC server socket lifecycle: PLC-owned.
- SLAC permission in controller mode: controller-owned via `CTRL_SLAC_*`.
- Energy permission in controller mode: controller-owned via `CTRL_AUTH_*` + heartbeat TTL.
- Module setpoints and Relay1 toggling: PLC-owned, derived from HLC requests plus control-plane gating.
- Module allowlist ownership: controller-owned via alloc transaction API.

## 2. CAN Control Plane Spec

All control-plane frames are **extended CAN**, **8-byte payload**.

### 2.1 CAN ID Layout

- Low nibble (`ID & 0x0F`) is PLC target ID.
- Controller transmits to base IDs below with target PLC nibble.
- PLC transmits status using its configured PLC ID nibble.

Controller -> PLC base IDs:

- `0x18FF5000` `CTRL_HEARTBEAT`
- `0x18FF5010` `CTRL_AUTH`
- `0x18FF5020` `CTRL_SLAC`
- `0x18FF5030` `CTRL_ALLOC_BEGIN`
- `0x18FF5040` `CTRL_ALLOC_DATA`
- `0x18FF5050` `CTRL_ALLOC_COMMIT`
- `0x18FF5060` `CTRL_ALLOC_ABORT`
- `0x18FF5070` `CTRL_AUX_RELAY`
- `0x18FF5080` `CTRL_SESSION`

PLC -> Controller base IDs:

- `0x18FF6000` `PLC_HEARTBEAT`
- `0x18FF6010` `PLC_CP_STATUS`
- `0x18FF6020` `PLC_SLAC_STATUS`
- `0x18FF6030` `PLC_HLC_STATUS`
- `0x18FF6040` `PLC_POWER_STATUS`
- `0x18FF6050` `PLC_SESSION_STATUS`
- `0x18FF6060` `PLC_IDENTITY_EVT`
- `0x18FF6070` `PLC_CMD_ACK`

### 2.2 Common Payload Header

For controller->PLC commands (bytes are `data[0..7]`):

- `b0`: protocol version (`1`)
- `b1`: command sequence number
- `b2`: controller ID nibble in low 4 bits
- `b3..b6`: command-specific data
- `b7`: `CRC8(poly=0x07, init=0x00)` over bytes `b0..b6`

### 2.3 Ingress Validation Pipeline

Frames are dropped before queueing unless all checks pass:

- extended frame
- DLC = 8
- target PLC nibble matches local PLC ID
- protocol version matches (`1`)
- controller ID nibble matches local configured controller ID
- CRC valid

Then command-layer validation applies:

- per-command sequence acceptance
- command-specific state/value checks

### 2.4 Sequence Acceptance Rule

Per command family, a sequence is accepted when:

- first frame for that family always accepted
- subsequent frame accepted only if `delta = new_seq - prev_seq` is in `1..127`
- duplicate (`delta=0`) rejected
- large backward jump (`delta>127`) rejected as stale

Families with independent sequence tracks:

- heartbeat
- auth
- slac
- alloc (begin/data/commit/abort share one track)
- aux relay
- session

### 2.5 ACK Frame (`PLC_CMD_ACK`)

`CAN_ID = 0x18FF6070 | plc_id`

- `b0`: version
- `b1`: PLC ack sequence
- `b2`: command type code
- `b3`: echoed command sequence
- `b4`: ack status
- `b5`: detail0
- `b6`: detail1
- `b7`: CRC

Ack status codes:

- `0`: `ACK_OK`
- `1`: `ACK_BAD_CRC`
- `2`: `ACK_BAD_VERSION`
- `3`: `ACK_BAD_TARGET`
- `4`: `ACK_BAD_SEQ`
- `5`: `ACK_BAD_STATE`
- `6`: `ACK_BAD_VALUE`

Command type codes used by firmware:

- `0x01`: generic parser/validation reject
- `0x10`: heartbeat
- `0x11`: auth
- `0x12`: slac
- `0x13`: alloc begin
- `0x14`: alloc data
- `0x15`: alloc commit
- `0x16`: alloc abort / alloc timeout event
- `0x17`: aux relay
- `0x18`: session

## 3. Controller -> PLC Command APIs

## 3.1 `CTRL_HEARTBEAT` (`0x18FF5000`)

Payload:

- `b3`: heartbeat timeout in 100 ms (`1..255` => `100..25500 ms`)
- other command bytes reserved

Reaction:

- updates `last_heartbeat_ms`
- marks heartbeat as seen
- updates runtime heartbeat timeout with clamp `500..10000 ms`
- returns `ACK_OK`

If missing in controller mode:

- SLAC arm/start is disabled while heartbeat not alive
- after grace windows, safe stop is forced (see watchdog section)

## 3.2 `CTRL_AUTH` (`0x18FF5010`)

Payload:

- `b3`: auth state
  - `0`: denied
  - `1`: pending
  - `2`: granted
- `b4`: auth TTL in 100 ms (0 => uses configured default)

Reaction:

- updates `auth_state`
- updates `auth_expiry_ms`
- returns `ACK_OK`

Invalid values (`b3 > 2`) return `ACK_BAD_VALUE`.

How HLC uses this in controller mode:

- heartbeat missing or `denied` -> Authorization response `FAILED + Finished`
- `granted` -> Authorization returns one `Ongoing`, then `Finished`
- `pending` -> Authorization returns `OK + Ongoing`

## 3.3 `CTRL_SLAC` (`0x18FF5020`)

Payload:

- `b3`: command
  - `0`: disarm
  - `1`: arm
  - `2`: start now
  - `3`: abort
- `b4`: arm validity in 100 ms (0 => configured default)

Reaction by command:

- `DISARM`
  - clears arm/start latch
  - triggers `controller_force_safe_stop("CtrlDisarm")`
- `ARM`
  - sets `slac_armed=true`, `slac_start_latched=false`
  - sets arm expiry
- `START_NOW`
  - sets `slac_armed=true`, `slac_start_latched=true`
  - sets arm expiry
- `ABORT`
  - clears arm/start latch
  - triggers `controller_force_safe_stop("CtrlAbort")`

All successful commands return `ACK_OK`.
Invalid command returns `ACK_BAD_VALUE`.

SLAC start gate in controller mode:

- requires heartbeat alive
- and either `slac_start_latched==true` or `(slac_armed && arm_not_expired)`

## 3.4 Allocation Transaction API

### 3.4.1 `CTRL_ALLOC_BEGIN` (`0x18FF5030`)

Payload:

- `b3`: transaction id
- `b4`: expected module count (`1..32`)
- `b5`: txn TTL in 100 ms (`0` => configured default)

Reaction:

- starts staged transaction
- clears staged list
- sets expiry
- `ACK_OK(detail0=txn_id, detail1=expected)`

Invalid expected count returns `ACK_BAD_VALUE`.

### 3.4.2 `CTRL_ALLOC_DATA` (`0x18FF5040`)

Payload:

- `b3`: transaction id
- `b4`: index (currently informational)
- `b5`: module address

Supported mapping in current firmware:

- `0x01` -> `MXR_1`

Reaction:

- requires active txn and matching txn_id
- appends module id if not already present (dedup)
- `ACK_OK(detail0=module_addr, detail1=staged_size)`

Error reactions:

- no active txn -> `ACK_BAD_STATE`
- txn mismatch or unknown module addr -> `ACK_BAD_VALUE`

### 3.4.3 `CTRL_ALLOC_COMMIT` (`0x18FF5050`)

Payload:

- `b3`: transaction id

Reaction:

- requires active txn and matching txn id and non-empty staged list
- calls `apply_new_allowlist(staged_allowlist, "CtrlAllocCommit")`
- on success: active allowlist updated
- always clears txn state after attempt
- returns
  - `ACK_OK(detail0=active_allowlist_size)` on success
  - `ACK_BAD_VALUE` on apply failure

### 3.4.4 `CTRL_ALLOC_ABORT` (`0x18FF5060`)

Reaction:

- clears staged txn state
- `ACK_OK`

### 3.4.5 Allocation Timeout

If staged txn TTL expires, firmware auto-aborts txn and sends:

- `ACK cmd=0x16, status=ACK_BAD_STATE, detail0=0xEE`

## 3.5 `CTRL_AUX_RELAY` (`0x18FF5070`)

Payload:

- `b3`: enable mask
  - bit0 -> Relay2 command valid
  - bit1 -> Relay3 command valid
- `b4`: state mask
  - bit0 -> desired Relay2 state
  - bit1 -> desired Relay3 state
- `b5`: hold timeout in 100 ms (`0` = no auto-off)

Reaction:

- applies selected relays only
- per relay, if hold timeout > 0, auto-off after timeout
- returns `ACK_OK(detail0=en_mask, detail1=st_mask)`

Relay1 is not controlled by this API.

## 3.6 `CTRL_SESSION` (`0x18FF5080`)

Payload:

- `b3`: action
  - `0`: none
  - `1`: soft stop
  - `2`: hard stop
  - `3`: clear stop state
- `b4`: timeout in 100 ms (`0` => 3000 ms, then clamped to `500..30000 ms`)
- `b5`: reason code (stored)

Reaction:

- `SOFT/HARD`: starts stop FSM and applies stop policy immediately
- `CLEAR/NONE`: clears stop flags only

ACK:

- `ACK_OK(detail0=action, detail1=stop_complete_flag)`
- invalid action -> `ACK_BAD_VALUE`

## 4. PLC -> Controller Status APIs

All status frames are periodic and include version + sequence + CRC.

## 4.1 `PLC_HEARTBEAT` (`0x18FF6000`, 500 ms)

- `b2`: standalone flag (1=true)
- `b3`: CP state char (`A/B/C/D/E/F`)
- `b4` bitfield:
  - bit0 HLC ready
  - bit1 HLC active
  - bit2 module output enabled
- `b5..b6`: uptime seconds low/high bytes

## 4.2 `PLC_CP_STATUS` (`0x18FF6010`, 200 ms)

- `b2`: CP state char
- `b3`: CP PWM duty percent
- `b4..b5`: CP millivolts little-endian
- `b6`: time since CP connected in 100 ms (8-bit)

## 4.3 `PLC_SLAC_STATUS` (`0x18FF6020`, 500 ms)

- `b2`: session started flag
- `b3`: session matched flag
- `b4`: SLAC FSM state enum value (0xFF if no FSM)
- `b5`: SLAC failure counter for current CP cycle
- `b6` bitfield:
  - bit0 `slac_armed`
  - bit1 `slac_start_latched`

## 4.4 `PLC_HLC_STATUS` (`0x18FF6030`, 500 ms)

- `b2` bitfield:
  - bit0 HLC ready
  - bit1 HLC active
- `b3`: auth state enum (`Unknown/Denied/Pending/Granted` values)
- `b4`: heartbeat alive flag
- `b5`: auth granted flag (`controller_auth_granted()`)
- `b6`: precharge_seen flag

## 4.5 `PLC_POWER_STATUS` (`0x18FF6040`, 200 ms)

- `b2..b3`: combined voltage * 10 (u16 little-endian)
- `b4..b5`: combined current * 10 (u16 little-endian)
- `b6` nibble packing:
  - low nibble: active modules
  - high nibble: assigned modules

## 4.6 `PLC_SESSION_STATUS` (`0x18FF6050`, 500 ms)

- `b2` bitfield:
  - bit0 session started
  - bit1 session matched
- `b3` bitfield:
  - bit0 Relay1 closed
  - bit1 Relay2 closed
  - bit2 Relay3 closed
  - bit3 stop_active
  - bit4 stop_hard
  - bit5 stop_complete
- `b4..b5`: meter Wh (u16 little-endian)
- `b6`: uptime seconds low byte

## 4.7 `PLC_IDENTITY_EVT` (`0x18FF6060`, event-driven)

Segmented identity/event transport:

- `b2`: event kind
  - `1`: local MAC
  - `2`: EV MAC
  - `3`: EVCCID
  - `4`: EMAID
- `b3`: event id
- `b4`: high nibble total segment count, low nibble segment index
- `b5..b6`: payload bytes for this segment (2 bytes per segment)

Notes:

- Local MAC is published once on boot.
- EV MAC is published when changed after SLAC match.
- EVCCID is published on SessionSetup request.

## 5. Controller Call Order (Recommended)

Controller mode (`standalone_mode=false`) recommended session sequence:

1. Start heartbeat loop every `<= 400 ms`.
2. Send `CTRL_AUTH pending` with TTL refresh cadence (for example every 800 ms).
3. On gun connect and business logic allow:
   - `CTRL_ALLOC_BEGIN`
   - `CTRL_ALLOC_DATA` for each module
   - `CTRL_ALLOC_COMMIT`
4. Send `CTRL_SLAC arm` and `CTRL_SLAC start`.
5. Wait for `PLC_SLAC_STATUS matched=1` and HLC status activity.
6. Once identity/event policy allows charging, send `CTRL_AUTH grant` refresh loop.
7. During charging maintain:
   - heartbeat refresh
   - auth TTL refresh
   - allocation refresh when needed
8. For controlled stop:
   - `CTRL_SESSION stop_soft` (or hard)
   - `CTRL_AUTH deny`
   - `CTRL_SLAC disarm`
9. Wait until `PLC_SESSION_STATUS stop_complete=1` and `PLC_POWER_STATUS assigned=0 active=0`.

## 6. Stop, Shutdown, and Module Deallocation Semantics

## 6.1 Stop Guarantees in Current Firmware

When stop is triggered (`CTRL_SESSION stop_*`, disarm, abort, reset, or watchdog):

- HLC/session power path is disabled.
- Relay1 is opened.
- Module OFF target is sent.
- Allowlist transition to empty is applied (release ownership for handover).

`controller_stop_is_complete()` requires:

- Relay1 open
- module output disabled
- no SLAC session active
- no HLC active client
- allowlist empty

## 6.2 Soft vs Hard Stop

- Soft stop:
  - starts graceful stop with deadline
  - if deadline exceeded, escalates to hard force stop
- Hard stop:
  - immediate force safe stop path

## 6.3 Allowlist Transition Behavior

`apply_new_allowlist()` now performs OFF transition on both sides:

- OFF applied to previous allowlist
- OFF applied to next allowlist
- relay opened
- then active allowlist state is switched

This prevents stale ownership and supports safe multi-PLC handover.

## 7. Watchdogs and Automatic Reactions

Controller mode watchdog behaviors:

- Heartbeat loss:
  - immediately clears SLAC arm/start permissions
  - after `CONTROLLER_HB_SOFT_STOP_GRACE_MS` (3000 ms) if energy path active -> safe stop
  - after `CONTROLLER_HB_HARD_STOP_GRACE_MS` (15000 ms) if session/HLC active -> safe stop
- Auth TTL expiry from `Granted`:
  - transitions to `Denied`
  - modules OFF + Relay1 open
- SLAC arm TTL expiry:
  - clears `slac_armed` and `slac_start_latched`
- Allocation txn TTL expiry:
  - staged allocation dropped and timeout ack emitted
- Relay2/Relay3 hold timeout:
  - relay auto-opens

## 8. HLC Power Path Reactions

## 8.1 Authorization Requests

In controller mode:

- `Denied` or heartbeat missing -> `FAILED` auth response
- `Pending` -> `OK + Ongoing`
- `Granted` -> first poll `Ongoing`, next poll `Finished`

This enforces at least one ongoing cycle before final authorization completion.

## 8.2 PreCharge Requests

- Uses EV requested target V and I, but clamps precharge current to `PRECHARGE_CURRENT_LIMIT_A` (2 A).
- If `controller_allows_energy()` false:
  - Relay1 open
  - modules OFF
  - EVSE status reported NotReady

## 8.3 CurrentDemand Requests

- Uses EV requested V/I as command target.
- Enables Relay1 and modules only when:
  - `controller_allows_energy() == true`
  - requested current > 0.05 A
- Response telemetry uses real module aggregate snapshots (with stale filtering).

## 8.4 PowerDelivery and SessionStop

- PowerDelivery stop/disable or controller energy disallow => Relay1 open and module current hold disabled.
- SessionStop request => modules OFF + Relay1 open.

## 9. Module Telemetry and Aggregation Contract

Aggregation for group `G_MAIN`:

- Current: sum of fresh module currents
- Voltage: average of fresh module voltages
- Power: `V * I`

Freshness controls:

- module telemetry stale threshold: `MODULE_TELEMETRY_FRESH_MS = 10000 ms`
- module manager runtime probe interval set to `200 ms`
- service loop runs at 10 ms normally, 50 ms during SLAC-critical stage

Fallback behavior:

- if combined V/I are too low/missing, response may fall back to last measured values for continuity.

## 10. Serial Developer API (Local Console)

Serial commands are a local controller emulator and debug API.

Commands:

- `CTRL HB [timeout_ms]`
- `CTRL AUTH <deny|pending|grant> [ttl_ms]`
- `CTRL SLAC <disarm|arm|start|abort> [arm_ms]`
- `CTRL RESET`
- `CTRL ALLOC1`
- `CTRL ALLOC BEGIN <txn> <expected> [ttl_ms]`
- `CTRL ALLOC DATA <txn> <index> <module_addr>`
- `CTRL ALLOC COMMIT <txn>`
- `CTRL ALLOC ABORT`
- `CTRL RELAY <enable_mask> <state_mask> [hold_ms]`
- `CTRL STOP <soft|hard|clear> [timeout_ms]`
- `CTRL MODE <standalone0|1> <plc_id 0..15> [controller_id 0..15]`
- `CTRL SAVE`
- `CTRL STATUS`

Notes:

- `CTRL MODE` changes runtime config in RAM only; call `CTRL SAVE` + reboot to persist.
- `CTRL RESET` clears runtime controller state and performs safe-stop.

## 11. SW4 Setup Portal API

If SW4 is held at boot:

- firmware starts AP `CBPLC-SETUP-<mac-suffix>`
- serves setup form on `/` for 120 seconds
- POST `/save` persists config to NVS and reboots

Fields:

- standalone mode
- SLAC requires controller start
- PLC ID
- controller ID
- heartbeat timeout ms
- auth TTL ms
- alloc TTL ms
- SLAC arm timeout ms

If SW4 is not pressed, portal is not started.

## 12. Edge Cases and Expected Reactions

## 12.1 Duplicate or stale command sequences

- Rejected with `ACK_BAD_SEQ`.
- No state mutation for that command.

## 12.2 CRC/version/target mismatch

- Rejected at parser layer with generic ack (`cmd_type=0x01`) and corresponding status.
- Not enqueued into command logic.

## 12.3 Unknown module address in alloc data

- `ACK_BAD_VALUE`.
- staged transaction remains active for corrected frames.

## 12.4 Commit with empty staged list

- `ACK_BAD_VALUE`.
- no allowlist change.

## 12.5 Heartbeat alive but auth pending forever

- Session can stay in authorization ongoing state.
- Energy path remains blocked unless granted.

## 12.6 Controller disarm/abort during active charge

- Safe-stop path runs:
  - modules OFF
  - Relay1 open
  - HLC client/socket stop
  - session reset

## 12.7 Stop complete but module reports `off=0`

Some module firmwares may not expose explicit off bit consistently.
Operationally safe completion is based on:

- allowlist released (`assigned=0`)
- active modules `0`
- output disabled
- current ~ `0 A`

## 12.8 Controller ID / PLC ID misconfiguration

- Frames with mismatched target/controller nibble are ignored.
- System appears idle (`waiting SLAC start auth`) in controller mode.

## 13. Default Timing and Limits Summary

- Startup log hold: 10 s
- Heartbeat timeout default: 1200 ms (clamp 500..10000)
- Auth TTL default: 2500 ms (clamp 500..30000)
- Alloc txn TTL default: 3000 ms (clamp via command / config)
- SLAC arm timeout default: 10000 ms (clamp 1000..60000)
- Soft stop grace on HB loss: 3000 ms
- Hard stop grace on HB loss with session/HLC active: 15000 ms
- Module allowlist lease in setpoint path: 1200 ms

## 14. Memory and CPU Budget Notes

Current build snapshot (`pio run`, 2026-03-06):

- RAM usage: ~26.3% (`86340 / 327680`)
- Flash usage: ~31.0% (`1035377 / 3342336`)

Runtime pacing notes:

- Main loop delay: `1 ms` during CP/session/HLC-active phases, else `5 ms`.
- Module service: `10 ms` nominal, `50 ms` SLAC-critical phase.
- Status TX rates:
  - CP/power at `200 ms`
  - heartbeat/SLAC/HLC/session at `500 ms`

SW4 portal memory behavior:

- WiFi AP + WebServer objects are created only in SW4-held boot path.
- In normal boot path (SW4 released), portal code path exits early and AP is not started.
- After successful save, firmware reboots and returns to normal memory profile.

## 15. Validation Checklist for Integrators

Before production integration:

1. Confirm controller sends heartbeat and auth refresh at stable cadence.
2. Confirm alloc transaction sequence is begin->data->commit every re-assignment.
3. Confirm SLAC arm/start is sent only after CP connect policy allows.
4. Confirm stop command path checks `PLC_SESSION_STATUS.stop_complete=1` and `PLC_POWER_STATUS assigned=0 active=0`.
5. Confirm controller handles `ACK_BAD_SEQ` by advancing/re-syncing sequence.
6. Confirm controller handles `ACK_BAD_VALUE` with retry/repair logic.
7. Confirm recovery behavior after controller restart (re-send mode heartbeat/auth/alloc/slac state).
