# CB PLC Firmware Control Plane Developer Guide

Updated: 2026-03-09

This document is the implementation-accurate contract for:

- controller <-> PLC CAN control plane
- PLC runtime mode behavior
- module allocation and deallocation semantics
- controller-managed power control
- serial developer API
- SW4 setup portal behavior

The source of truth is `src/main.cpp`.

## 1. Architecture and Authority Model

### 1.1 Runtime Modes

- `mode=0` `Standalone`
  - PLC is autonomous for SLAC start, auth, module targets, HLC power replies, and Relay1.
- `mode=1` `ControllerSupported`
  - controller owns heartbeat, auth, SLAC permission, and allocation
  - PLC still owns module targets, HLC power replies, and Relay1 timing
- `mode=2` `ControllerManaged`
  - controller owns heartbeat, auth, SLAC permission, allocation, live power setpoints, live HLC power-side reply data, and may command Relay1 through `CTRL_AUX_RELAY`
  - PLC still owns CP, SLAC, HLC transport, and hard-stop safety overrides

### 1.2 Decision Ownership

- CP sampling, SLAC FSM, HLC socket lifecycle: PLC-owned in all modes
- SLAC permission in `mode=1/2`: controller-owned via `CTRL_SLAC_*`
- energy permission in `mode=1/2`: controller-owned via heartbeat + auth + non-empty allowlist
- module allowlist ownership in `mode=1/2`: controller-owned via allocation transaction API
- module setpoints:
  - `mode=0/1`: PLC-owned from HLC demand
  - `mode=2`: controller-owned through `CTRL_POWER_SETPOINT`
- HLC power-side reply values:
  - `mode=0/1`: PLC-owned from local module telemetry
  - `mode=2`: controller-owned through `CTRL_HLC_FEEDBACK`
- Relay1:
  - `mode=0/1`: PLC-owned
  - `mode=2`: controller-commandable through `CTRL_AUX_RELAY` bit 2, but PLC safety logic can still force it open

### 1.3 Persisted PLC Identity

Persisted in NVS:

- `mode`
- `plc_id`
- `connector_id`
- `controller_id`
- `local_module_address`
- `can_node_id`
- controller timeout fields

Derived at runtime:

- logical group id: `PLC_GROUP_<plc_id>`
- local module id: `PLC<plc_id>_MXR_<module_addr>`
- module-manager owner id: `(controller_id << 8) | plc_id`

Legacy config migration:

- v3 `standalone_mode=true` -> `mode=0`
- v3 `standalone_mode=false` -> `mode=1`

## 2. CAN Control Plane Spec

All control-plane frames are extended CAN with 8-byte payloads.

### 2.1 CAN ID Layout

- low nibble (`ID & 0x0F`) is PLC target id
- controller transmits to the base ids below plus the target PLC nibble
- PLC transmits status using its configured PLC id nibble

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
- `0x18FF5090` `CTRL_POWER_SETPOINT`
- `0x18FF50A0` `CTRL_HLC_FEEDBACK`

PLC -> controller base IDs:

- `0x18FF6000` `PLC_HEARTBEAT`
- `0x18FF6010` `PLC_CP_STATUS`
- `0x18FF6020` `PLC_SLAC_STATUS`
- `0x18FF6030` `PLC_HLC_STATUS`
- `0x18FF6040` `PLC_POWER_STATUS`
- `0x18FF6050` `PLC_SESSION_STATUS`
- `0x18FF6060` `PLC_IDENTITY_EVT`
- `0x18FF6070` `PLC_CMD_ACK`
- `0x18FF6080` `PLC_BMS_STATUS`

### 2.2 Common Payload Header

For controller -> PLC commands:

- `b0`: protocol version (`1`)
- `b1`: command sequence
- `b2`: controller id nibble in low 4 bits
- `b3..b6`: command-specific payload
- `b7`: `CRC8(poly=0x07, init=0x00)` over `b0..b6`

For PLC -> controller frames:

- `b0`: protocol version (`1`)
- `b1`: per-frame-family sequence
- `b2..b6`: frame-specific payload
- `b7`: CRC8 over `b0..b6`

### 2.3 Ingress Validation Pipeline

Frames are dropped before queueing unless all checks pass:

- extended frame
- DLC = 8
- target PLC nibble matches local PLC id
- protocol version matches
- controller id nibble matches configured controller id
- CRC valid

Then command-layer validation applies:

- per-family sequence acceptance
- command-specific state and value checks

All controller-plane CAN families are consumed by the PLC control-plane receive path and never forwarded into `cbmodules`.

Module CAN ingress is filtered separately to:

- local module
- active allowlist modules
- staged allocation modules
- ownership traffic for those same modules

### 2.4 Sequence Acceptance Rule

Each family accepts sequences with:

- first frame always accepted
- later frame accepted only when `delta = new_seq - prev_seq` is in `1..127`
- duplicate (`delta=0`) rejected
- large backward jump (`delta>127`) rejected as stale

Independent sequence tracks exist for:

- heartbeat
- auth
- slac
- allocation (`BEGIN/DATA/COMMIT/ABORT` share one track)
- aux relay
- session
- managed power
- managed feedback

Heartbeat session-marker change or heartbeat re-establishment resets the non-heartbeat sequence tracks.

## 3. ACK Frame

`CAN_ID = 0x18FF6070 | plc_id`

- `b0`: version
- `b1`: PLC ack sequence
- `b2`: command type
- `b3`: echoed command sequence
- `b4`: ack status
- `b5`: detail0
- `b6`: detail1
- `b7`: CRC

Ack status:

- `0` `ACK_OK`
- `1` `ACK_BAD_CRC`
- `2` `ACK_BAD_VERSION`
- `3` `ACK_BAD_TARGET`
- `4` `ACK_BAD_SEQ`
- `5` `ACK_BAD_STATE`
- `6` `ACK_BAD_VALUE`

Command type codes used by firmware:

- `0x01`: parser / validation reject
- `0x10`: heartbeat
- `0x11`: auth
- `0x12`: slac
- `0x13`: alloc begin
- `0x14`: alloc data
- `0x15`: alloc commit
- `0x16`: alloc abort / alloc timeout event
- `0x17`: aux relay
- `0x18`: session
- `0x19`: managed power setpoint
- `0x1A`: managed HLC feedback

## 4. Controller -> PLC Command APIs

## 4.1 `CTRL_HEARTBEAT` (`0x18FF5000`)

Payload:

- `b3`: heartbeat timeout in 100 ms units (`1..255` -> `100..25500 ms`, clamped to `500..10000 ms`)
- `b4..b6`: optional session marker, used only for command-stream resync

Reaction:

- updates `last_heartbeat_ms`
- marks heartbeat as seen
- updates runtime heartbeat timeout if `b3 > 0`
- resets sequence state if heartbeat had expired or the session marker changed
- returns `ACK_OK`

In `mode=1/2`, missing heartbeat:

- immediately clears controller SLAC arm/start permission after real heartbeat loss
- forces safe stop after `3000 ms` if energy path is active
- forces safe stop after `15000 ms` if session/HLC is active

## 4.2 `CTRL_AUTH` (`0x18FF5010`)

Payload:

- `b3`: auth state
  - `0`: denied
  - `1`: pending
  - `2`: granted
- `b4`: auth TTL in 100 ms units (`0` -> use configured default)

Reaction:

- updates auth state and expiry
- explicit `Denied` immediately:
  - clears managed power cache
  - clears managed feedback cache
  - turns modules off
  - opens Relay1
- returns `ACK_OK`

Invalid auth state returns `ACK_BAD_VALUE`.

HLC authorization behavior in `mode=1/2`:

- heartbeat missing or `Denied` -> auth response `FAILED`
- `Pending` -> auth response `Ongoing`
- `Granted` -> one `Ongoing`, then `Finished`

## 4.3 `CTRL_SLAC` (`0x18FF5020`)

Payload:

- `b3`: command
  - `0`: disarm
  - `1`: arm
  - `2`: start now
  - `3`: abort
- `b4`: arm validity in 100 ms units (`0` -> configured default)

Reaction:

- `DISARM`
  - clears arm/start state
  - calls safe stop
- `ARM`
  - sets `slac_armed=true`, `slac_start_latched=false`
  - updates arm expiry
- `START_NOW`
  - sets `slac_armed=true`, `slac_start_latched=true`
  - updates arm expiry
- `ABORT`
  - clears arm/start state
  - calls safe stop

All valid commands return `ACK_OK`.
Invalid command returns `ACK_BAD_VALUE`.

SLAC start gate in `mode=1/2`:

- heartbeat must be alive
- and either `slac_start_latched==true` or `(slac_armed && !expired)`

## 4.4 `CTRL_ALLOC_BEGIN` (`0x18FF5030`)

Payload:

- `b3`: transaction id
- `b4`: expected module count (`1..32`)
- `b5`: transaction TTL in 100 ms units (`0` -> configured default)
- `b6`: assignment power limit in `0.5 kW` units (`0` -> unlimited by assignment contract)

Reaction:

- starts staged transaction
- clears staged allowlist and staged module limits
- stores expected size, TTL, and assignment limit
- returns `ACK_OK(detail0=txn_id, detail1=expected)`

Invalid expected size returns `ACK_BAD_VALUE`.

## 4.5 `CTRL_ALLOC_DATA` (`0x18FF5040`)

Payload:

- `b3`: transaction id
- `b4`: module CAN group (`1..7`)
- `b5`: module CAN address
- `b6`: per-module power limit in `0.5 kW` units (`0` -> module default max)

Reaction:

- requires active transaction and matching txn id
- validates module group and module address
- upserts remote module metadata if needed
- deduplicates module ids in staged allowlist
- stores per-module limit for that staged module
- returns `ACK_OK(detail0=module_addr, detail1=staged_unique_count)`

Error reactions:

- no active txn -> `ACK_BAD_STATE`
- txn mismatch, bad group, or bad module address -> `ACK_BAD_VALUE`

## 4.6 `CTRL_ALLOC_COMMIT` (`0x18FF5050`)

Payload:

- `b3`: transaction id

Reaction:

- requires active txn
- txn id must match
- staged unique module count must equal expected count
- normalizes staged power limits to the staged allowlist
- applies limits before swapping allowlists
- pushes the new allowlist through `apply_group_setpoint_allowlist`
- if remote modules are present and first apply fails, retries discovery/polling for up to `1500 ms`
- if output is active and the set changed, re-applies the last target to the new allowlist
- clears staged transaction state after the attempt

ACK:

- success -> `ACK_OK(detail0=active_allowlist_size)`
- failure -> `ACK_BAD_VALUE`

## 4.7 `CTRL_ALLOC_ABORT` (`0x18FF5060`)

Payload:

- none

Reaction:

- clears staged transaction state
- if idle and no active session/HLC/Relay1 output remains, may release the active allowlist too
- returns `ACK_OK`

Allocation timeout auto-aborts and emits:

- `cmd=0x16`
- `status=ACK_BAD_STATE`
- `detail0=0xEE`

## 4.8 `CTRL_AUX_RELAY` (`0x18FF5070`)

Payload:

- `b3`: enable mask
  - bit0 -> Relay2 command valid
  - bit1 -> Relay3 command valid
  - bit2 -> Relay1 command valid (`mode=2` only)
- `b4`: state mask
  - bit0 -> desired Relay2 state
  - bit1 -> desired Relay3 state
  - bit2 -> desired Relay1 state
- `b5`: hold timeout in 100 ms units (`0` -> no auto-open timeout)

Reaction:

- applies only the relays enabled by `b3`
- Relay1 close is rejected unless:
  - runtime mode is `2`
  - energy is currently allowed
  - CP is connected
- Relay2 and Relay3 are valid in all modes
- selected relays use per-relay auto-open deadlines if `hold_ms > 0`

ACK:

- success -> `ACK_OK(detail0=en_mask, detail1=st_mask)`
- invalid state -> `ACK_BAD_STATE`

## 4.9 `CTRL_SESSION` (`0x18FF5080`)

Payload:

- `b3`: action
  - `0`: none
  - `1`: soft stop
  - `2`: hard stop
  - `3`: clear stop state
- `b4`: timeout in 100 ms units (`0` -> `3000 ms`, clamped to `500..30000 ms`)
- `b5`: reason code

Reaction:

- `SOFT/HARD`
  - starts stop FSM
  - applies stop policy immediately
- `CLEAR/NONE`
  - clears stop flags
  - clears managed power and feedback caches

ACK:

- `ACK_OK(detail0=action, detail1=stop_complete_flag)`
- invalid action -> `ACK_BAD_VALUE`

## 4.10 `CTRL_POWER_SETPOINT` (`0x18FF5090`)

Only valid in `mode=2`.

Payload:

- `b3..b6`: packed 32-bit little-endian value

Bit layout:

- bit0: `enable`
- bits1..14: target voltage in `0.1 V` units
- bits15..26: target current in `0.1 A` units

Reaction:

- rejects with `ACK_BAD_STATE` unless:
  - current mode is `2`
  - if `enable=1`, energy is allowed
  - if `enable=1`, allowlist is non-empty
  - if `enable=1`, CP is connected
- `enable=1` applies module target immediately
- `enable=0` turns modules off
- stores managed power cache for freshness tracking

Managed power freshness timeout:

- `1200 ms`
- stale managed power command -> modules off + Relay1 open

## 4.11 `CTRL_HLC_FEEDBACK` (`0x18FF50A0`)

Only valid in `mode=2`.

Payload:

- `b3..b6`: packed 32-bit little-endian value

Bit layout:

- bit0: `ready`
- bit1: current limit achieved
- bit2: voltage limit achieved
- bit3: power limit achieved
- bits4..17: present voltage in `0.1 V` units
- bits18..29: present current in `0.1 A` units
- bit30: `valid`

Reaction:

- rejects with `ACK_BAD_STATE` unless current mode is `2`
- updates managed feedback cache
- does not directly energize modules or relays

Managed feedback freshness timeout:

- `1200 ms`
- stale feedback clears the feedback cache
- HLC responses then degrade to not-ready / zero-current behavior until fresh feedback returns

## 5. PLC -> Controller Status APIs

All status frames include version, per-family sequence, and CRC.

### 5.1 `PLC_HEARTBEAT` (`0x18FF6000`, every `500 ms`)

- `b2`: current mode (`0`, `1`, or `2`)
- `b3`: CP state char
- `b4` bitfield:
  - bit0 HLC ready
  - bit1 HLC active
  - bit2 module output enabled
- `b5..b6`: uptime seconds low/high bytes

### 5.2 `PLC_CP_STATUS` (`0x18FF6010`, every `200 ms`)

- `b2`: CP state char
- `b3`: CP PWM duty percent
- `b4..b5`: CP millivolts little-endian
- `b6`: time since CP connected in `100 ms` units

### 5.3 `PLC_SLAC_STATUS` (`0x18FF6020`, every `500 ms`)

- `b2`: session started flag
- `b3`: session matched flag
- `b4`: SLAC FSM state (`0xFF` if no FSM)
- `b5`: SLAC failure counter for current CP cycle
- `b6` bitfield:
  - bit0 `slac_armed`
  - bit1 `slac_start_latched`

### 5.4 `PLC_HLC_STATUS` (`0x18FF6030`, every `500 ms`)

- `b2` bitfield:
  - bit0 HLC ready
  - bit1 HLC active
- `b3`: auth state enum
- `b4`: heartbeat alive flag
- `b5`: `controller_auth_granted()` flag
- `b6` bitfield:
  - bit0 precharge seen
  - bit1 precharge converged

### 5.5 `PLC_POWER_STATUS` (`0x18FF6040`, every `200 ms`)

- `b2..b3`: combined voltage in `0.1 V` units
- `b4..b5`: combined current in `0.1 A` units
- `b6` nibble packing:
  - low nibble = active modules
  - high nibble = assigned modules

This is the fast local electrical telemetry path from module aggregation.

### 5.6 `PLC_SESSION_STATUS` (`0x18FF6050`, every `500 ms`)

- `b2` bitfield:
  - bit0 session started
  - bit1 session matched
- `b3` bitfield:
  - bit0 Relay1 closed
  - bit1 Relay2 closed
  - bit2 Relay3 closed
  - bit3 stop active
  - bit4 stop hard
  - bit5 stop complete
- `b4..b5`: meter Wh (`u16`)
- `b6`: last EV SoC (`0..100`, `0xFF` unknown)

### 5.7 `PLC_IDENTITY_EVT` (`0x18FF6060`, event-driven with retry`)

- `b2`: event kind
  - `1`: local MAC
  - `2`: EV MAC
  - `3`: EVCCID
  - `4`: EMAID
  - `5`: PLC identity tuple
- `b3`: event id
- `b4`: high nibble total segments, low nibble segment index
- `b5..b6`: 2 payload bytes for this segment

Event kind `5` PLC identity tuple payload:

- `plc_id`
- `connector_id`
- `controller_id`
- `local_module_address`
- logical module group
- owner id high byte
- owner id low byte

### 5.8 `PLC_BMS_STATUS` (`0x18FF6080`, every `250 ms` or on change)

This frame is especially important in `mode=2`.

- `b2..b3`: latest EV-requested voltage in `0.1 V` units
- `b4..b5`: latest EV-requested current in `0.1 A` units
- `b6` bitfield:
  - bit0 request valid
  - bit1 delivery ready flag as currently interpreted by PLC
  - bits2..7 HLC stage code

Current stage codes used by firmware:

- `1`: precharge
- `2`: current demand
- `3`: power-delivery update / stop edge

The PLC sets this from decoded HLC demand. In `mode=2`, the external controller should treat it as the latest EV request snapshot and answer with fresh `CTRL_POWER_SETPOINT` and `CTRL_HLC_FEEDBACK`.

## 6. Controller Call Order

## 6.1 `mode=1` Recommended Flow

1. Send `CTRL_HEARTBEAT` every `<= 400 ms`.
2. Keep `CTRL_AUTH pending` alive until charging is allowed.
3. Build the module assignment:
   - `CTRL_ALLOC_BEGIN`
   - `CTRL_ALLOC_DATA` for each module
   - `CTRL_ALLOC_COMMIT`
4. When policy allows, send `CTRL_SLAC arm` and `CTRL_SLAC start`.
5. Wait for SLAC match and HLC activity.
6. Once identity/business logic allows charging, refresh `CTRL_AUTH grant`.
7. Let the PLC own precharge/current-demand targets and Relay1 behavior.
8. For stop:
   - `CTRL_SESSION stop_soft` or `stop_hard`
   - `CTRL_AUTH deny`
   - `CTRL_SLAC disarm`
9. Wait for:
   - `PLC_SESSION_STATUS.stop_complete=1`
   - `PLC_POWER_STATUS assigned=0 active=0`

## 6.2 `mode=2` Recommended Flow

Use the same sequence as `mode=1`, plus a managed power loop:

1. Watch `PLC_BMS_STATUS` for the latest EV target.
2. Send `CTRL_POWER_SETPOINT` at a stable cadence faster than the `1200 ms` timeout.
3. Send `CTRL_HLC_FEEDBACK` at a stable cadence faster than the `1200 ms` timeout.
4. Command Relay1 through `CTRL_AUX_RELAY` bit 2 when the controller wants the contactor closed.
5. On stop or deny, immediately:
   - `CTRL_POWER_SETPOINT enable=0`
   - `CTRL_HLC_FEEDBACK valid=1 ready=0`
   - open Relay1

In `mode=2`, controller mistakes directly affect what the EV sees, so the power/feedback loop should be treated as a real-time control loop, not a best-effort background update.

## 7. Stop, Shutdown, and Release Semantics

### 7.1 Stop Guarantees

When stop is triggered by command, watchdog, disarm, abort, disconnect, or HLC teardown:

- modules are turned off
- Relay1 is opened
- managed power and managed feedback caches are cleared
- active allowlist is released when the stop path reaches the empty-assignment state

`controller_stop_is_complete()` requires:

- Relay1 open
- module output disabled
- no SLAC session active
- no HLC active client
- allowlist empty

Operationally, a fully released power path is best confirmed with both:

- `PLC_SESSION_STATUS.stop_complete=1`
- `PLC_POWER_STATUS assigned=0 active=0`

If the EV remains physically plugged in after stop, CP usually settles at `B` or `B2`. That is connected idle, not active charging, and should not be interpreted as a failed stop.

### 7.2 Soft vs Hard Stop

- soft stop:
  - starts graceful stop with deadline
  - escalates to hard behavior if deadline expires
- hard stop:
  - immediate force-safe-stop path

## 8. Watchdogs and Automatic Reactions

Controller-mode watchdogs:

- heartbeat loss:
  - clears SLAC arm/start immediately after real loss
  - safe stop after `3000 ms` if energy path active
  - safe stop after `15000 ms` if only session/HLC active
- auth expiry from `Granted`:
  - becomes `Denied`
  - modules off
  - Relay1 open
- SLAC arm TTL expiry:
  - clears `slac_armed` and `slac_start_latched`
- allocation txn TTL expiry:
  - staged transaction dropped
  - timeout ACK emitted
- Relay1/Relay2/Relay3 hold timeout:
  - relay auto-opens
- managed power timeout in `mode=2`:
  - modules off
  - Relay1 open
- managed feedback timeout in `mode=2`:
  - feedback cache cleared
  - HLC responses fall back to not-ready / zero-current behavior

## 9. HLC Power Path Reactions

### 9.1 PreCharge

- `mode=0/1`
  - PLC applies requested voltage locally
  - current is limited to `2 A`
  - convergence is based on present voltage vs requested voltage
- `mode=2`
  - PLC publishes demand in `PLC_BMS_STATUS`
  - controller feedback drives `EVSEReady` / present voltage in the response
  - PLC does not compute the managed power response on its own

### 9.2 CurrentDemand

- `mode=0/1`
  - PLC applies requested voltage/current locally if energy allowed
  - telemetry and limit flags are derived from module status
- `mode=2`
  - PLC publishes demand in `PLC_BMS_STATUS`
  - controller feedback drives `ready`, present voltage/current, and limit flags
  - PLC does not synthesize those fields from local power telemetry

### 9.3 PowerDelivery Stop and SessionStop

All modes:

- clear delivery enable state
- modules off
- Relay1 open

`mode=2` also clears managed power cache so the next session cannot inherit stale controller intent.

## 10. Serial Developer API

Serial commands are a local controller emulator and debug API.

Commands:

- `CTRL HB [timeout_ms]`
- `CTRL AUTH <deny|pending|grant> [ttl_ms]`
- `CTRL SLAC <disarm|arm|start|abort> [arm_ms]`
- `CTRL RESET`
- `CTRL ALLOC1`
- `CTRL ALLOC BEGIN <txn> <expected> [ttl_ms] [limit_kw]`
- `CTRL ALLOC DATA <txn> <module_group> <module_addr> [limit_kw]`
- `CTRL ALLOC COMMIT <txn>`
- `CTRL ALLOC ABORT`
- `CTRL RELAY <enable_mask> <state_mask> [hold_ms]`
- `CTRL POWER <enable0|1> <voltage_v> <current_a>`
- `CTRL FEEDBACK <valid0|1> <ready0|1> <present_v> <present_i> [curr_lim0|1] [volt_lim0|1] [pwr_lim0|1]`
- `CTRL STOP <soft|hard|clear> [timeout_ms]`
- `CTRL MODE <mode0|1|2> <plc_id 1..15> [controller_id 1..15]`
- `CTRL OWNERSHIP <connector_id> <module_addr>`
- `CTRL SAVE`
- `CTRL STATUS`

Mode tokens accepted:

- `0`, `standalone`, `local`
- `1`, `controller`, `controller_supported`, `supported`
- `2`, `managed`, `controller_managed`, `full`

Notes:

- `CTRL ALLOC1` is a convenience helper that allocates the current local module in the current local group with the module max-power limit.
- `CTRL STOP` is valid in all 3 modes. In `mode=0`, it acts as a direct local operator/debug safe-stop path.
- `CTRL MODE` changes runtime config in RAM only; `CTRL SAVE` persists it
- `CTRL RESET` clears controller runtime state and performs safe stop
- `CTRL STATUS` reports current mode and managed power / feedback freshness flags

## 11. SW4 Setup Portal

If SW4 is held at boot:

- firmware starts AP `CBPLC-SETUP-<mac-suffix>`
- serves setup form on `/` for 120 seconds
- `POST /save` persists config to NVS and reboots

Fields:

- `mode`
- `slac_requires_controller_start`
- `plc_id`
- `connector_id`
- `controller_id`
- `local_module_address`
- heartbeat timeout
- auth TTL
- alloc TTL
- SLAC arm timeout

For backward compatibility, the form handler still accepts legacy `standalone=<0|1>`, but it normalizes and stores `mode`.

## 12. Edge Cases Integrators Should Expect

- wrong PLC id or controller id nibble -> frame ignored
- bad CRC or wrong protocol version -> rejected with generic ACK
- duplicate or stale sequence -> `ACK_BAD_SEQ`
- unknown module address or bad group in alloc data -> `ACK_BAD_VALUE`
- commit with missing unique modules -> `ACK_BAD_VALUE`
- heartbeat seen once then lost -> controller permissions collapse and stop logic starts
- auth pending forever -> HLC may remain in authorization ongoing, but energy stays blocked
- controller disarm/abort during active charge -> immediate safe stop
- allowlist change during active charge -> PLC retargets the new module set
- managed mode with missing power updates -> modules off + Relay1 open
- managed mode with missing feedback updates -> EV replies fall back to not-ready semantics
