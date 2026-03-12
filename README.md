# CB PLC Firmware (ESP32-S3)

Integrated EVSE firmware built from:

- `cbslac` for SLAC matching over QCA7000
- `jpv2g` for ISO 15118 / DIN 70121 HLC SECC handling
- `cbmodules` for local and leased DC power-module control

This README describes the current runtime behavior of the firmware end to end: boot flow, the 3 runtime modes, multi-module control, runtime allocation and deallocation, controller-managed charging, and the edge cases that are handled in code today.

## Documentation

- `README.md`
  Runtime architecture, mode behavior, charging flow, and safety handling.
- `DEVELOPER_CONTROL_PLANE_API.md`
  Wire-level controller CAN contract, serial developer API, and status uplinks.
- `IMPLEMENTATION_TODO.md`
  Remaining backlog items and validation gaps.

## Runtime Model

Each PLC is one EVSE endpoint. The firmware owns:

- CP sampling, debounce, and PWM advertisement
- QCA7000 transport and SLAC state machine
- HLC server lifecycle and EV request decoding
- Relay outputs and module-manager integration
- Controller CAN ingress validation and command watchdogs
- PLC-to-controller status publication

Persisted runtime config in NVS:

- `mode`
- `slac_requires_controller_start`
- `plc_id`
- `connector_id`
- `controller_id`
- `local_module_address`
- `can_node_id`
- controller heartbeat/auth/allocation/SLAC timeout values

Derived runtime identity:

- local logical group id: `PLC_GROUP_<plc_id>`
- local module id: `PLC<plc_id>_MXR_<module_addr>`
- `cbmodules` owner id: `(controller_id << 8) | plc_id`

Important defaults:

- generic source build default is `mode=0`
- dedicated `controller-plc1` and `controller-plc2` PlatformIO envs default to `mode=1`
- config version `3` is migrated on boot:
  - legacy `standalone_mode=true` -> `mode=0`
  - legacy `standalone_mode=false` -> `mode=1`

## Operating Modes

The old boolean `standalone_mode` is replaced by `mode`.

### `mode=0` Standalone

The PLC is locally autonomous:

- controller heartbeat/auth/SLAC/energy gates short-circuit to allowed
- boot keeps the local module in the active allowlist
- local module group enables efficient module usage
- CP connection alone can advertise PWM and start SLAC
- HLC authorization is local
- module targets are applied directly from HLC requests
- Relay1 is PLC-controlled

This is the self-contained path: CP -> SLAC -> HLC -> modules -> Relay1.

### `mode=1` External Controller

The controller owns the entire power side. The PLC keeps CP, SLAC, and HLC transport:

- heartbeat, auth, SLAC permission, and allocation are still controller-owned
- PLC publishes live EV demand to the controller through `PLC_BMS_STATUS`
- controller sends live `CTRL_POWER_SETPOINT` commands for module output
- controller sends live `CTRL_HLC_FEEDBACK` commands for the values the PLC returns to the EV
- controller may directly command Relay1 through `CTRL_AUX_RELAY` bit 2
- if managed power commands or managed feedback go stale, the PLC fails safe

In `mode=1`, the PLC is the protocol endpoint and safety executor, but not the charging policy or power-control brain.

## Relay Ownership

- Relay1 is the gun contactor.
- Relay2 and Relay3 are auxiliary relays.

By mode:

- `mode=0`: Relay1 is PLC-owned, Relay2/3 may still be controller-commanded.
- `mode=1`: Relay1, Relay2, and Relay3 may all be commanded through `CTRL RELAY`.

Even in `mode=1`, local hard-stop conditions override controller intent. CP disconnect, heartbeat-loss safe stop, auth denial/expiry, SLAC abort/disarm, HLC session stop, and explicit stop paths all open Relay1 locally.

## Boot Sequence

On every boot the firmware:

1. starts serial logging
2. loads runtime config from NVS and migrates legacy config if needed
3. normalizes mode, ids, addresses, and timeout fields
4. refreshes derived local group/module identity
5. reads local MAC and opens all relays
6. optionally starts the SW4 setup portal for 120 seconds
7. configures CP PWM at 100 percent
8. seeds the local module inventory and initializes `cbmodules`
9. seeds the local allowlist, then clears it again if `mode` requires controller ownership
10. publishes local identity to the controller uplink
11. initializes the QCA7000 transport and EVSE SLAC FSM, retrying every 1 second on failure

Main loop order is fixed:

1. serial developer commands
2. QCA ingress
3. raw controller CAN polling
4. controller RX dispatch
5. controller watchdogs
6. periodic PLC status TX
7. CP / SLAC / HLC progression
8. module-manager service

That order matters because same-iteration controller heartbeats, auth, allocation, power, and feedback are all visible before the charging gates are evaluated.

## End-to-End Charging Flow

### CP and PWM

CP is sampled and filtered into `A/B/C/D/E/F` states with:

- connect debounce
- longer disconnect debounce
- connected-hold filtering to absorb short disconnect spikes

PWM behavior:

- `100%` when not advertising charge
- `5%` only when CP is connected, no hold is active, and the mode-specific permission gate allows it

Mode effect:

- `mode=0`: advertise immediately after stable plug-in
- `mode=1`: advertise only when controller gating permits SLAC start, unless a session is already active

### `mode=0` Session Path

1. EV plugs in and CP becomes stable.
2. SLAC starts automatically.
3. On SLAC match, the PLC publishes EV MAC and starts HLC.
4. HLC authorization follows the autonomous local path.
5. `PreChargeReq` applies requested voltage with current limited to `2 A`.
6. Once present voltage converges, Relay1 is allowed to close.
7. `CurrentDemandReq` keeps updating module voltage/current targets.
8. `PowerDelivery stop`, `SessionStop`, CP disconnect, HLC exit, or SLAC failure tears the session down locally.

### `mode=1` Session Path

1. Controller keeps heartbeat, auth, and allocation fresh.
2. PLC handles CP, SLAC, and HLC transport exactly as before.
3. On every `PreChargeReq`, `CurrentDemandReq`, and `PowerDelivery` update, the PLC publishes the EV/BMS demand through `PLC_BMS_STATUS`.
4. Controller responds with:
   - `CTRL_POWER_SETPOINT` for module enable + target voltage/current
   - `CTRL_HLC_FEEDBACK` for the values the PLC should report back to the EV
   - optional Relay1 command through `CTRL RELAY 0x04 ...`
5. During precharge, the controller usually keeps Relay1 open while sending notional present voltage feedback.
6. During current demand, the controller keeps power and feedback fresh while holding Relay1 closed if charge should continue.
7. If managed power or managed feedback stop refreshing for more than `1200 ms`, the PLC fails safe:
   - stale power command -> modules off + Relay1 open
   - stale feedback -> EV replies degrade to not-ready / zero-current behavior

Important `mode=1` detail:

- `CTRL_POWER_SETPOINT` does not close Relay1 by itself
- `CTRL_HLC_FEEDBACK` does not energize modules by itself
- controller must keep power, feedback, auth, heartbeat, allocation, and Relay1 intent aligned

## Module Control and Multiple Modules

### Local and Remote Modules

The PLC always knows its own local module address. Remote modules are introduced at runtime by controller allocation traffic.

Naming:

- local module ids: `PLC<plc_id>_MXR_<addr>`
- leased remote module ids: `LEASED_MXR_<addr>`

For every leased module the controller provides:

- module CAN group
- module CAN address
- optional per-module power limit

When a remote module first appears in allocation, the PLC upserts it into `cbmodules`. Remote objects stay cached across later releases so the next lease does not need a full cold rediscovery.

### What Reaches `cbmodules`

Controller-plane CAN frames never reach `cbmodules`. They are intercepted first and handled by the PLC control-plane parser.

Module CAN ingress is filtered to:

- the local module
- active allowlist modules
- staged allocation modules
- ownership traffic for those same module addresses

That means:

- inactive foreign modules are ignored
- staged modules can already be observed before commit
- removing a module from the allowlist removes it from runtime control even if its cached object remains in memory

### Group Interpretation

The PLC controls one logical `cbmodules` group per PLC.

Aggregation rules:

- combined current = sum of fresh module currents
- combined voltage = average of fresh module voltages
- combined power = `V * I`

Modules are excluded from the aggregate if they are stale, faulted, isolation-latched, leased away, or not part of the active runtime group.

Freshness:

- module telemetry freshness window: `10000 ms`
- normal module service interval: `10 ms`
- SLAC-critical service interval: `50 ms`

### Power Limits

There are two limit layers:

- assignment-wide power limit for the whole allowlist
- optional per-module power limits for leased modules

When a target current is applied:

- requested voltage/current are sanitized to non-negative values
- assignment-wide limit clamps current by available power
- `cbmodules` performs the final capability clamp on the live allowlist

`mode=0` also enables `efficient_module_usage` on the group. Controller-owned modes do not.

## Runtime Allocation and Deallocation

Allocation is transaction-based in controller modes.

### Begin

`CTRL_ALLOC_BEGIN` starts a staged transaction with:

- transaction id
- expected module count
- transaction TTL
- optional assignment-wide power limit

The staged allowlist and staged per-module limits are reset on every new transaction.

### Data

Each `CTRL_ALLOC_DATA` frame contributes:

- transaction id
- module CAN group
- module CAN address
- optional per-module power limit

Handled behavior:

- bad groups are rejected
- unknown module addresses are rejected
- duplicate module addresses are deduplicated
- staged per-module limits are stored by module id

### Commit

`CTRL_ALLOC_COMMIT` succeeds only when:

- a transaction is active
- transaction id matches
- staged unique module count exactly matches the declared expected count

Commit behavior:

- staged power limits are normalized to the staged allowlist
- new limits are applied before the allowlist swap
- the new allowlist is pushed through `cbmodules::apply_group_setpoint_allowlist`
- if output is already active and the live set changed, the last requested target is re-applied to the new set
- if the first commit attempt fails and remote modules are involved, the PLC retries discovery/polling for up to `1500 ms`

### Same-Allowlist Refresh

If the new allowlist is identical to the active one and only the lease needs refreshing, the firmware refreshes the group allowlist lease instead of doing a full transition.

### Release and Deallocation

If the allowlist becomes empty:

- output is forced off
- requested current is reset
- assignment-wide limit is cleared
- per-module hard limits are restored to defaults
- remote ownership state is pruned back to the empty set

Release can happen because of:

- `CTRL_ALLOC_ABORT` while idle
- explicit stop or safe stop
- auth deny / expiry
- mode transition away from standalone
- controller-managed power timeout
- internal release after the power path has already been driven to zero

Release is only fully complete when all of these are true:

- Relay1 is open
- module output is disabled
- allowlist is empty
- group telemetry reports `assigned=0`
- group telemetry reports `active=0`

## Edge Cases and Safety Handling

Handled explicitly in current code:

- controller CAN frames with wrong target, wrong controller id, bad version, bad CRC, non-extended format, or short DLC are ignored before command handling
- heartbeat, auth, SLAC, allocation, aux-relay, session, managed power, and managed feedback all keep independent sequence tracking
- duplicates and stale backward jumps are rejected as bad sequence
- heartbeat session-marker change resets command sequence state
- before the first valid heartbeat arrives, controller modes wait rather than treating the link as failed
- after real heartbeat loss, SLAC arm/start is cleared immediately
- heartbeat loss forces safe stop after `3000 ms` if energy is active, or after `15000 ms` if session/HLC is active
- auth expiry or explicit deny turns modules off and opens Relay1
- SLAC arm expiry clears the arm/start latch
- `DISARM` and `ABORT` both force safe stop
- CP disconnect always tears down SLAC, HLC, modules, and Relay1
- `PowerDelivery stop`, `SessionStopReq`, and HLC worker exit all force modules off and open Relay1
- allocation timeout auto-aborts the staged transaction
- allocation commit with missing or duplicate-only data is rejected
- live allowlist shrink or expansion during a running session re-targets the new module set using the last requested target
- local module isolation is only auto-cleared when it is safe to do so
- Relay2 and Relay3 hold timers auto-open on timeout
- Relay1 hold timer is only available in `mode=1`
- mode changes always safe-stop first, then reseed or clear the allowlist according to the new mode
- managed power and managed feedback are cleared on stop, auth deny, session start, and session stop so stale controller state cannot leak into the next session
- if a stop happens while the EV remains plugged in, CP normally falls back to connected idle (`B` / `B2`) rather than disconnected `A`; that is still a successful stop with charging disabled

## Serial Developer API

Useful commands:

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
- `CTRL MODE <mode0|1> <plc_id> [controller_id]`
- `CTRL OWNERSHIP <connector_id> <module_addr>`
- `CTRL SAVE`
- `CTRL STATUS`

Mode tokens accepted by `CTRL MODE`:

- `0`, `standalone`, `local`
- `1`, `external`, `external_controller`, `uart`, `router`, `controller_uart_router`, `uart_router`
- legacy aliases `2`, `3`, `controller`, `controller_supported`, `managed`, `controller_managed`, `full` are accepted and normalized to `mode=1`

Notes:

- `CTRL ALLOC1` is a convenience helper that allocates the current local module in the current local group with the module max-power limit.
- `CTRL STOP` is valid in both modes. In `mode=0`, it acts as a direct local operator/debug safe-stop even though no external controller is required.
- `CTRL MODE` changes runtime config immediately in RAM; `CTRL SAVE` persists it
- `CTRL OWNERSHIP` updates connector/module identity in RAM; `CTRL SAVE` persists it
- `CTRL MODE` does not change `can_node_id`; use the build environment or SW4 setup portal for that

## Setup Portal

If SW4 is held at boot, the firmware starts an AP and serves a setup form for 120 seconds.

Fields include:

- `mode`
- `slac_requires_controller_start`
- `plc_id`
- `connector_id`
- `controller_id`
- `local_module_address`
- timeout fields

The portal still accepts the legacy `standalone` argument for backward compatibility, but it stores the normalized `mode` field.

## Example Harnesses

### `tools/mode2_uart_vehicle_charge_test.py`

Primary end-to-end UART-router harness:

- boots both PLCs into `mode=1`
- resolves borrower/donor PLC ids dynamically from live `CTRL STATUS`
- drives controller heartbeat/auth/SLAC/feedback over UART
- drives module CAN directly, matching the production controller architecture
- validates precharge, current demand, relay behavior, and session stop on live hardware

### `tools/mode2_serial_module_transition_test.py`

Focused transition harness for module/relay attach-release behavior in `mode=1`.

## Validation Snapshot

Latest manual validation captured in this repo on `2026-03-09`:

- build verification passed for:
  - default `esp32-s3-devkitc-1`
  - `controller-plc1`
  - `controller-plc2`
- live `mode=0` charging on `/dev/ttyACM0` passed for:
  - `10` minute window, log: `logs/mode0_live_10m_20260309_c.log`
  - `15` minute window, log: `logs/mode0_live_15m_20260309.log`
- live stop-path verification on `/dev/ttyACM0` passed during an in-progress `30` minute run:
  - stop command: `CTRL STOP hard 3000`
  - observed result: Relay1 opened, CP returned to `B2`, group state became `en=0 assigned=0 active=0`, module output current dropped to `0.00 A`
- still pending:
  - full uninterrupted `30` minute `mode=0` pass result

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

## Provision Runtime Identity

After flashing, provision over serial and reboot:

Standalone board:

```text
CTRL MODE 0 <plc_id> <controller_id>
CTRL OWNERSHIP <connector_id> <module_addr>
CTRL SAVE
```

Controller-backed board:

```text
CTRL MODE 1 <plc_id> <controller_id>
CTRL OWNERSHIP <connector_id> <module_addr>
CTRL SAVE
```

Commissioning guidance:

- use `controller-plc1` / `controller-plc2` or `standalone-plc1` / `standalone-plc2` PlatformIO envs when board identity is known at build time
- use `CTRL STATUS` after boot to verify mode, CP phase, SLAC gate, energy gate, allowlist size, and controller-managed freshness flags
- if you need to change runtime config without serial, hold SW4 during boot and use the setup portal
