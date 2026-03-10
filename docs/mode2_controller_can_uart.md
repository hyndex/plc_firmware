# Mode 2 Controller Interface: CAN and UART

This document describes the implemented controller interface for `mode=2` (`controller_managed`) in the PLC firmware.

It covers:
- identity and addressing
- how CAN targeting/filtering works
- every controller CAN command and every PLC CAN status frame
- every UART/serial `CTRL ...` command
- the end-to-end charging flow used by the controller scripts

## 1. Identity and Addressing

The runtime identity fields are:

- `plc_id`: logical PLC identity, `1..15`
- `connector_id`: logical connector identity
- `controller_id`: controller identity, `1..15`
- `can_node_id`: CAN node nibble used in the low 4 bits of controller/PLC CAN arbitration IDs
- `local_module_address`: local module address on the shared CAN/module side

Runtime config stores these in `RuntimeConfig`.

Important normalization rules:

- `plc_id == 0` is normalized into `1..15`
- `connector_id == 0` becomes `plc_id`
- `can_node_id == 0` becomes `plc_id`
- `local_module_address == 0` becomes `plc_id`

That means every PLC has a stable `plc_id`, and `connector_id` can either be explicitly provisioned or default to the same value.

Current controller build profiles:

| Build env | plc_id | connector_id | controller_id | local_module_address | can_node_id |
|---|---:|---:|---:|---:|---:|
| `controller-plc1` | 1 | 1 | 7 | 1 | 10 |
| `controller-plc2` | 2 | 2 | 7 | 3 | 11 |

Practical consequence:

- on CAN, the controller addresses a PLC by `can_node_id`, not directly by `connector_id`
- the controller maps a PLC to a connector through the PLC identity data: `plc_id + connector_id`
- on serial, the controller learns identity from `CTRL STATUS` or boot `[CFG]`

## 2. CAN Transport

### 2.1 Arbitration ID layout

Controller to PLC command bases:

- `0x18FF5000` heartbeat
- `0x18FF5010` auth
- `0x18FF5020` SLAC
- `0x18FF5030` alloc begin
- `0x18FF5040` alloc data
- `0x18FF5050` alloc commit
- `0x18FF5060` alloc abort
- `0x18FF5070` aux relay
- `0x18FF5080` session
- `0x18FF5090` power setpoint
- `0x18FF50A0` HLC feedback

PLC to controller status bases:

- `0x18FF6000` heartbeat
- `0x18FF6010` CP status
- `0x18FF6020` SLAC status
- `0x18FF6030` HLC status
- `0x18FF6040` power status
- `0x18FF6050` session status
- `0x18FF6060` identity event
- `0x18FF6070` command ACK
- `0x18FF6080` BMS status

The low nibble of the arbitration ID is the target/source `can_node_id`.

Example:

- PLC1 built as `controller-plc1` uses `can_node_id=10`, so controller commands go to `...A`
- PLC2 built as `controller-plc2` uses `can_node_id=11`, so controller commands go to `...B`

### 2.2 CAN filtering and targeting

Important: the MCP2515 hardware filter is currently configured as accept-all for extended frames.

That is intentional because the same CAN controller receives:

- controller control-plane frames
- module telemetry
- module ownership traffic

The actual selectivity is enforced in firmware software:

- controller frames are only accepted when the arbitration ID low nibble matches local `can_node_id`
- controller frames are also rejected unless payload byte 2 low nibble matches local `controller_id`
- module traffic is only accepted for the local module or addresses currently present in the active/staged allowlist

So the answer to "does the PLC only accept messages addressed to itself?" is:

- yes for controller traffic, functionally
- but enforcement is software-level, not a restrictive MCP2515 hardware mask

### 2.3 Common controller CAN command format

Bytes for controller-to-PLC commands:

- `byte0`: protocol version, currently `1`
- `byte1`: sequence number, per command family
- `byte2`: controller ID in low nibble
- `byte3..byte6`: command-specific payload
- `byte7`: CRC-8 over bytes `0..6`

If CRC, version, target controller ID, or sequence is wrong, the PLC rejects the frame and may respond with an ACK error.

ACK status codes:

- `0`: OK
- `1`: bad CRC
- `2`: bad version
- `3`: bad target
- `4`: bad sequence
- `5`: bad state
- `6`: bad value

### 2.4 Every controller CAN command

#### `CAN_ID_CTRL_HEARTBEAT` `0x18FF500N`

Payload:

- `byte3`: heartbeat timeout in 100 ms units
- `byte4..6`: session marker, little-endian 24-bit

Effect:

- refreshes controller liveness
- a new session marker causes command-sequence resync
- on first heartbeat or session-marker change, the PLC republishes its local identity over CAN

ACK:

- cmd `0x10`

#### `CAN_ID_CTRL_AUTH` `0x18FF501N`

Payload:

- `byte3`: auth state
  - `0` deny
  - `1` pending
  - `2` granted
- `byte4`: TTL in 100 ms units

Effect:

- drives HLC authorization policy
- `deny` also clears managed power/feedback caches, turns modules off, and opens relay1

ACK:

- cmd `0x11`
- `detail0 = auth state`

#### `CAN_ID_CTRL_SLAC` `0x18FF502N`

Payload:

- `byte3`: command
  - `0` disarm
  - `1` arm
  - `2` start now
  - `3` abort
- `byte4`: arm timeout in 100 ms units

Effect:

- gates whether SLAC may start
- `disarm` and `abort` force a safe stop

ACK:

- cmd `0x12`
- `detail0 = command`

#### `CAN_ID_CTRL_ALLOC_BEGIN` `0x18FF503N`

Payload:

- `byte3`: transaction ID
- `byte4`: expected module count
- `byte5`: TTL in 100 ms units
- `byte6`: total assignment power limit in 0.5 kW units

Effect:

- begins a staged allocation transaction

ACK:

- cmd `0x13`
- `detail0 = txn_id`
- `detail1 = expected_modules`

#### `CAN_ID_CTRL_ALLOC_DATA` `0x18FF504N`

Payload:

- `byte3`: transaction ID
- `byte4`: module group
- `byte5`: module address
- `byte6`: per-module power limit in 0.5 kW units

Effect:

- stages one module into the pending allowlist

ACK:

- cmd `0x14`
- `detail0 = module_addr`
- `detail1 = staged_count`

#### `CAN_ID_CTRL_ALLOC_COMMIT` `0x18FF505N`

Payload:

- `byte3`: transaction ID

Effect:

- validates staged allocation
- resolves module IDs from module addresses
- applies allowlist and per-module limits

ACK:

- cmd `0x15`
- `detail0 = resulting allowlist size`

#### `CAN_ID_CTRL_ALLOC_ABORT` `0x18FF506N`

Payload:

- none

Effect:

- clears staged allocation transaction
- if there is no active session/HLC/relay, it may also release the current allowlist

ACK:

- cmd `0x16`

#### `CAN_ID_CTRL_AUX_RELAY` `0x18FF507N`

Payload:

- `byte3`: enable mask
- `byte4`: state mask
- `byte5`: hold time in 100 ms units

Bit meaning:

- bit `0x01`: relay2
- bit `0x02`: relay3
- bit `0x04`: relay1

Effect:

- relay1 control is only accepted in `mode=2`
- relay1 close is rejected unless energy is allowed and CP is connected
- if a hold time is provided, the relay auto-deadline is armed

ACK:

- cmd `0x17`
- `detail0 = enable_mask`
- `detail1 = state_mask`

#### `CAN_ID_CTRL_SESSION` `0x18FF508N`

Payload:

- `byte3`: action
  - `0` none
  - `1` soft stop
  - `2` hard stop
  - `3` clear
- `byte4`: timeout in 100 ms units
- `byte5`: reason code

Effect:

- `soft/hard` starts controller-owned stop policy
- `clear` clears stop-active state and caches

ACK:

- cmd `0x18`
- `detail0 = action`
- `detail1 = stop_complete(0/1)`

#### `CAN_ID_CTRL_POWER_SETPOINT` `0x18FF509N`

Payload bytes `3..6` are a packed little-endian 32-bit value:

- bit `0`: enable
- bits `1..14`: target voltage in `0.1 V`
- bits `15..26`: target current in `0.1 A`

Effect:

- only valid in `mode=2`
- when `enable=1`, PLC drives modules to target V/I
- when `enable=0`, PLC turns modules off

ACK:

- cmd `0x19`
- `detail0 = enable(0/1)`

#### `CAN_ID_CTRL_HLC_FEEDBACK` `0x18FF50AN`

Payload bytes `3..6` are a packed little-endian 32-bit value:

- bit `0`: ready
- bit `1`: current limit achieved
- bit `2`: voltage limit achieved
- bit `3`: power limit achieved
- bits `4..17`: present voltage in `0.1 V`
- bits `18..29`: present current in `0.1 A`
- bit `30`: valid

Effect:

- only valid in `mode=2`
- feeds the HLC state machine with present V/I and readiness

ACK:

- cmd `0x1A`
- `detail0 = valid(0/1)`
- `detail1 = ready(0/1)`

### 2.5 Every PLC CAN status frame

#### `CAN_ID_PLC_HEARTBEAT` `0x18FF600N`

- `byte2`: current mode
- `byte3`: CP state character
- `byte4`: bitfield
  - bit0 `hlc_ready`
  - bit1 `hlc_active`
  - bit2 `module_output_enabled`
- `byte5..6`: uptime seconds, little-endian low 16 bits

Period: 500 ms

#### `CAN_ID_PLC_CP_STATUS` `0x18FF601N`

- `byte2`: CP state character
- `byte3`: PWM duty percent
- `byte4..5`: CP millivolts, little-endian
- `byte6`: connected-since in 100 ms units low byte

Period: 200 ms

#### `CAN_ID_PLC_SLAC_STATUS` `0x18FF602N`

- `byte2`: `session_started`
- `byte3`: `session_matched`
- `byte4`: raw FSM state
- `byte5`: SLAC failure count for current CP session
- `byte6`: bitfield
  - bit0 `slac_armed`
  - bit1 `slac_start_latched`

Period: 500 ms

#### `CAN_ID_PLC_HLC_STATUS` `0x18FF603N`

- `byte2`: bitfield
  - bit0 `hlc_ready`
  - bit1 `hlc_active`
- `byte3`: auth state
- `byte4`: heartbeat alive
- `byte5`: auth granted
- `byte6`: bitfield
  - bit0 `precharge_seen`
  - bit1 `precharge_converged`

Period: 500 ms

#### `CAN_ID_PLC_POWER_STATUS` `0x18FF604N`

- `byte2..3`: combined group voltage in `0.1 V`
- `byte4..5`: combined group current in `0.1 A`
- `byte6`: low nibble `active_modules`, high nibble `assigned_modules`

Period: 200 ms

#### `CAN_ID_PLC_SESSION_STATUS` `0x18FF605N`

- `byte2`: bitfield
  - bit0 `session_started`
  - bit1 `session_matched`
- `byte3`: bitfield
  - bit0 `relay1_closed`
  - bit1 `relay2_closed`
  - bit2 `relay3_closed`
  - bit3 `stop_active`
  - bit4 `stop_hard`
  - bit5 `stop_complete`
- `byte4..5`: energy Wh low 16 bits
- `byte6`: EV SOC percent if known

Period: 500 ms

#### `CAN_ID_PLC_IDENTITY_EVT` `0x18FF606N`

- `byte2`: event kind
- `byte3`: event ID
- `byte4`: high nibble total segments, low nibble current segment index
- `byte5..6`: 2 bytes of segment payload

Event kinds:

- `1`: local PLC MAC
- `2`: EV MAC
- `3`: EVCCID
- `4`: EMAID
- `5`: local routing identity

Event kind `5` payload is 7 bytes:

- `byte0`: `plc_id`
- `byte1`: `connector_id`
- `byte2`: `controller_id`
- `byte3`: `local_module_address`
- `byte4`: `local_group`
- `byte5`: owner ID high byte
- `byte6`: owner ID low byte

Publish timing:

- once at boot
- again on first accepted controller heartbeat
- again when the controller heartbeat session marker changes

#### `CAN_ID_PLC_CMD_ACK` `0x18FF607N`

- `byte2`: command type
- `byte3`: echoed sequence number
- `byte4`: ACK status
- `byte5`: detail0
- `byte6`: detail1

Sent in response to accepted command traffic or certain watchdog faults.

#### `CAN_ID_PLC_BMS_STATUS` `0x18FF608N`

- `byte2..3`: requested EV voltage in `0.1 V`
- `byte4..5`: requested EV current in `0.1 A`
- `byte6`: bitfield
  - bit0 `valid`
  - bit1 `delivery_ready`
  - bits2..7 `hlc_stage`

Period: 250 ms or immediately when fresh BMS data changes.

## 3. UART / Serial Controller Interface

### 3.1 Transport model

UART/USB serial is a newline-delimited text interface.

Only lines starting with `CTRL` are handled by the controller command parser.

Important implementation detail:

- serial commands do not bypass controller logic
- they are converted into the same internal controller RX frames that CAN uses
- the injection path is `serial_inject_controller_frame(can_with_plc_id(...))`

So CAN and serial ultimately exercise the same controller-owned state machine.

### 3.2 Stable serial identity/status outputs

Boot/config line:

- `[CFG] mode=... plc_id=... can_node_id=... connector_id=... controller_id=... module_addr=... local_group=... owner_id=...`

On-demand stable controller status:

- `CTRL STATUS`
- response:
  - `[SERCTRL] STATUS mode=... plc_id=... connector_id=... controller_id=... module_addr=... local_group=... module_id=... cp=... duty=... hb=... auth=... allow_slac=... allow_energy=... armed=... start=... alloc_sz=... ctrl_power=... ctrl_fb=... stop_active=... stop_hard=... stop_done=...`

ACK line:

- `[SERCTRL] ACK cmd=0x.. seq=.. status=.. detail0=.. detail1=..`

Controller scripts should use `CTRL STATUS` as the authoritative serial identity/status query for deciding which connector/PLC they are attached to.

### 3.3 Every serial command

#### `CTRL HELP`

Prints command help.

#### `CTRL HB [timeout_ms]`

Serial equivalent of CAN heartbeat.

#### `CTRL AUTH <deny|pending|grant|0|1|2> [ttl_ms]`

Serial equivalent of CAN auth command.

#### `CTRL SLAC <disarm|arm|start|start_now|abort|0|1|2|3> [arm_ms]`

Serial equivalent of CAN SLAC command.

#### `CTRL RESET`

- clears controller runtime state
- resets serial sequence counters
- forces a safe stop

#### `CTRL ALLOC1`

Convenience helper:

- creates a one-module allocation transaction
- uses local group and local module address
- equivalent to:
  - `ALLOC BEGIN`
  - `ALLOC DATA`
  - `ALLOC COMMIT`

#### `CTRL ALLOC BEGIN <txn> <expected> [ttl_ms] [limit_kw]`

Stage allocation transaction begin.

#### `CTRL ALLOC DATA <txn> <module_group> <module_addr> [limit_kw]`

Stage one module entry.

#### `CTRL ALLOC COMMIT <txn>`

Commit staged allocation.

#### `CTRL ALLOC ABORT`

Abort staged allocation.

#### `CTRL RELAY <enable_mask> <state_mask> [hold_ms]`

Serial equivalent of aux relay command.

Relay bits:

- `0x01` relay2
- `0x02` relay3
- `0x04` relay1

#### `CTRL POWER <enable0|1> <voltage_v> <current_a>`

Serial equivalent of managed power setpoint.

#### `CTRL FEEDBACK <valid0|1> <ready0|1> <present_v> <present_i> [curr_lim0|1] [volt_lim0|1] [pwr_lim0|1]`

Serial equivalent of managed HLC feedback.

#### `CTRL STOP <soft|hard|clear> [timeout_ms]`

Serial equivalent of session stop/clear.

#### `CTRL MODE <mode0|1|2> <plc_id> [controller_id]`

Changes runtime mode and updates PLC/controller identity in RAM.

Important:

- this is not persistent by itself
- use `CTRL SAVE` and reboot if you want it stored in NVS

Modes:

- `0`: standalone
- `1`: controller_supported
- `2`: controller_managed

#### `CTRL OWNERSHIP <connector_id> <module_addr>`

Updates connector ownership and local module address in RAM.

Important:

- also requires `CTRL SAVE` and reboot if you want persistence

#### `CTRL SAVE`

Persist current runtime config to NVS.

#### `CTRL STATUS`

Print full controller status and identity.

### 3.4 Serial pacing recommendation

The parser is line-oriented and consumes bytes from a single serial buffer.

Host scripts should:

- send exactly one `CTRL ...` command per newline
- avoid writing many commands back-to-back with zero spacing
- keep a small inter-command gap of about `40 ms` to avoid merged lines on the USB/UART path

## 4. End-to-End Mode 2 Flow

This is the intended controller-owned module and relay flow.

### 4.1 Bootstrap

1. Put PLC into `mode=2`
2. Confirm identity with `CTRL STATUS` or CAN identity
3. Start controller heartbeat loop
4. Begin/commit allocation for the module(s) the connector owns
5. Set auth to `pending`
6. Keep power off, feedback invalid/not-ready, relay1 open until HLC requests energy

### 4.2 CP and SLAC

1. EV reaches `B1`
2. Controller sends `SLAC arm`
3. Controller sends `SLAC start`
4. PLC progresses to SLAC match and HLC connection

### 4.3 Identity and auth

1. PLC publishes EV identity events when available:
   - EV MAC
   - EVCCID
   - EMAID
2. Controller grants auth after it is satisfied with identity/policy

### 4.4 PreCharge

1. HLC sends `PreCharge`
2. Controller commands module voltage to requested target voltage
3. Precharge current should stay limited
   - in the live test harness we cap precharge at `2 A`
4. Relay1 stays open during precharge
5. Controller reports `ready=1` only once measured voltage has converged

### 4.5 PowerDelivery / CurrentDemand

1. Once voltage is converged, controller allows relay1 to close
2. Controller continues sending power setpoints and HLC feedback
3. In the no-load simulator case:
   - current stays near `0 A`
   - voltage must still track the EV request correctly
4. The current-demand loop is considered healthy when:
   - HLC remains active
   - module/group voltage stays near target
   - relay1 remains closed
   - controller power and feedback frames stay fresh

### 4.6 Stop

1. Controller sends hard or soft stop
2. Controller denies auth
3. Controller disarms SLAC
4. Controller powers modules off
5. Controller marks HLC feedback as not-ready
6. Controller opens relay1
7. Controller aborts staged allocation / releases allowlist

The PLC considers stop complete only when all of these are true:

- relay1 open
- module output disabled
- session stopped
- HLC inactive
- allowlist empty
- local module group released

## 5. Answers to the specific questions

### Does the PLC accept only CAN messages addressed to itself?

Yes for controller traffic, but the enforcement is software-level:

- addressed PLC node must match local `can_node_id`
- controller ID must match local `controller_id`

The MCP2515 hardware filter itself is currently not narrow; it is configured accept-all and the firmware performs the real target checks.

### Does every PLC have a PLC ID that the controller can map to a connector ID?

Yes.

The mapping lives in runtime config as:

- `plc_id`
- `connector_id`

On CAN, the controller can learn that mapping from identity event kind `5`.

On serial, the controller can learn it from:

- boot `[CFG]`
- `CTRL STATUS`

### Does the PLC publish which PLC it is on UART/serial?

Yes.

`CTRL STATUS` prints:

- `plc_id`
- `connector_id`
- `controller_id`
- `module_addr`
- `local_group`
- `module_id`

That is enough for a serial controller script to know which connector it is controlling.

### Does the PLC publish which PLC it is on CAN?

Yes, through identity event kind `5`.

That event contains:

- `plc_id`
- `connector_id`
- `controller_id`
- `local_module_address`
- `local_group`
- owner ID

The firmware now republishes that local identity:

- at boot
- on first accepted controller heartbeat
- when the controller heartbeat session marker changes

