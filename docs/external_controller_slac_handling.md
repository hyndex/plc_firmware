# External Controller SLAC Handling Guide

Updated: 2026-03-14

This document describes the current SLAC handling model for:

- `mode=0` `standalone`
- `mode=1` `external_controller`
- `tools/mode2_uart_vehicle_charge_test.py`

The current intent is simple: in `mode=1`, SLAC failure and retry behavior should
track the standalone firmware path as closely as possible. The controller should
keep the contract alive and feed EV-facing data, but it should not fight the PLC's
own retry loop with unnecessary disarms, aborts, or resets.

## 1. Terms and Visible States

Controller-side status lines expose CP phases like this:

- `A`
  - no EV connected
- `B1`
  - EV connected, but not yet in the digital-communication window
  - typical live status: `cp=B1 duty=100`
- `B2`
  - digital communication enabled
  - typical live status: `cp=B2 duty=5`
  - this is where SLAC matching and HLC setup should progress
- `C`
  - charging path active
- `D`
  - ventilation-required state if ever used by the EV
- `E/F`
  - fault/no-PWM pulse behavior
  - the current retry path can intentionally issue an `E/F`-style pulse after repeated SLAC failures

For this bench, the important distinction is:

- `B1` is the quiet connected window
- `B2` is the active digital-communication window

## 2. Ownership Split

### `mode=0` `standalone`

The PLC owns the full charging path:

- CP / PWM
- SLAC
- HLC
- authorization
- module control
- EV-facing present voltage/current
- relay execution

End-to-end path:

- `CP -> SLAC -> HLC -> local module control -> Relay1`

### `mode=1` `external_controller`

The PLC still owns EV protocol handling, but the controller owns the power path
decisions:

- PLC owns:
  - CP / PWM
  - SLAC FSM
  - HLC SECC lifecycle
  - EV request decoding
  - relay execution
- Controller owns:
  - authorization policy
  - SLAC gating contract
  - EV-facing feedback values
  - relay decisions
  - module CAN / module power targets

End-to-end path:

- `CP -> SLAC -> HLC on PLC`
- `EV demand/status -> [SERCTRL] UART events`
- `controller feedback/relay commands -> CTRL ...`

Important rule:

- in `mode=1`, the PLC does not autonomously change relays because of HLC, CP, or SLAC transitions
- `CTRL STOP` changes PLC session-stop policy, but relay state still belongs to `CTRL RELAY`

## 3. Flashing and Making the Mode Effective

### Flash targets

Controller builds:

```bash
pio run -e controller-plc1 -t upload --upload-port /dev/ttyACM0
pio run -e controller-plc2 -t upload --upload-port /dev/ttyACM1
```

Standalone builds:

```bash
pio run -e standalone-plc1 -t upload --upload-port /dev/ttyACM0
pio run -e standalone-plc2 -t upload --upload-port /dev/ttyACM1
```

### Critical persistence rule

Flashing alone does not guarantee the running mode changed.

The firmware loads persisted runtime config from NVS on boot. That means:

- build defaults in `platformio.ini` are boot defaults only
- saved NVS config wins after reboot
- after changing mode or ownership from UART, use `CTRL SAVE` if you want the change to persist across reboot
- use `CTRL RESET` or power-cycle to start from a clean runtime state after reconfiguration

### Mode 1 bench setup used on this bench

Borrower (`plc1`):

```text
CTRL MODE mode1 1 1
CTRL OWNERSHIP 1 1
CTRL SAVE
CTRL RESET
```

Donor (`plc2`):

```text
CTRL MODE mode1 2 1
CTRL OWNERSHIP 2 2
CTRL SAVE
CTRL RESET
```

The donor ownership changed from `addr=3/group=2` to `addr=2/group=2`.

### Mode 0 bench setup

Borrower (`plc1`):

```text
CTRL MODE mode0 1 1
CTRL OWNERSHIP 1 1
CTRL SAVE
CTRL RESET
```

Donor (`plc2`):

```text
CTRL MODE mode0 2 1
CTRL OWNERSHIP 2 2
CTRL SAVE
CTRL RESET
```

### Confirm status after reboot

Use:

```text
CTRL STATUS
```

Mode 1 should look similar to:

```text
[SERCTRL] STATUS mode=1(external_controller) plc_id=1 connector_id=1 controller_id=1 module_addr=0x01 local_group=1 cp=B1 duty=100 ...
[SERCTRL] STATUS mode=1(external_controller) plc_id=2 connector_id=2 controller_id=1 module_addr=0x02 local_group=2 cp=A duty=100 ...
```

## 4. Standalone SLAC Failure Handling

Standalone is the reference behavior.

When CP is stable and connected, standalone:

- waits for a stable connected CP window
- starts SLAC on its own
- drives PWM to the digital-communication window
- starts HLC after SLAC match

On failure, standalone does not immediately hard-reset the session from outside.
Instead it uses an internal retry policy:

- SLAC progress timeout: `12 s`
- hold after failure: `3 s`
- E/F pulse after repeated failures in the same CP attach: after `2` failures
- E/F pulse duration: `4 s`

In effect:

- first failed match: hold and retry
- repeated failed matches: hold, then E/F pulse, then retry

## 5. Mode 1 Golden Path

Mode 1 should now follow the same retry shape as standalone, but with a controller
contract layered on top.

### Step 1: establish controller liveness

Keep the heartbeat alive:

```text
CTRL HB 3000
```

The harness currently drives heartbeat about every `800 ms`.

### Step 2: establish authorization policy

While waiting for the session to identify itself, keep auth alive:

```text
CTRL AUTH pending 6000
```

Promote to grant when charging is allowed:

```text
CTRL AUTH grant 6000
```

### Step 3: wait for the EV to land in `B1`

Do not force digital communication while the cable is not yet stable. The clean
entry window is:

- EV connected
- `cp=B1`
- `duty=100`

### Step 4: start SLAC with `start`, not just `arm`

Use:

```text
CTRL SLAC start 45000
```

This does two things:

- arms the SLAC contract
- latches immediate start permission

When the contract is valid, the PLC can move from `B1` to `B2` and begin SLAC.

### Step 5: while waiting for a match, keep the contract alive

While the EV is still trying to match:

- keep sending `CTRL HB`
- keep auth alive
- refresh the SLAC contract with `CTRL SLAC start`, not `CTRL SLAC arm`

Current harness behavior is to refresh every roughly `8-12 s`.

This is the key rule:

- use `CTRL SLAC start` for keepalive
- do not refresh a waiting session with `CTRL SLAC arm`

`arm` clears the `start` latch. If you replace `start` with `arm` while the EV is
still trying, you can accidentally remove the permission the PLC needs to restart
cleanly after its internal hold window.

### Step 6: let the PLC retry naturally

If the EV sends multiple SLAC requests, do not force an immediate external reset
after the first failure.

The intended behavior is:

- EV retries normally
- PLC enters hold on failed/expired matching
- PLC optionally applies E/F pulse after repeated failures
- PLC restarts naturally when:
  - CP is still connected
  - the controller contract is still valid
  - the hold window expired

This avoids fighting the EV's own retry burst.

### Step 7: once matched, feed HLC properly

After match and HLC startup:

- watch `[SERCTRL] EVT BMS`
- watch `[SERCTRL] EVT HLC`
- send controller-fed values with `CTRL FEEDBACK`
- drive the power path with explicit `CTRL RELAY`

Example:

```text
CTRL FEEDBACK 1 1 500 10 0 0 0 0
CTRL RELAY 1 1 5000
```

### Step 8: stop cleanly

Preferred stop sequence:

```text
CTRL STOP soft 3000
CTRL RELAY 1 0 0
CTRL AUTH deny 6000
CTRL SLAC disarm 3000
```

Important:

- `CTRL STOP` alone does not open relays
- relay changes still require `CTRL RELAY`

## 6. Current Mode 1 Failure Handling

After the current firmware alignment, `mode=1` no longer uses a separate
controller-only in-place SLAC restart path. Instead it follows the standalone
shape:

- wait for stable CP
- start session if controller contract allows it
- if matching stalls for `12 s`, enter hold
- if the FSM reaches `MatchingFailed` or `NoSlacPerformed`, enter hold
- after repeated failures in the same CP attach, issue the `4 s` E/F pulse
- if the controller contract is still valid after hold, restart naturally

This means the correct controller policy is:

- keep the contract alive
- do not churn `disarm`, `abort`, or `reset`
- only escalate after the EV retry burst has gone quiet or the session is clearly stuck beyond the normal hold/retry path

## 7. `CTRL SLAC` Command Semantics

Use the commands like this:

- `CTRL SLAC arm <ms>`
  - arms the lease
  - clears any previous start latch
  - use only if you want permission without an immediate start latch
- `CTRL SLAC start <ms>`
  - arms the lease
  - sets the start latch
  - use this for the real `B1 -> B2` trigger
  - use this again for waiting-session keepalive
- `CTRL SLAC disarm <ms>`
  - clears the contract
  - safe-stops an active session when needed
- `CTRL SLAC abort <ms>`
  - explicit abort path
  - also clears the contract and stops the active session

Operational guidance:

- `start` is the normal command for the borrower
- donor should normally stay `AUTH deny` and `SLAC disarm`
- do not alternate `start -> arm -> start` during one attach unless you are intentionally removing and re-granting permission

## 8. Reading the Event Stream

The controller should primarily watch:

- `[SERCTRL] EVT CP ...`
  - CP state, PWM duty, and controller gating flags
- `[SERCTRL] EVT SLAC ...`
  - SLAC FSM state, failure count, and progress
- `[SERCTRL] EVT HLC ...`
  - HLC readiness and feedback visibility
- `[SERCTRL] EVT BMS ...`
  - EV-requested stage, target voltage, and target current
- `[SERCTRL] EVT SESSION ...`
  - relays, stop state, meter Wh, SOC
- `[SERCTRL] ACK ...`
  - command acceptance/rejection

Useful interpretations:

- `cp=B1 duty=100`
  - connected, quiet start window
- `cp=B2 duty=5`
  - digital communication enabled
- repeated `waiting SLAC start auth`
  - controller contract is missing or expired
- `[SLAC] enter hold ...`
  - expected recovery behavior after match failure or timeout
- `[CP] E/F pulse ...`
  - expected stronger recovery after repeated failures

## 9. Python Harness Usage

The current live harness is:

- `tools/mode2_uart_vehicle_charge_test.py`

Current default bench identities are:

- borrower home module: `addr=1/group=1`
- donor module: `addr=2/group=2`

### Single session

```bash
python3 tools/mode2_uart_vehicle_charge_test.py
```

### Three consecutive 3-minute sessions with 1-minute gaps

```bash
python3 tools/mode2_uart_vehicle_charge_test.py --session-profile align3x3m
```

This profile currently means:

- `home_duration_s=60`
- `shared_duration_s=60`
- `post_release_duration_s=60`
- `session_count=3`
- `session_gap_s=60`
- `session_start_timeout_s>=240`

### Five consecutive 5-minute sessions with 1-minute gaps

```bash
python3 tools/mode2_uart_vehicle_charge_test.py --session-profile align5x5m
```

This profile currently means:

- `home_duration_s=100`
- `shared_duration_s=100`
- `post_release_duration_s=100`
- `session_count=5`
- `session_gap_s=60`
- `session_start_timeout_s>=360`

### What the harness now does

The harness now intentionally avoids fighting the PLC retry path:

- keeps borrower heartbeat alive
- keeps borrower auth alive
- refreshes borrower SLAC with `CTRL SLAC start`
- does not immediately disarm after one failed match
- no longer uses the old Python-side `B1 too long -> hard recovery` loop as the default retry mechanism

Do not wrap this script in another shell loop that resets both ESPs between
sessions unless you are debugging a boot-time issue.

## 10. Warnings and Cautions

- Flashing a controller or standalone build does not guarantee the runtime mode changed. Saved NVS config still wins on boot until you update it with `CTRL SAVE` and restart cleanly.
- In `mode=1`, relays are controller-owned. `CTRL STOP` does not by itself open the power path.
- The correct borrower keepalive is `CTRL SLAC start`, not repeated `CTRL SLAC arm`.
- Do not `CTRL SLAC disarm` or `CTRL RESET` after the first failed match attempt. Let the EV retry and let the PLC hold/E/F recovery path run first.
- A `B2 -> B1 -> B2` cycle can be part of normal recovery. It is not automatically a reason to hard reset.
- Donor should stay disarmed in this test flow. Only the borrower should be trying to match.
- If heartbeat is lost long enough, the PLC clears the stale SLAC contract on its own.

## 11. Current Bench Limitations

As of 2026-03-14, SLAC start and retry behavior in `mode=1` is materially better,
but two bench-level cautions remain:

- protocol stop still often falls back to forced stop
- the `align3x3m` validation showed donor-side release/status recovery issues after session 2, which is downstream of SLAC matching itself

So the current state is:

- Mode 1 SLAC handling is aligned with standalone retry behavior
- end-to-end charging can pass
- remaining instability is now more visible in donor release and stop handling than in initial SLAC start handling
