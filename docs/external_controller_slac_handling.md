# External Controller SLAC Handling Guide

Updated: 2026-03-18

This document is the current reference for:

- `mode=0` `standalone`
- `mode=1` `external_controller`
- [`tools/external_controller_single_module_charge_test.py`](/home/jpi/Desktop/EVSE/plc_firmware/tools/external_controller_single_module_charge_test.py)

The current design goal is:

- keep standalone as the reference SLAC behavior
- in `mode=1`, let the PLC own SLAC retry and recovery after one controller `start`
- keep the controller responsible for heartbeat, auth, relay commands, module CAN control, and EV-facing feedback
- avoid controller-side reset churn while the PLC and EV are still in their normal retry loop

## 1. Runtime Modes and Bench Tools

The firmware now has 2 effective runtime behaviors:

- `mode=0` `standalone`
- `mode=1` `external_controller`

Legacy aliases such as `mode=2`, `mode=3`, `uart_router`, and older
controller-managed names normalize into the same `mode=1 external_controller`
runtime path.

Current bench scripts:

- [`tools/external_controller_single_module_charge_test.py`](/home/jpi/Desktop/EVSE/plc_firmware/tools/external_controller_single_module_charge_test.py)
  - recommended for pure controller-mode SLAC / HLC / charging validation
  - single PLC, single module, single relay, no donor/share path

## 2. Terms and Visible States

The controller-side event stream exposes CP phases like this:

- `A`
  - unplugged
- `B1`
  - EV connected, but digital communication not yet enabled
  - typical line: `cp=B1 duty=100`
- `B2`
  - digital communication window
  - typical line: `cp=B2 duty=5`
  - SLAC and HLC startup should happen here
- `C`
  - charging path active
- `D`
  - ventilation-required state if the EV ever uses it
- `E/F`
  - fault / no-PWM pulse state
  - the PLC may intentionally pulse into this area during stronger SLAC recovery

For this bench:

- `B1` is the quiet attach window
- `B2` is the digital-communication window
- `C` means the EV has accepted charging

## 3. Ownership Split

### `mode=0` `standalone`

The PLC owns the full charging path:

- CP / PWM
- SLAC
- HLC
- authorization
- relay execution
- module manager / local module control
- EV-facing present voltage / current

End-to-end path:

- `CP -> SLAC -> HLC -> local module control -> Relay1`

### `mode=1` `external_controller`

The PLC still owns EV protocol handling, but the controller owns the power path
decisions.

PLC owns:

- CP / PWM
- SLAC FSM
- HLC SECC lifecycle
- EV request decoding
- UART event stream / ACK surface

Controller owns:

- heartbeat / liveness contract
- authorization policy
- initial "start digital communication" permission
- relay decisions
- EV-facing feedback values
- module CAN control and power targets

End-to-end path:

- `CP -> SLAC -> HLC on PLC`
- `EV demand/status -> [SERCTRL] UART events`
- `controller feedback + relay commands -> CTRL ...`
- `controller -> modules over external CAN`

Important rule in `mode=1`:

- the PLC does not autonomously drive the module-manager path
- `CTRL STOP` affects PLC-side stop policy, but relay state still belongs to `CTRL RELAY`

## 4. Flashing and Making the Mode Effective

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

### Persistence rule

Flashing alone does not guarantee the runtime mode changed.

The firmware loads runtime config from NVS on boot. That means:

- `platformio.ini` defaults are boot defaults only
- saved NVS config wins after reboot
- after changing mode or ownership through UART, use `CTRL SAVE` if the change should persist
- use `CTRL RESET` or power-cycle after reconfiguration if you want a clean runtime restart

### Mode 1 bench setup used here

Borrower / PLC1:

```text
CTRL MODE 1 1 1
CTRL OWNERSHIP 1 1
CTRL SAVE
CTRL RESET
```

Donor / PLC2:

```text
CTRL MODE 1 2 1
CTRL OWNERSHIP 2 2
CTRL SAVE
CTRL RESET
```

### Mode 0 bench setup

Borrower / PLC1:

```text
CTRL MODE 0 1 1
CTRL OWNERSHIP 1 1
CTRL SAVE
CTRL RESET
```

Donor / PLC2:

```text
CTRL MODE 0 2 1
CTRL OWNERSHIP 2 2
CTRL SAVE
CTRL RESET
```

### Confirm status

Use:

```text
CTRL STATUS
```

Mode 1 should report something similar to:

```text
[SERCTRL] STATUS mode=1(external_controller) plc_id=1 connector_id=1 controller_id=1 module_addr=0x01 local_group=1 cp=B1 duty=100 ...
```

## 5. Standalone Reference Behavior

Standalone is still the reference path.

When CP is stable and connected, standalone:

- waits for a stable connected CP window
- enables PWM
- waits for a stable `B2` digital-communication window
- starts SLAC on its own
- starts HLC after SLAC match

On failure, standalone does not rely on an outside controller reset loop.
It uses internal retry behavior:

- SLAC progress timeout: `45 s`
- hold after failure: `10 s`
- wrapper-only soft retry on very fast early `MatchingFailed`: `2` retries with `1.2 s` hold
- E/F pulse after repeated failures in the same CP attach: after `3` failures
- E/F pulse duration: `180 ms`

In effect:

- first failed match: hold, then retry
- repeated failed matches: hold, then E/F pulse, then retry

## 6. Current Mode 1 SLAC Lifecycle

The important firmware change on 2026-03-15 is that `mode=1` now uses the same
practical SLAC retry shape as standalone after one controller `start`.

### 6.1 What changed in firmware

The firmware now keeps a PLC-owned SLAC contract internally:

- `CTRL SLAC start` latches `slac_plc_owned=1`
- that PLC-owned contract survives the PLC's own retry pulses and hold windows
- the contract is no longer expected to be refreshed periodically by the controller
- SLAC start in controller mode is gated by stable B2/PWM timing, matching standalone more closely

The controller-owned / PLC-owned split is now:

- controller says: "digital communication is allowed, start now"
- PLC says: "I will keep retrying SLAC with the standalone recovery path until I match or the contract is explicitly cleared"

### 6.2 What the controller does now

For one EV attach:

1. keep heartbeat alive
2. keep auth alive
3. wait for a clean attach window around `B1` / `B2`
4. send one `CTRL SLAC start 45000`
5. do not keep re-sending `CTRL SLAC start`
6. let the PLC retry naturally

### 6.3 What the PLC does now

Once the controller issued one valid `CTRL SLAC start`, the PLC:

- enables PWM / digital communication when the stable CP window is ready
- starts SLAC
- if matching stalls for `45 s`, enters hold
- if the SLAC FSM hits a very fast early `MatchingFailed`, retries locally with a short `1.2 s` hold before escalating
- if the SLAC FSM reaches `MatchingFailed` or `NoSlacPerformed` outside that soft-retry window, enters hold
- after repeated failures in the same attach, issues the `180 ms` E/F pulse
- restarts naturally if CP is still connected and the PLC-owned contract still exists

This is the same recovery shape used by standalone:

- progress timeout -> hold
- repeated failures -> hold + E/F pulse
- retry without controller reset churn

### 6.4 When the PLC-owned contract clears

The latched controller contract is cleared when:

- the EV really unplugs back to `A`
- the controller explicitly sends `CTRL SLAC disarm`
- the controller explicitly sends `CTRL SLAC abort`
- stop / safe-stop handling clears the session contract
- heartbeat-loss cleanup decides the waiting contract is stale

Important nuance:

- PLC retry pulses and temporary `B2 -> B1 -> F -> B2` recovery transitions do not by themselves require a new controller `start`
- a real unplug to `A` does require a new `CTRL SLAC start` on the next attach

## 7. `CTRL SLAC` Command Semantics

Use the commands like this:

- `CTRL SLAC arm <ms>`
  - lease only
  - clears the `start` latch
  - leaves `slac_plc_owned=0`
  - use only when you want permission without immediate start
- `CTRL SLAC start <ms>`
  - arms the contract
  - latches immediate start permission
  - sets the PLC-owned retry path
  - this is the normal borrower command
  - one `start` per EV attach is the intended policy
- `CTRL SLAC disarm <ms>`
  - clears the contract
  - stops waiting sessions cleanly
- `CTRL SLAC abort <ms>`
  - explicit abort path
  - also clears the active contract

Operational guidance:

- use `start`, not `arm`, for the borrower
- do not alternate `start -> arm -> start` during a normal attach
- donor should normally remain `AUTH deny` and `SLAC disarm` in controller-mode charge tests

## 8. Controller Golden Path

Recommended controller sequence:

### Step 1: establish liveness

```text
CTRL HB 3000
```

The current Python harness drives heartbeat about every `800 ms`.

### Step 2: establish auth

While waiting:

```text
CTRL AUTH grant 6000
```

### Step 3: wait for attach

Clean start window:

- EV connected
- `cp=B1` or early `B2`
- no controller-side reset churn

### Step 4: start digital communication once

```text
CTRL SLAC start 45000
```

### Step 5: let the PLC own retry

Do:

- keep sending `CTRL HB`
- keep sending `CTRL AUTH grant`
- wait for `MATCHED`, `PreCharge`, and `CurrentDemand`

Do not:

- periodically refresh `CTRL SLAC start`
- replace `start` with `arm` while the EV is still retrying
- immediately `disarm`, `abort`, or `reset` after the first failed match

### Step 6: feed HLC correctly

After `MATCHED` / HLC startup:

- watch `[SERCTRL] EVT BMS`
- send `CTRL FEEDBACK`
- drive relay state with `CTRL RELAY`

Example:

```text
CTRL FEEDBACK 1 1 500 0 0 0 0
CTRL RELAY 1 1 5000
```

### Step 7: stop cleanly

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

## 9. Reading the Event Stream

The controller should primarily watch:

- `[SERCTRL] EVT CP ...`
- `[SERCTRL] EVT SLAC ...`
- `[SERCTRL] EVT HLC ...`
- `[SERCTRL] EVT BMS ...`
- `[SERCTRL] EVT SESSION ...`
- `[SERCTRL] ACK ...`

Useful interpretations:

- `cp=B1 duty=100`
  - quiet attach window
- `cp=B2 duty=5`
  - digital communication enabled
- repeated `waiting SLAC start auth`
  - contract is missing or expired
- `[SLAC] enter hold ...`
  - expected retry behavior
- `[CP] E/F pulse ...`
  - expected stronger recovery after repeated failures
- `plc_owned=1`
  - controller already handed SLAC retry ownership to the PLC

## 10. Recommended Python Harness Usage

### 10.1 Preferred controller-mode bring-up / SLAC validation

Use the simple single-module harness:

```bash
python3 tools/external_controller_single_module_charge_test.py
```

What it does:

- PLC1 only
- connector 1 only
- module `addr=1/group=1` only
- `Relay1` only
- no donor/share orchestration
- follows the same practical shape as standalone:
  - `SLAC -> HLC -> PreCharge -> PowerDelivery -> CurrentDemand`

For repeated or batch runs, pin the serial port explicitly and run one instance at
a time:

```bash
python3 tools/external_controller_single_module_charge_test.py \
  --plc-port /dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_B8:F8:62:4B:C5:20-if00 \
  --plc-id 1 \
  --controller-id 1
```

Important batch rule:

- do not run multiple copies concurrently
- the harness opens the PLC port exclusively
- concurrent copies or stale background processes will corrupt the dataset

### 10.2 Long-hold usage

For long holds, the harness timeout must cover:

- attach and SLAC startup time
- the requested `CurrentDemand` hold
- the stop / cleanup window

Do not leave the default short timeout in place for a `45 minute` run.

Validated long-hold example:

```bash
python3 tools/external_controller_single_module_charge_test.py \
  --plc-port /dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_B8:F8:62:4B:C5:20-if00 \
  --plc-id 1 \
  --controller-id 1 \
  --current-demand-hold-s 2700 \
  --session-start-timeout-s 3600 \
  --stop-timeout-s 20
```

Teardown interpretation rule:

- if `modem reprime`, `HLC ready=0 active=0`, or `client session done` happen
  only after the host `CTRL STOP`, they are expected cleanup markers
- if they happen before the host stop, treat them as dropout evidence

## 11. Bench Validation Snapshot

### 11.1 Standalone confirmation

Standalone was revalidated after the mode-1 changes and still reaches charging:

- `CurrentDemand ready=1`
- about `500 V`
- `0.00 A` in the no-load simulator case

Reference artifact:

- [`logs/standalone_plc1_confirm2_20260315_080509_summary.json`](/home/jpi/Desktop/EVSE/plc_firmware/logs/standalone_plc1_confirm2_20260315_080509_summary.json)

### 11.2 Single-run mode-1 confirmation

Mode 1 was then validated with the simple harness:

- one controller `CTRL SLAC start`
- PLC-owned retry behavior
- successful `MATCHED -> PreCharge -> CurrentDemand`

Representative clean run:

- [`logs/external_controller_single_module_charge_20260315_081616_summary.json`](/home/jpi/Desktop/EVSE/plc_firmware/logs/external_controller_single_module_charge_20260315_081616_summary.json)

### 11.3 10-session serial characterization

Latest clean serial batch:

- artifact JSON:
  - [`logs/controller_single_module_10x_hold120_gap60_serial_20260315_083729_aggregate.json`](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap60_serial_20260315_083729_aggregate.json)
- artifact CSV:
  - [`logs/controller_single_module_10x_hold120_gap60_serial_20260315_083729_aggregate.csv`](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap60_serial_20260315_083729_aggregate.csv)

Batch conditions:

- `10` sessions
- `120 s` charge hold per session
- `60 s` gap between sessions
- sessions run serially, one after another

Results:

- `8/10` full pass
- `10/10` reached `MATCHED`
- `10/10` reached `PreCharge`
- `9/10` reached `CurrentDemand ready`
- controller `CTRL SLAC start` count was `1` in every session
- PLC internal SLAC session restarts ranged from `1` to `5` per session

SLAC match time from controller `CTRL SLAC start` to `MATCHED`:

- min: `1.991 s`
- max: `47.781 s`
- average: `22.118 s`

`CurrentDemand ready` time:

- pass-session min: `8.215 s`
- pass-session max: `53.671 s`
- pass-session average: `28.411 s`

Observed retry totals across the 10 sessions:

- SLAC progress timeouts: `9`
- E/F pulses: `10`
- terminal SLAC failures before recovery: `11`

Interpretation:

- PLC-owned SLAC retry is working
- the controller no longer needs to babysit SLAC restarts
- most sessions still need some PLC-side retry activity
- only one session in the batch completed with no retry at all

### 11.4 Failure classification from the 10-session batch

Session 4:

- had a real SLAC timeout and `CM_ATTEN_CHAR` failure
- recovered to `MATCHED` and `PreCharge`
- final runner failure was host-side heartbeat ACK timeout `0x10`

References:

- [`session04.log`](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap60_serial_20260315_083729_session04.log#L496)
- [`session04.log`](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap60_serial_20260315_083729_session04.log#L669)
- [`session04.log`](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap60_serial_20260315_083729_session04.log#L909)
- [`session04.log`](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap60_serial_20260315_083729_session04.log#L1027)
- [`session04_runner.out`](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap60_serial_20260315_083729_session04_runner.out#L16)

Session 5:

- reached `MATCHED`
- reached `PreCharge`
- reached stable `CurrentDemand` around `499.9 V / 0.0 A`
- later lost PLC status / feedback visibility and the runner failed on `timeout waiting for PLC status`

References:

- [`session05.log`](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap60_serial_20260315_083729_session05.log#L740)
- [`session05.log`](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap60_serial_20260315_083729_session05.log#L894)
- [`session05.log`](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap60_serial_20260315_083729_session05.log#L1039)
- [`session05.log`](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap60_serial_20260315_083729_session05.log#L3451)
- [`session05_runner.out`](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap60_serial_20260315_083729_session05_runner.out#L28)

### 11.5 Post-cleanup short-session validation on 2026-03-17 and 2026-03-18

After the transport and harness cleanup fixes, the simple controller-mode path
was rerun in a new serial batch:

- `9` completed consecutive `120 s` sessions passed
- average `CTRL SLAC start -> MATCHED`: `1.925 s`
- average `CTRL SLAC start -> PreCharge`: `6.226 s`
- average `CTRL SLAC start -> CurrentDemand`: `8.239 s`
- average `CTRL SLAC start -> CurrentDemand ready`: `8.337 s`
- average `host stop -> relay open`: `0.123 s`
- average `host stop -> stop_done=1`: `20.342 s`

Important nuance:

- the intended batch was `10` sessions
- session `10` was interrupted externally before its summary and aggregate
  files were written
- the validated dataset therefore consists of the `9` completed sessions only

Full per-session table and artifact links:

- [`docs/external_controller_validation_20260318.md`](/home/jpi/Desktop/EVSE/plc_firmware/docs/external_controller_validation_20260318.md)

### 11.6 Long-hold validation on 2026-03-18

One `45 minute` controller-mode hold was then run with:

- `CurrentDemand` target hold `2700 s`
- `session_start_timeout_s=3600`
- `stop_timeout_s=20`

Result:

- `pass`
- `CTRL SLAC start -> MATCHED`: `1.920 s`
- `CTRL SLAC start -> CurrentDemand`: `8.323 s`
- `CTRL SLAC start -> CurrentDemand ready`: `8.654 s`
- sustained `CurrentDemand` before host stop: `2700.010 s`

Observed teardown order:

1. host `CTRL STOP soft 20000`
2. relay open `0.132 s` later
3. `stop_done=1` at `20.087 s`
4. `SLAC modem reprime (session stop)` and `HLC client session done rc=-9`
   only after the stop path completed

No premature teardown markers appeared before the commanded stop:

- no `PowerDeliveryRuntime stop=1`
- no `WeldingDetectionRuntime`
- no `SessionStopRuntime`
- no `modem reprime` before the host stop

Full timing table and artifact links:

- [`docs/external_controller_validation_20260318.md`](/home/jpi/Desktop/EVSE/plc_firmware/docs/external_controller_validation_20260318.md)

## 12. Current Conclusions and Cautions

Current state after the latest firmware and harness updates:

- controller mode now follows the standalone retry model much more closely
- one controller `CTRL SLAC start` per EV attach is enough
- the PLC owns SLAC retry / hold / E/F recovery after that
- standalone still works
- mode 1 now repeatedly reaches `CurrentDemand` in the simple single-module path
- the host-driven cleanup path is stable in the simple harness
- a `45 minute` hold has completed cleanly in the simple single-module path

Remaining instability is no longer best described as "controller mode cannot do
SLAC". The remaining issues are:

- long but recoverable SLAC retries on some attaches
- the latest interrupted short-session batch still lacks a closed `10/10`
  aggregate because session `10` was stopped externally
- SLAC `CM_SET_KEY` strictness hardening is still deferred
- more complex donor/share behavior in the larger borrower/donor harness
