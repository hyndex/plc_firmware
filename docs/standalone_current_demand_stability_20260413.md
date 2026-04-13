# Standalone CurrentDemand Stability Report

Updated: 2026-04-13

This report captures the standalone real-vehicle charging regressions that were
observed on branch `Both-Guns-Working`, the firmware and tooling fixes applied
to resolve them, and the validation used to confirm stable charging without
removing the configured safety headroom.

Scope:

- firmware in standalone mode on `PLC1`
- real vehicle and connected gun on `/dev/ttyACM1`
- configured charging headroom preserved at `10 V / 1 A`
- focus on `CableCheck -> PreCharge -> PowerDelivery -> CurrentDemand ->
  stop/welding`

## 1. Observed Field Symptoms

The main field symptoms during live vehicle testing were:

- some sessions failed before `CurrentDemand`
- some sessions reached `CurrentDemand` but the EV backed off from `30 A` to
  `3 A` and then `0 A`
- stop-phase welding verification sometimes appeared to disappear entirely
- after some completed sessions, the charger looked frozen because no further
  useful HLC logs appeared

The safety headroom was intentionally kept enabled throughout this work because
removing it risked pushing the vehicle into an error state.

## 2. What Was Actually Wrong

### 2.1 Custom `CableCheck` handling caused early session loss

An earlier custom ISO2 `CableCheck` path had drifted away from the library
behavior. In live testing, sessions that should have progressed into
`PreCharge` sometimes terminated early after `CableCheck`.

Resolution:

- reverted `hlc_handle_cable_check()` to the library-default handling in
  `src/main.cpp`

Result:

- the connected gun reliably reached `PreCharge`, `PowerDelivery`, and
  `CurrentDemand` again

### 2.2 Configured headroom was being reported as an EVSE limit

The charger intentionally applies a lower electrical target than the EV
requests:

- requested `384.1 V / 30.0 A`
- applied `374.1 V / 29.0 A`

That is correct for the configured `10 V / 1 A` safety headroom. The bug was
that the newer standalone `CurrentDemandRes` path also advertised this as:

- `EVSECurrentLimitAchieved=1`
- `EVSEPowerLimitAchieved=1`

even when real charger capacity was still available. That is not what those
flags mean, and it is the sort of misleading CurrentDemand signaling that can
cause the EV to back off unexpectedly.

Resolution:

- changed `src/main.cpp` so configured headroom does not set current-limit or
  power-limit flags
- preserved the physical headroom itself

Result:

- active charging still uses the safety margin
- `CurrentDemandRes` no longer falsely claims saturation

### 2.3 Standalone readiness was too strict during normal ramps

The standalone path had become too eager to respond `EVSE_NotReady` while the
power path was already active and the output was still converging toward the
new target. That made the EV-visible behavior harsher than the older stable
baseline.

Resolution:

- split the current-readiness check so standalone mode can report ready based on
  safety instead of same-cycle convergence
- kept the stricter convergence rule for controller mode

Result:

- the EV no longer sees unnecessary `NotReady` responses during a normal
  current ramp in standalone mode

### 2.4 Zero-current stop tail was too strict

At the end of charging, the EV can request `0 A` while measured current is
still decaying down through a small residual range. The newer logic had become
too strict there and could hold the response in a not-ready state while current
rundown was still physically normal.

Resolution:

- relaxed the zero-request rundown readiness logic in `src/main.cpp`
- allowed stop-phase readiness through a bounded residual-current decay window

Result:

- normal EV stop ramps can proceed into `PowerDelivery(stop)` and welding
  detection more reliably

### 2.5 Welding monitor false-aborted during EV-driven stop

Part of the confusion around the missing welding logs was not firmware
freezing. The standalone welding monitor was aborting the capture when the
CurrentDemand hold ended before its own validation logic expected, which made a
normal EV/BMS stop look like a firmware hang.

Resolution:

- fixed `tools/standalone_plc_welding_monitor.py` so it continues logging
  through EV-driven stop ramps instead of aborting too early

Result:

- stop-phase captures now remain usable through `PowerDelivery(stop)` and
  welding validation

## 3. Comparison Against Older Stable Baselines

The current standalone branch was compared against:

- sibling repo `../Basic`, branch `cleanup`
- commit `e6d46f1b47a1ac80a47da12beb4a07f45ebed94a`
- commit `d040460428c4c33d64cc9158a2685661a6bcb72d`

Findings from that comparison:

- `d040460...` is PKI-focused and unrelated to the charging instability
- the older stable line did not treat configured headroom as a current/power
  limit condition
- the older stable line was less aggressive about reporting not-ready during
  active standalone charging
- the post-session CP/SLAC churn after a completed stop also exists in the
  older line, so it was not the main cause of the real-vehicle `30 A -> 3 A ->
  0 A` backoff

## 4. Standards / Guide Comparison

The behavior was also checked against local copies of:

- `design_guide_combined_charging_system_v7`
- `fhwa-2022-0008-0405_attachment_2`

The practical conclusions used here were:

- `CurrentDemand` responses must remain truthful and internally consistent
- `EVSECurrentLimitAchieved` / `EVSEPowerLimitAchieved` should indicate real
  EVSE saturation, not deliberate safety headroom
- the EVSE should tolerate rapid CurrentDemand request changes without
  oscillating readiness during normal active charging
- the end-of-charge minimum-current transition must not be handled so strictly
  that a normal ramp-down gets treated like a fault

## 5. Firmware / Tool Changes Included

This stabilization work is captured by commit:

- `43713c1` `fix: stabilize standalone current demand and monitoring`

Files included:

- `src/main.cpp`
- `tools/standalone_plc_live_monitor.py`
- `tools/standalone_plc_welding_monitor.py`
- `tools/standalone_plc_live_batch_test.py`

The key functional changes are:

- standalone fake welding now reports a one-shot residual `15 V` before `0 V`
- active standalone `CurrentDemand` uses the configured `10 V / 1 A` headroom
- active standalone `CurrentDemand` no longer reports false EVSE saturation
- standalone active charging no longer reports not-ready just because
  convergence is still catching up
- stop-phase rundown logic accepts bounded residual current during EV-requested
  `0 A`
- monitor scripts were updated so their validation matches the corrected
  firmware behavior

## 6. Validation Summary

Validation done during this work included:

- repeated live connected-gun sessions to confirm recovery through
  `CableCheck -> PreCharge -> PowerDelivery -> CurrentDemand`
- a `5/5` reset-driven live batch with the configured `10 V / 1 A` headroom
- real-vehicle retests after the CurrentDemand response fixes
- a sustained `5 minute` real-vehicle continuous-charging hold

Important confirmed behavior after the fixes:

- active current delivery remains stable with `delivery=1`
- `ready=1` remains steady through the charging plateau
- the EVSE still applies `10 V / 1 A` headroom
- `currLim=0` and `pwrLim=0` during healthy headroom-limited charging
- no mid-session collapse was seen during the `5 minute` hold

Representative stable plateau:

- request: about `384.1 V / 30.0 A`
- applied target: about `374.1 V / 29.0 A`
- present current: about `29.0 A`

## 7. Residual Non-Blocking Issue

One separate cleanup issue still exists after a completed session if the plug
remains inserted:

- the firmware can autonomously re-enter SLAC because CP returns to `B`
- if the EV does not actually begin a new HLC session, this can fall into a
  noisy `WaitForMatchingStart / SlacInitTimeout` retry path

This was observed after completed sessions, but it is not what was breaking the
active `CurrentDemand` charging plateau. Active charging stability and stop-path
behavior were the priority in this round.

## 8. Current Conclusion

The real-vehicle instability was not caused by the safety headroom itself. The
main problems were protocol-level mismatches in the newer standalone
`CurrentDemand` and stop handling:

- custom `CableCheck` behavior
- false EVSE limit signaling
- overly strict standalone readiness gating
- overly strict zero-current rundown gating
- monitor-side false failure handling

Those issues were corrected without removing the `10 V / 1 A` margin. The
result is a stable standalone real-vehicle charging session that can sustain
continuous charging and present a more consistent stop/welding path for further
testing.
