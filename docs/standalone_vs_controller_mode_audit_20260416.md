# Standalone vs Controller Mode End-to-End Audit

Updated: 2026-04-16

## Scope

This note maps the real charging path across:

- standalone PLC firmware
- controller-mode PLC firmware
- Linux `./controller` service
- the Python external-controller harness used as the minimal controller-mode reference

The goal is to identify code-path, threshold, timing, and operational differences that can upset the EV even when the configured `10 V / 1 A` safety headroom is preserved.

This audit does not change welding behavior or the configured headroom. It only documents the current contracts and the gaps between them.

## Reference Artifacts

### PLC firmware

- `plc_firmware/src/main.cpp`
- `plc_firmware/tools/external_controller_single_module_charge_test.py`
- `plc_firmware/tools/standalone_plc_live_monitor.py`

### Linux controller

- `controller/src/service.cpp`
- `controller/src/controller.cpp`
- `controller/src/plc_transport.cpp`
- `controller/docs/PLC_CONTRACT.md`
- `controller/configs/charger.json`

### Validation / field logs

- standalone stable hold:
  - `plc_firmware/logs/standalone_plc2_5m_20260415_live.log`
  - `plc_firmware/logs/standalone_plc2_5m_20260415_summary.json`
- current failing controller-mode probe:
  - `plc_firmware/logs/controller_mode_probe_20260416T005810_onestart.log`
  - `plc_firmware/logs/controller_mode_probe_20260416T005810_onestart_summary.json`
- older known-good controller-mode baseline:
  - `plc_firmware/docs/external_controller_validation_20260318.md`
  - `plc_firmware/logs/controller_single_module_1x_hold2700_cleanupfix_20260318_session01.log`
- standalone stabilization report:
  - `plc_firmware/docs/standalone_current_demand_stability_20260413.md`

## 1. Topology Split

### Standalone mode

In standalone mode the PLC owns the full charging path:

- CP read/filter/PWM output
- SLAC session lifecycle and retries
- HLC request handling
- relay control
- module targeting
- EV-facing present voltage/current
- EV-facing readiness
- EV-facing limit flags
- welding response policy

Relevant code:

- `main.cpp:10675-11308`
- `main.cpp:9289-10223`
- `main.cpp:4990-5398`

### Controller mode

In controller mode the PLC still owns CP, SLAC, and HLC, but the power path is split:

- PLC still owns CP/PWM, SLAC, HLC, EV MAC, BMS request parsing
- Linux controller or the Python harness owns auth policy, start policy, relay intent, module targets, and EV-facing feedback data
- PLC answers HLC from host-provided `CTRL FEEDBACK`

Contract:

- `controller/docs/PLC_CONTRACT.md:3-86`

Important consequence:

- controller mode is not just "standalone plus UART"
- it is a different ownership model with a latency-sensitive host feedback loop in the middle of `PreCharge`, `PowerDelivery`, and `CurrentDemand`

## 2. Stage-by-Stage Execution Map

### 2.1 QCA / runtime initialization

The same PLC binary builds both modes, but runtime behavior diverges as soon as mode checks are hit.

Common init:

- `try_init_qca_and_fsm()` creates QCA transport, resets modem, waits for sync, opens the SLAC channel, primes NMK, and starts the EVSE FSM
- code: `main.cpp:10902-10957`

Standalone-only module-manager behavior:

- module manager is configured locally with `runtime_probe_interval_ms = 80`
- ramp sequencing is disabled in standalone so the PLC can move directly to the EV-requested target after precharge and let real module limits clamp the hardware
- code: `main.cpp:6719-6747`

Controller-mode implication:

- controller mode does not have this local module-manager loop in the active charging path
- it depends on host feedback instead

### 2.2 CP sampling, filtering, fault hold, and PWM

The CP path is handled in `process_cp_and_fsm()` and `apply_cp_output()`.

Relevant code:

- `main.cpp:10675-10714`
- `main.cpp:10989-11183`

Key behavior:

- CP is read with `read_cp_mv_robust()`
- raw CP is mapped to a symbolic state with `cp_state_from_mv()`
- transient disconnect/fault samples are filtered
- once a session is active, the hold anchor becomes `max(last analog CP connected, last digital activity)`
- code: `main.cpp:11029-11066`

This is one of the recent important standalone stabilizations:

- false `F` or `A` samples during active HLC or power delivery no longer immediately kill a healthy session

PWM gating difference:

- `controller_permits_pwm` in controller mode requires `g_session_started || g_hlc_active`
- standalone additionally permits PWM through `mode_is_local_autonomous()`
- code: `main.cpp:10685-10693`

Duty behavior:

- if PWM is allowed and settled: `5%`
- otherwise: `100%`
- code: `main.cpp:10701-10713`

Session start difference:

- `start_session()` sets `g_slac_pwm_enable_at_ms = now + CP_STABLE_MS` only in controller mode
- standalone sets it to `0`
- code: `main.cpp:10716-10741`

Start condition difference:

- standalone requires `pwm_ready`
- controller mode can enter session start as soon as the controller contract allows it
- code: `main.cpp:11146-11183`

Operational risk:

- controller mode has an extra start-order dependency between CP connect, host auth/arming, PWM settle delay, and the PLC FSM
- standalone does not wait on the same host path

### 2.3 SLAC ownership and retry behavior

The runtime contract says:

- controller sends one `CTRL SLAC start` per eligible attach
- PLC owns PWM settle, SLAC retry, hold, and recovery after that

Reference:

- `controller/docs/PLC_CONTRACT.md:61-78`

Linux controller behavior:

- decides when to send `CTRL SLAC start` or `CTRL SLAC disarm`
- uses session state, CP phase, serial compatibility windows, and retry timers
- code: `service.cpp:2779-2830`

Python harness behavior:

- now mirrors the same one-start-per-attach intent
- control, feedback, and telemetry default cadence are all `100 ms`
- code: `external_controller_single_module_charge_test.py:2324-2345`

Important timing delta:

- standalone path is local and waits on module telemetry
- controller path waits on host feedback
- PLC bounded host-feedback wait is `120 ms`
- code: `main.cpp:572-579`, `main.cpp:5368-5396`
- Linux controller control, telemetry, and feedback periods are all `50 ms`
- code: `charger.json:72-81`
- Python harness defaults are all `100 ms`
- code: `external_controller_single_module_charge_test.py:2327-2330`

Operational meaning:

- controller mode is inherently more vulnerable to one-cycle stale feedback, serial delay, or control-loop skew

### 2.4 Identity and authorization

Identity comes from PLC/HLC/SLAC-side events, not from standalone module telemetry.

Linux parsing:

- `plc_transport.cpp:1138-1152`

The controller-mode contract uses:

- `AUTH pending` for autocharge identity discovery
- `AUTH grant` only after identity/external authorization or explicit grant
- `CTRL SLAC start` after the contract is ready

Reference:

- `controller/docs/PLC_CONTRACT.md:81-86`

This part is not the current main mismatch. MAC delivery and auth have already been seen working on the live bench.

### 2.5 PreCharge

#### Standalone PLC path

PreCharge request handling:

- parse EV target voltage/current
- apply local module target
- close relay if energy is allowed
- wait for post-target local telemetry
- declare ready when voltage converges to the request

Code:

- `main.cpp:9289-9482`
- `main.cpp:5308-5333`

Standalone has two separate relaxed-handoff helpers:

- `standalone_precharge_start_ready()`
- `standalone_relaxed_handoff_ready()`

Code:

- `main.cpp:3997-4053`

Relaxed-handoff rules:

- require relay closed
- require module output enabled
- require assigned modules
- if not fully converged, allow handoff when:
  - present current is visible
  - present voltage is above an accept floor
  - either multiple precharge samples have been seen or one strong current sample exists

This is a real EV-facing behavior, not just an internal optimization.

#### Controller-mode PLC path

In controller mode the PLC does not inspect local module telemetry for precharge readiness.

It does:

- read host `CTRL FEEDBACK`
- accept `fb.ready`
- or call `controller_precharge_start_ready(ctx, req_v, fb)`

Code:

- `main.cpp:9318-9402`

#### Linux controller path

The Linux controller computes the feedback that the PLC will consume:

- `precharge_feedback_ready()` returns ready if either:
  - voltage is within controller voltage tolerance
  - or a bench heuristic passes: relay closed, target above `10 V`, present voltage above `max(5 V, 5% of target)`, and present current above `max(0.5 A, 75% of requested current)`

Code:

- `service.cpp:1198-1211`
- feedback emission: `service.cpp:2491-2511`

#### Python harness path

The Python harness mirrors the standalone relaxed-handoff idea more closely than the Linux controller does.

Relevant logic:

- `external_controller_single_module_charge_test.py:1880-1917`
- supporting headroom/ready helpers: `external_controller_single_module_charge_test.py:1482-1667`

Important mismatch:

- standalone firmware has a specific multi-sample/strong-single-sample relaxed-handoff contract
- Linux controller currently uses a simpler `voltage_ready || bench_ready` heuristic
- Python harness is closer to standalone here than the Linux controller is

This is one of the strongest real contract deltas that can upset the EV during the `PreCharge -> PowerDelivery -> CurrentDemand` handoff.

### 2.6 PowerDelivery(start)

#### Standalone

Standalone `PowerDeliveryReq(start)` does all of the following locally:

- if precharge is not converged, it tries:
  - direct refresh
  - relaxed handoff
  - wait-for-convergence window
  - relaxed handoff again after wait
- if convergence succeeds, it starts delivery locally
- once the relay is closed and delivery is armed, it answers `ready=1` and lets `CurrentDemand` report the actual V/I

Code:

- `main.cpp:10067-10168`

#### Controller mode

Controller mode does not own this locally.

It does:

- check host feedback
- consider `fb.ready` or `controller_precharge_start_ready()`
- allow a relaxed start after enough precharge samples
- require `allow_energy`, `precharge_seen`, `precharge_converged`, and `relay1_closed`

Code:

- `main.cpp:10013-10066`

Operational consequence:

- standalone can self-correct and locally wait through the sensitive handoff
- controller mode depends on the host already having sent a satisfactory feedback snapshot

### 2.7 CurrentDemand

This is where the EV is most likely to react to truthfulness, latency, or readiness oscillation.

#### Standalone CurrentDemand

Relevant code:

- readiness and limit helpers: `main.cpp:3790-3987`
- fresh sample collection: `main.cpp:5284-5333`
- runtime response path: `main.cpp:9684-9950`

Standalone behavior:

- applies module targets locally
- waits for a fresh-enough post-target sample or timeout
- computes:
  - `current_converged`
  - `current_ready_safe`
  - low-request rundown state
  - `voltage_ready_safe`
  - sustained output shortfall
  - truthful limit flags
- returns EVSE ready if:
  - delivery is active
  - target is applied
  - relay is closed
  - sample is usable
  - current is safe or in allowed rundown
  - voltage is safe

Important standalone truths:

- active charging uses physical `10 V / 1 A` headroom
- configured headroom must not set EVSE current/power limit flags
- zero-current rundown is allowed through a bounded decay window

These exact concerns are documented in:

- `standalone_current_demand_stability_20260413.md:50-112`

#### Controller-mode CurrentDemand in PLC

Relevant code:

- `main.cpp:9520-9682`

Controller-mode PLC behavior:

- reads `CTRL FEEDBACK`
- does not use local module telemetry for the EV response
- computes current-safe, current-converged, rundown, voltage-safe from host values
- uses the host-reported limit flags
- intentionally no longer requires same-cycle convergence for `ready=1`

Important note:

- the old suspicion that controller mode was missing major `CurrentDemandRes` fields is not supported by the current code
- both ISO and DIN controller-mode responses fill:
  - `EVSEPresentVoltage`
  - `EVSEPresentCurrent`
  - `EVSECurrentLimitAchieved`
  - `EVSEVoltageLimitAchieved`
  - `EVSEPowerLimitAchieved`
  - `EVSEMaximumVoltageLimit`
  - `EVSEMaximumCurrentLimit`
  - `EVSEMaximumPowerLimit`

Code:

- controller mode response fill: `main.cpp:9621-9679`
- standalone response fill: `main.cpp:9896-9946`

Inference:

- the current code does not look like it is missing a core `CurrentDemandRes` max-limit field
- isolation status is handled elsewhere in the HLC flow, not as a `CurrentDemandRes` max-limit substitute

#### Linux controller CurrentDemand feedback

Relevant code:

- zero-current safe threshold: `service.cpp:147`
- current/voltage safe helpers: `service.cpp:1213-1230`
- session planning and intentional headroom: `service.cpp:2157-2342`
- delivery feedback generation: `service.cpp:2514-2559`

Linux controller behavior:

- splits module targets using controller-side headroom logic
- aggregates module feedback
- computes `ready`
- computes limit flags
- sends `CTRL FEEDBACK`

The logic is conceptually aligned with standalone, but not identical.

#### Python harness CurrentDemand feedback

Relevant code:

- `external_controller_single_module_charge_test.py:1512-1667`
- send loop / feedback cadence: `external_controller_single_module_charge_test.py:1782-1980`

This harness now mirrors the standalone truthfulness rules much more closely than before:

- preserves physical headroom
- does not claim EVSE saturation just because of configured headroom
- permits bounded low-current rundown

## 3. Exact Mismatches Found

### 3.1 Zero-request current readiness threshold mismatch

This is a direct code mismatch.

PLC firmware and Python harness:

- zero-request ready threshold is `0.75 A`
- code:
  - `main.cpp:3800-3813`
  - `external_controller_single_module_charge_test.py:1542-1548`

Linux controller:

- zero-request ready threshold is `4.0 A`
- code:
  - `service.cpp:147`
  - `service.cpp:1213-1221`

Why it matters:

- during `0 A` rundown, Linux controller can still say "ready" while measured current is much higher than the PLC standalone contract allows
- that is a real EV-visible contract difference

### 3.2 PreCharge relaxed handoff is not identical across the three paths

Standalone:

- explicit relaxed handoff function with sample-count and current-flow conditions
- code: `main.cpp:4013-4053`

Python harness:

- broadly mirrors the relaxed handoff intent

Linux controller:

- uses `voltage_ready || bench_ready`
- code: `service.cpp:1198-1211`

Why it matters:

- the EV is highly sensitive around `PreCharge -> PowerDelivery(start) -> first CurrentDemand`
- if controller mode advertises readiness under a different heuristic than standalone, the EV can see a different session shape even when the physical bus looks close

### 3.3 Controller mode is more latency-sensitive than standalone by design

Standalone:

- local runtime probe interval: `80 ms`
- fresh sample timeout for CurrentDemand: `80 ms`
- precharge post-target timeout: `600 ms`

Code:

- `main.cpp:560-579`
- `main.cpp:5284-5333`
- `main.cpp:6719-6747`

Controller mode:

- Linux controller periods: `50 ms` control, `50 ms` telemetry, `50 ms` feedback
- PLC waits up to `120 ms` for feedback newer than the request-local baseline

Code:

- `charger.json:72-81`
- `main.cpp:5368-5396`

Python harness:

- `100 ms` control, telemetry, and feedback cadence
- code: `external_controller_single_module_charge_test.py:2327-2330`

Why it matters:

- standalone’s closed loop is local and synchronous enough that the same code can wait for real local telemetry
- controller mode adds serial transport, host scheduling, CAN refresh, and feedback serialization into the middle of the HLC timing path

### 3.4 Standalone has delivery-recovery logic that controller mode does not

Standalone:

- can detect sustained output shortfall
- can open relay, drop output, and re-arm delivery recovery locally

Code:

- `main.cpp:3939-3987`
- `main.cpp:4077-4294`
- `main.cpp:9749-9787`

Controller mode:

- no equivalent PLC-owned recovery path exists for host-managed delivery
- controller mode depends on the host continuing to feed acceptable feedback or on the EV tolerating the transient

Why it matters:

- if the EV reacts badly to one bad handoff or short transient, standalone may recover locally while controller mode may not

### 3.5 Operational CP behavior differs between old good controller mode and the current failing run

Older known-good controller-mode run:

- sustained `cp=C duty=5` through active charging
- examples:
  - `controller_single_module_1x_hold2700_cleanupfix_20260318_session01.log:367`
  - `...:379`
  - `...:398`

Current failing controller-mode run:

- brief early `cp=C` before arming
- active charging path remains `B2`
- examples:
  - `controller_mode_probe_20260416T005810_onestart.log:61`
  - `...:295`
  - `...:351`
  - `...:447`
  - `...:677`

Standalone stable run:

- current demand plateau is healthy, with continuous ready/current hold
- reference: `standalone_plc2_5m_20260415_live.log:277-430`

Why it matters:

- even with HLC fields improved, the EV may still react differently if controller-mode CP behavior is not matching the older good controller-mode baseline

This is one of the strongest operational differences still visible on the bench.

### 3.6 The old "missing CurrentDemand response information" suspicion is only partly right

What was previously wrong in this repo:

- configured headroom was being reported as EVSE saturation
- documented in `standalone_current_demand_stability_20260413.md:50-78`

What is true now:

- current controller-mode and standalone code both fill the core max-limit fields
- the Python harness now also avoids false saturation from configured headroom

What is still possible:

- not a missing struct field
- but a semantic mismatch in when `ready`, `currLim`, `pwrLim`, or precharge readiness are asserted

That is consistent with the live failures seen now.

## 4. Observed Timing Differences

### Older validated controller mode

From `external_controller_validation_20260318.md:244-286`:

- average `CTRL SLAC start -> MATCHED`: `1.928 s`
- average `-> HLC active`: `5.039 s`
- average `-> PreCharge`: `6.236 s`
- average `-> PreCharge ready`: `7.378 s`
- average `-> CurrentDemand`: `8.297 s`
- average `-> CurrentDemand ready`: `8.338 s`

### Current failing controller-mode probe

From `controller_mode_probe_20260416T005810_onestart.log`:

- `CTRL SLAC start` entered before line `295`
- `MATCHED` occurs later than the older baseline
- `PowerDeliveryReq(start)` appears at line `566`
- `CurrentDemand` reaches `30 A` at lines `604` and `628`
- EV then requests `3 A` at line `647`
- EV then requests `0 A` at line `673`
- EV explicitly issues `PowerDeliveryReq(stop)` at line `684`

Operational conclusion:

- controller mode is no longer failing at SLAC or auth
- it is failing after charging has already started, during or immediately after the first sustained `CurrentDemand` plateau

## 5. What Is Already Aligned

- physical `10 V / 1 A` headroom is preserved in standalone and controller-mode helper logic
- controller-mode PLC no longer insists on same-cycle current convergence for `ready=1`
- Python harness no longer falsely claims saturation because of configured headroom
- one `CTRL SLAC start` per attach is restored
- controller-mode `CurrentDemandRes` now includes the main max-limit fields

These were necessary fixes. They were not sufficient to restore stable controller-mode charging.

## 6. Strongest Remaining EV-Upset Candidates

Ranked from most concrete to more speculative:

1. Linux controller zero-request ready mismatch (`4.0 A` vs `0.75 A`)
2. Linux controller precharge-handoff heuristic not matching standalone’s relaxed-handoff contract
3. Controller-mode feedback loop timing skew relative to the local standalone path
4. Missing controller-mode equivalent of standalone delivery recovery
5. CP behavior in the current controller-mode path not matching the older known-good `cp=C` baseline

## 7. Current Best Interpretation

The current gap is not "controller mode forgot a mandatory `CurrentDemandRes` field" in the simple structural sense.

The evidence is stronger for this interpretation:

- standalone is stable with the same physical headroom
- controller-mode PLC and Python harness now send much more truthful CurrentDemand information than before
- the remaining differences are now in:
  - readiness thresholds
  - relaxed-handoff rules
  - latency/freshness budgets
  - stop-tail handling
  - CP behavior during sustained delivery

That is exactly the class of differences that can make a real EV back off from `30 A` to `3 A` to `0 A` even though the power path is physically capable.

## 8. Recommended Next Alignment Work

1. Make Linux controller zero-request readiness match the PLC/harness `0.75 A` contract.
2. Lift standalone’s relaxed precharge-handoff contract into the Linux controller service instead of using only `voltage_ready || bench_ready`.
3. Capture a controller-mode run with full CP phase, `CTRL FEEDBACK`, and module telemetry aligned on one timeline to see whether `CP=C` is lost before or after the EV steps down.
4. If the EV still backs off after items 1 and 2, add a controller-mode recovery strategy analogous to standalone delivery recovery, but without changing the welding or headroom safety behavior.

## 9. Bottom Line

The biggest present difference between standalone and controller mode is not raw HLC message support. It is that standalone is a local closed-loop controller, while controller mode is a split-loop system with different thresholds and timing on the EV-facing readiness path.

That is the most credible place where the EV can still be getting upset.

## 10. Update 2026-04-16 04:16 IST

Direct Python controller mode on `PLC2` now passes a full `300 s` hold with the live vehicle.

Validated artifact:

- `logs/controller_mode_5m_20260416T_isofix_summary.json`
- result: `pass`
- proof peak: about `369.4 V`, `29.01 A`, `10.7 kW`

The fix that changed the live result:

- `main.cpp:3902-3924`
- external-controller mode no longer forces `EVSEIsolationStatus` to `Unavailable` for every DC response
- it now derives a controller-mode best-effort isolation assessment from:
  - active connected session
  - no emergency stop / hard stop
  - fresh valid controller feedback

Interpretation:

- standalone had already been populating a best-effort `EVSEIsolationStatus`
- controller mode was omitting that field entirely whenever power was externally driven
- the live vehicle had been reaching real `CurrentDemand` current and then backing out within a few seconds
- after restoring controller-mode isolation-status population, the same Python harness sustained charging for the full hold window

Residual note:

- the control/status view still reports the CP path as `B2` during the sustained hold on this bench
- despite that, the EV no longer performs the old `30 A -> 3 A -> 0 A -> stop` collapse
- for the direct Python controller-mode path, the missing controller-mode isolation-status population was the blocker that mattered on the live bench
