# Mode 2 Borrow/Release Hardening TODO

## Stage 1: Audit Reconciliation
- [completed] Compare the inline audit against `tools/mode2_serial_borrow_charge_test.py`
- [completed] Confirm current relay mapping versus intended lender bridge relay usage
- [completed] Confirm PLC relay GPIO mapping against `../Basic` (`Relay1=P00`, `Relay2=P01`, `Relay3=P02`)
- [completed] Confirm current stop-path behavior versus desired stop isolation behavior
- [completed] Confirm current JSON/session parsing gaps and stale-feedback hazards

## Stage 2: Controller Script Corrections
- [completed] Fix lender bridge relay command path so the bridge function drives the intended relay
- [completed] Expand relay-state parsing so borrower power relay and lender bridge relay are both tracked explicitly
- [completed] Harden session reset and CP disconnect reset behavior
- [completed] Harden stale-request power-off behavior
- [completed] Harden JSON payload extraction and CurrentDemand detection
- [completed] Remove false-ready behavior by gating feedback on real live module voltage
- [completed] Improve async allocation visibility with explicit status refresh
- [completed] Harden stop path so startup/auth/SLAC logic cannot fight shutdown
- [completed] Surface serial reader failures to the main loop
- [completed] Fix relative-path log/summary parent directory handling

## Stage 3: Borrow/Release Transition Logic
- [completed] Reconcile bridge-close, allocation, and donor readiness logic
- [completed] Ensure shared phase is accepted only when real module/bus voltage is present
- [completed] Ensure release shrinks to local-only before bridge open
- [completed] Ensure post-release current budget is restored only after local path is stable

## Stage 4: Validation
- [completed] Run `py_compile` on updated controller scripts
- [completed] Build firmware for relay-mask alignment and status visibility
- [completed] Reflash PLCs for relay-mask alignment and status visibility
- [completed] Run live relay sanity checks on current firmware
- [completed] Verify `CTRL RELAY 2 2 0` closes Relay2 and `CTRL RELAY 4 4 0` closes Relay3
- [pending] Run shortened live smoke test on the patched borrow/release flow
- [pending] Verify bridge relay action, module activation, and real voltage during borrow
- [pending] Verify release path does not drop EV voltage
- [pending] Run full-duration live borrow session

## Stage 5: Closeout
- [in_progress] Update this TODO with final status and remaining risks
- [pending] Document final evidence logs and exact command used

## Stage 6: Mode 2 Controller Ownership
- [completed] Remove mode 2 relay/power guardrails that rejected controller commands on local CP/auth/HB state
- [completed] Remove mode 2 watchdog auto-shutdown on heartbeat/auth/power freshness expiry
- [completed] Remove mode 2 local HLC teardown paths from overriding controller-owned relay/power state
- [completed] Preserve controller-managed power/feedback cache across mode 2 session start/stop transitions
- [completed] Rebuild and reflash both PLCs with the mode 2 ownership patch
- [completed] Re-verify Relay1 authority in mode 2 on an unplugged PLC (`cp=A`)

## Current Diagnosis
- [completed] Confirm the real relay-mapping bug was controller relay bit order, not GPIO pin mapping
- [completed] Confirm the old serial borrow path used historical `Relay1=0x04`, `Relay2=0x01`, `Relay3=0x02` semantics
- [completed] Confirm current firmware now matches `Basic` bit order: `Relay1=0x01`, `Relay2=0x02`, `Relay3=0x04`
- [completed] Confirm supply collapse is driven by `CtrlPowerTimeout` and `CtrlAuthExpired` opening Relay1, not by Relay2 pin mapping
- [completed] Confirm UART control cadence can starve for more than 4 s under verbose logging, so `AUTH grant 3000` is not safe on this path
- [completed] Eliminate PLC-side mode 2 controller-contract churn as a source of forced relay/power shutdown
- [pending] Re-run the full shared-borrow charging profile on the new mode 2 controller-owned firmware and validate EV-facing voltage continuity
