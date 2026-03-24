# External Controller Validation Report

Updated: 2026-03-25

This report captures the current `Both-Guns-Working` controller-mode state
after the `cbmodules` host harness migration and the controller-side SLAC-start
visibility fix.

Scope:

- firmware in `mode=1 external_controller`
- current controller harness:
  - [`tools/controller_cbmodules_single_module_charge.cpp`](/home/jpi/Desktop/EVSE/plc_firmware/tools/controller_cbmodules_single_module_charge.cpp)
- `PLC1 -> module 1`
- `PLC2 -> module 2`
- no-load validation
  - the DC load was disconnected during these checks
  - `0.0 A` is therefore expected even when charging is healthy

## 1. Current Baseline

The current controller harness now uses the same `jpmodules::ModuleManager`
path as standalone instead of the older raw-CAN Python loop.

Two controller-side behaviors are important in this baseline:

- active `HB` / `AUTH` refresh is kept alive during charging
- raw PLC SLAC/FSM lines are latched immediately so the host does not resend
  `CTRL SLAC start` while the PLC is already inside
  `session start / EnterBCD / WaitForMatchingStart`

That second point is the key one-shot SLAC correction for this branch.

## 2. One-Shot SLAC Validation

Reference failure before the raw-FSM latching fix:

- [controller_cbmodules_single_module_charge_20260325_004956.log](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_cbmodules_single_module_charge_20260325_004956.log)

Observed pattern:

- `session start`
- `EnterBCD`
- `WaitForMatchingStart -> SignalError`
- `SlacInitTimeout`
- repeated multiple times before a later match

Reference clean run after the fix:

- [controller_cbmodules_single_module_charge_20260325_010503.log](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_cbmodules_single_module_charge_20260325_010503.log)
- [controller_cbmodules_single_module_charge_20260325_010503_summary.json](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_cbmodules_single_module_charge_20260325_010503_summary.json)

Confirmed from that run:

- `1` SLAC session start
- `0` `WaitForMatchingStart -> SignalError`
- `0` `SlacInitTimeout`
- `1` `Matched`
- result `pass`

## 3. Gun 1 Validation

Fresh `PLC1 -> module 1` controller session:

- [controller_cbmodules_single_module_charge_20260325_000259.log](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_cbmodules_single_module_charge_20260325_000259.log)
- [controller_cbmodules_single_module_charge_20260325_000259_summary.json](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_cbmodules_single_module_charge_20260325_000259_summary.json)

Result:

- `5 minute` hold: `pass`
- reached `Matched -> CurrentDemand`
- held stably around `499.9 V / 0.0 A`
- teardown was host-driven, not a mid-session drop

Additional longer observation:

- [controller_cbmodules_single_module_charge_20260325_012600.log](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_cbmodules_single_module_charge_20260325_012600.log)
- [controller_cbmodules_single_module_charge_20260325_012600_summary.json](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_cbmodules_single_module_charge_20260325_012600_summary.json)

Important note:

- this run held electrically for about `1194.713 s` in `CurrentDemand`
- the harness still labeled it `fail`
- the run then entered commanded stop early, so this was a harness hold-accounting
  false negative rather than a spontaneous electrical drop

## 4. Gun 2 Validation

Live `PLC2 -> module 2` controller run used for long observation:

- [controller_cbmodules_single_module_charge_20260325_015424.log](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_cbmodules_single_module_charge_20260325_015424.log)

Observed timing:

- `Matched`: `2026-03-25 01:54:30.990`
- first `CurrentDemandReq`: `2026-03-25 01:54:35.753`
- latest checked `CurrentDemandRuntime`: `2026-03-25 02:21:40.237`

Observed hold:

- `CurrentDemand` sustained for `1624.484 s`
- about `27 min 4.5 s` observed with no mid-session drop

During the observed hold:

- `relay1=1`
- `cp=C`
- `slac=1`
- `hlc=1`
- `appliedV=500.0`
- `presentV=499.9`
- `allow=1`
- `delivery=1`
- `targetApplied=1`
- `ready=1`

Again, `presentI=0.0 A` is expected here because the DC load was disconnected.

## 5. Current Conclusion

Current controller-mode status on this branch:

- both guns are charging successfully in controller mode on the present no-load
  bench setup
- the one-shot SLAC regression was not in `cbslac`
- it was in controller-side start visibility and retry behavior
- latching raw PLC SLAC/FSM lines fixed the observed duplicate-start pattern

Residual issue still open:

- the harness long-hold pass/fail accounting is not yet fully aligned with the
  real electrical hold duration, as shown by the `PLC1` `012600` run
