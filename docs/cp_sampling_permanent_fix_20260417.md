# CP Sampling Permanent Fix - 2026-04-17

## Problem class
The recurring field symptom was a charger stuck in `Available` after power-cycle while the gun/BMS side still indicated CP present, plus earlier transient false disconnects during live charging.

## What was missing
Production firmware trusted one CP path only: the low-level RTC ADC sampler. The ADC test firmware already had a second observation path using `analogReadMilliVolts()` burst sampling plus HLW-style hysteresis, but production did not.

That left two blind spots:
1. A production-only ADC/controller drift or stuck-high condition could pin CP at `A` with no automatic cross-check.
2. There was no automatic recovery when the ADC path timed out or disagreed with a second sampler.

## Permanent fixes
1. Active-session CP fault hold remains in place so short false `A/F/U` samples do not tear down a live session.
2. Production CP state now uses hysteretic classification instead of a raw threshold-only state jump.
3. Production firmware now runs a second analog burst verifier during suspicious idle/pre-session `A` windows and during ADC trouble.
4. If the verifier repeatedly says `connected` while the RTC path says `A`, production temporarily trusts the verifier and can reinitialize the CP ADC input path automatically.
5. `CTRL STATUS` now exposes CP diagnostics (`cp_src`, `cp_rtc_mv`, `cp_verify_mv`, `cp_verify_state`, `cp_plateau_mv`, `cp_peak_mv`, `cp_adc_reinit`, `cp_adc_timeouts`) so the next field debug does not need a custom firmware just to see what happened.
6. The ADC test environment was aligned with the production PLC estimator so diagnostics and production behavior no longer diverge.

## Interpretation
If production still reports `cp=A` while `cp_src=rtc` and `cp_verify_state=A`, the issue is not the old firmware freeze path. That points to the actual analog CP seen at the PLC input being high.

If production reports `cp_src=verify` or repeated `cp_adc_reinit`, then the firmware recovered from a sampler-path issue automatically.
