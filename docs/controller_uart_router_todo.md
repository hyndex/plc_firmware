# Controller UART Bridge Status

This document is now an archival status note rather than an active TODO list.

Completed outcomes now reflected in the codebase:

- controller-managed runtime is the current `mode=1` / `external_controller`
- legacy aliases still normalize into that same runtime mode
- PLC module CAN / module-manager are bypassed in controller mode
- controller-facing status is exported on the `[SERCTRL]` UART event stream
- unsupported old commands such as `CTRL ALLOC ...` and `CTRL POWER ...` were removed from the supported UART surface
- relay ownership in controller mode is controller-side

Remaining validation is hardware verification, not firmware API design:

- verify relay behavior on hardware with the external controller in the loop
- verify `CTRL FEEDBACK` covers the full PreCharge / CurrentDemand flow on hardware
- verify production controller software consumes `[SERCTRL] EVT BMS` and related events as expected
