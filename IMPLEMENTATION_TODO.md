# PLC Firmware Rework TODO (Standalone + Controller-Orchestrated)

Updated: 2026-03-07  
Owner: Codex  
Scope: Rework firmware architecture so `StandaloneMode=disabled` is controller-authoritative for authorization and SLAC start, while preserving a complete standalone path.

## Confirmed Requirements (Authoritative)
- `StandaloneMode=enabled`:
  - PLC is fully independent (SLAC/HLC/modules/Relay1 local).
- `StandaloneMode=disabled`:
  - PLC depends on external controller for authorization decision.
  - PLC must publish status continuously to controller (CP/SLAC/HLC/session/module/safety).
  - PLC must publish identity/events (RFID, EV MAC, EVCCID, EMAID where available).
  - Controller explicitly decides when SLAC is allowed to start after gun connect.
  - Relay1 remains PLC-owned; Relay2/Relay3 can be externally commanded.
- Optional SW4 boot WiFi manager:
  - Start portal only when SW4 held at boot.
  - No portal tasks/allocations in normal boot.
  - Save -> reboot.

## Safety Invariants (Must Never Be Broken)
- [ ] No energy path without valid authorization in controller mode.
- [ ] No autonomous SLAC start in controller mode unless controller armed/started it.
- [ ] Relay1 open on CP drop/session end/fault/timeouts.
- [ ] Module output OFF on session end/deallocation/watchdog timeout.
- [ ] Controller command frames are sequence + CRC checked; stale commands rejected.
- [ ] In controller mode, command loss fails safe (hold/no new session and safe stop).
- [x] PLC accepts only targeted controller frames and only its own local module traffic at the transport boundary.

## External Controller Contract (New V1)
### PLC -> Controller (TX)
- [ ] `PLC_HEARTBEAT`: uptime, fw version, mode, fault summary, watchdog health.
- [ ] `PLC_CP_STATUS`: CP state, duty, robust mV, connect/disconnect timestamps.
- [ ] `PLC_SLAC_STATUS`: idle/armed/running/matched/fail-hold + reason counters.
- [ ] `PLC_HLC_STATUS`: protocol, stage, auth_pending/granted, charging flags.
- [ ] `PLC_POWER_STATUS`: module aggregate present V/I/P, active modules, freshness.
- [ ] `PLC_SESSION_STATUS`: session epoch, contactor state, energy counters.
- [ ] `PLC_IDENTITY_EVT`: EV MAC, EVCCID, EMAID segments, RFID UID events.

### Controller -> PLC (RX)
- [ ] `CTRL_HEARTBEAT`: controller alive marker (required in controller mode).
- [ ] `CTRL_AUTH_STATE`: grant/deny/pending with ttl and reason code.
- [ ] `CTRL_SLAC_CONTROL`: `DISARM`, `ARM`, `START_NOW`, `ABORT`.
- [ ] `CTRL_ALLOC_TXN_*`: atomic module assignment transaction (begin/data/commit/abort).
- [ ] `CTRL_AUX_RELAY`: Relay2/Relay3 command with safety policy bits.
- [ ] `CTRL_LIMITS_POLICY`: optional caps/derates/no-energy mode.

## Mode-Specific Session Flow
### Standalone Enabled
- [ ] CP stable -> local SLAC trigger -> HLC -> local auth policy -> module + Relay1 control.

### Standalone Disabled (Controller Authority)
- [ ] CP stable only raises `gun_connected` event.
- [ ] PLC waits for `CTRL_SLAC_CONTROL=ARM/START_NOW`.
- [ ] SLAC runs only while controller authorization window is valid.
- [ ] HLC AuthorizationReq response derived from `CTRL_AUTH_STATE` only:
  - pending -> Ongoing
  - granted -> Finished
  - denied/expired -> FAIL path
- [ ] If controller heartbeat/auth ttl expires during session -> safe stop path:
  - stop energy request
  - open Relay1
  - module OFF + release

## Multi-Stage Implementation Plan
## Stage A: Configuration and Boot Path
- Status: In Progress
- Dependencies: None
- Tasks:
  - [x] Add persistent config: `standalone_mode`, controller timeouts, SLAC policy gates, relay2/3 policy.
  - [x] Add persistent identity config: `plc_id`, `connector_id`, `controller_id`, `local_module_address`.
  - [x] Implement SW4 boot sample window and conditional WiFi manager startup.
  - [x] Ensure non-WiFi boot path has zero portal/task allocation.

## Stage B: Control Plane Protocol Layer
- Status: In Progress
- Dependencies: Stage A
- Tasks:
  - [x] Define CAN IDs, payload schema, seq, CRC, and ack/nack behavior.
  - [x] Implement parser/validator with duplicate and stale frame suppression.
  - [x] Consume controller CAN families before `cbmodules` so controller traffic never leaks into the module stack.
  - [x] Mask module CAN ingress to the PLC's own local module address and matching ownership-claim traffic only.
  - [x] Implement deterministic command application order per tick.

## Stage C: Controller Liveness and Authorization Gate
- Status: In Progress
- Dependencies: Stage B
- Tasks:
  - [x] Add controller heartbeat watchdog with grace window.
  - [x] Add auth TTL watchdog and explicit auth state machine.
  - [x] Bind HLC authorization responses to external auth state in controller mode.

## Stage D: SLAC Arm/Start Gating
- Status: In Progress
- Dependencies: Stage B, Stage C
- Tasks:
  - [x] Split `gun_connected` from `slac_start`.
  - [x] Add SLAC arm/start state machine driven by `CTRL_SLAC_CONTROL`.
  - [x] Handle abort/retry/fail-hold transitions and report reasons upstream.

## Stage E: Identity and Event Uplink
- Status: In Progress
- Dependencies: Stage B
- Tasks:
  - [x] Publish PLC identity tuple (`plc_id`, `connector_id`, `controller_id`, `local_module_address`, logical group, owner_id`) on boot.
  - [x] Publish EVCCID + local MAC as segmented events with session epoch sequencing.
  - [ ] Publish EV MAC/EMAID when available from runtime path (pending explicit extraction points).
  - [ ] Publish RFID events with dedupe and replay protection (RFID HW not yet integrated in this firmware).
  - [x] Add rate limiting so identity traffic cannot starve control frames.

## Stage F: Module Allocation and Power Path Integration
- Status: In Progress
- Dependencies: Stage B, Stage C
- Tasks:
  - [x] Implement controller-owned allocation begin/data/commit/abort transactions.
  - [x] Enforce hard clamp: only assigned modules are ever controlled via allowlist.
  - [x] Keep Relay1 + module enable/disable transitions safe on all exits.
  - [x] Ensure current-demand telemetry uses real module aggregate only.

## Stage G: Relay Ownership Model
- Status: In Progress
- Dependencies: Stage B
- Tasks:
  - [x] Keep Relay1 local-only ownership.
  - [x] Add Relay2/Relay3 external command path with safety interlocks and timeout-off.
  - [x] Publish applied relay state + reject reason codes.

## Stage H: Remove Unneeded Legacy Paths
- Status: In Progress
- Dependencies: Stage C, Stage D, Stage F
- Tasks:
  - [x] Remove EVSE_FAST-style assumptions from PLC firmware architecture.
  - [x] Remove duplicate/unused state machines that conflict with controller mode.
  - [x] Keep only one authority per decision path (no split-brain).

## Stage I: Fault Handling and Recovery
- Status: In Progress
- Dependencies: Stage C, Stage F, Stage G
- Tasks:
  - [x] Define fail-safe matrix for controller loss/auth expiry/CP drop in runtime logic.
  - [x] Add recovery rules for reconnect/re-arm/re-auth/session restart.
  - [ ] Add persistent fault/event journal (bounded ring) for post-mortem.

## Stage J: Verification Matrix (Host + ESP + Live)
- Status: In Progress
- Dependencies: Stage A..I
- Tasks:
  - [ ] Unit tests for control-plane decode/validation/state transitions.
  - [ ] Integration tests for standalone and controller modes.
  - [ ] Two-ESP/multi-controller contention tests for module ownership safety.
  - [x] Live tests on `/dev/ttyACM0` and `/dev/ttyACM1` with erase, flash, persisted provisioning, and serial verification.
  - [ ] End-to-end test: gun connect -> controller SLAC start -> auth pending->finish -> charging -> graceful stop.

## Stage K: Documentation and Operational Runbook
- Status: Completed
- Dependencies: Stage J
- Tasks:
  - [x] Update README with architecture modes and control authority table.
  - [x] Add CAN contract doc with all frames/signals/timers/error codes.
  - [x] Add commissioning checklist and failure-recovery playbooks.
  - [x] Add memory/CPU budget notes for SW4-gated WiFi manager path.

## Open Risks / Decisions To Lock
- [ ] Controller heartbeat timeout default (recommend 1200 ms).
- [ ] Auth TTL default and refresh cadence.
- [ ] SLAC arm validity window after gun connected.
- [ ] Identity event retention policy if controller temporarily offline.
- [ ] Security hardening level beyond targeted controller-id/module-address masking (trusted CAN vs authenticated control frames).
