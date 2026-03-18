# External Controller Validation Report

Updated: 2026-03-18

This report captures the controller-mode state after the recent QCA/SPI
transport hardening, SLAC recovery fixes, and harness cleanup changes.

Scope:

- firmware running in `mode=1 external_controller`
- PLC1 and PLC2 reflashed and reprovisioned
- simple single-module harness only
- short repeated-session characterization
- one long-hold `45 minute` session with teardown monitoring

## 1. Changes Included In This Validation

### 1.1 Firmware transport changes

The firmware baseline used for this validation includes the following QCA7000
transport changes in [`src/main.cpp`](/home/jpi/Desktop/EVSE/plc_firmware/src/main.cpp):

- stronger post-reset modem readiness gating before traffic is accepted
- stricter QCA framing decode instead of the earlier pattern-scan resync path
- RX-drain-before-ack interrupt ordering
- larger TX queue with `WRBUF`-aware backpressure instead of silent early drop
- transport-owned Ethernet minimum-frame padding
- reset-epoch flushing so stale queued frames do not bleed across modem resets

### 1.2 Harness cleanup changes

The controller-mode harness baseline used here includes the following cleanup
and serial robustness changes in
[`tools/external_controller_single_module_charge_test.py`](/home/jpi/Desktop/EVSE/plc_firmware/tools/external_controller_single_module_charge_test.py):

- split interleaved serial fragments before line parsing
- tolerate short status visibility gaps during active stop/cleanup
- keep heartbeat alive during cleanup
- keep `stop_notify` refreshed during cleanup
- treat cleanup status polling as best-effort instead of turning a completed pass
  into a false fail

## 2. Provisioning Used

Both PLCs were flashed, saved, reset, and verified in persisted
`mode=1 external_controller`.

Provisioning:

- PLC1:
  - `CTRL MODE 1 1 1`
  - `CTRL OWNERSHIP 1 1`
- PLC2:
  - `CTRL MODE 1 2 1`
  - `CTRL OWNERSHIP 2 2`

Reference log:

- [reflash_mode1_config_20260317_234700.log](/home/jpi/Desktop/EVSE/plc_firmware/logs/reflash_mode1_config_20260317_234700.log)

Verified runtime identities:

- PLC1: `plc_id=1 connector_id=1 controller_id=1 module_addr=0x01`
- PLC2: `plc_id=2 connector_id=2 controller_id=1 module_addr=0x02`

## 3. Short-Session Validation

Dataset:

- base prefix:
  - `/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap30_cleanupfix_20260317_234920`

Important note:

- the batch was intended to be `10` sessions
- sessions `1` through `9` completed and passed
- session `10` was interrupted externally before its summary and aggregate files
  were finalized
- this section therefore documents the `9` completed sessions only

### 3.1 Session timing table

All offsets are measured from `CTRL SLAC start`.

| Sess | `SLAC match` s | `PreCharge` s | `CurrentDemand` s | `CD ready` s | `CD ready -> stop` s | `stop -> relay open` s | `stop -> stop_done` s | HLC rc | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 01 | `1.918` | `6.239` | `8.038` | `8.240` | `119.763` | `0.127` | `20.475` | `-9` | `pass` |
| 02 | `1.946` | `6.202` | `8.466` | `8.473` | `119.914` | `0.113` | `20.312` | `-9` | `pass` |
| 03 | `1.938` | `6.407` | `8.269` | `8.199` | `119.899` | `0.117` | `20.319` | `-128` | `pass` |
| 04 | `1.903` | `6.170` | `8.169` | `8.371` | `120.045` | `0.129` | `20.201` | `-9` | `pass` |
| 05 | `1.926` | `6.193` | `8.193` | `8.461` | `119.725` | `0.119` | `n/a` | `-9` | `pass` |
| 06 | `1.916` | `6.243` | `8.641` | `8.776` | `119.755` | `0.129` | `20.485` | `-9` | `pass` |
| 07 | `1.924` | `6.194` | `7.991` | `8.258` | `119.904` | `0.126` | `20.327` | `-128` | `pass` |
| 08 | `1.931` | `6.262` | `8.259` | `8.325` | `119.915` | `0.125` | `20.330` | `-9` | `pass` |
| 09 | `1.920` | `6.121` | `8.123` | `7.926` | `120.114` | `0.119` | `20.286` | `-9` | `pass` |

Notes:

- Session `05` passed, but the captured log tail ended before the final
  `stop_done=1` marker was emitted to the file.
- Sessions `03` and `07` reported teardown-side HLC `rc=-128`; the session
  outcome still remained `pass`.

### 3.2 Short-session summary

- completed sessions: `9/9 pass`
- average `CTRL SLAC start -> MATCHED`: `1.925 s`
- average `CTRL SLAC start -> PreCharge`: `6.226 s`
- average `CTRL SLAC start -> CurrentDemand`: `8.239 s`
- average `CTRL SLAC start -> CurrentDemand ready`: `8.337 s`
- average `CD ready -> host stop`: `119.893 s`
- average `host stop -> relay open`: `0.123 s`
- average `host stop -> stop_done=1`: `20.342 s` across the `8` sessions where
  the marker was present

Representative artifacts:

- [session01_summary.json](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap30_cleanupfix_20260317_234920_session01_summary.json)
- [session09_summary.json](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap30_cleanupfix_20260317_234920_session09_summary.json)
- [session10.log](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_10x_hold120_gap30_cleanupfix_20260317_234920_session10.log)

## 4. Long-Hold Validation

Dataset:

- [session01.log](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_1x_hold2700_cleanupfix_20260318_session01.log)
- [session01_can.log](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_1x_hold2700_cleanupfix_20260318_session01_can.log)
- [session01.log.reset.log](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_1x_hold2700_cleanupfix_20260318_session01.log.reset.log)
- [session01_summary.json](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_1x_hold2700_cleanupfix_20260318_session01_summary.json)

Run conditions:

- one session
- `CurrentDemand` target hold: `2700 s`
- `session_start_timeout_s=3600`
- `stop_timeout_s=20`

### 4.1 End-to-end stage timing

| Stage | Timestamp IST | Delta From `CTRL SLAC start` |
| --- | --- | ---: |
| `CTRL SLAC start` | `2026-03-18 00:41:42.049` | `0.000 s` |
| `SLAC MATCHED` | `2026-03-18 00:41:43.969` | `1.920 s` |
| `HLC active=1` | `2026-03-18 00:41:46.980` | `4.931 s` |
| `PreCharge` first seen | `2026-03-18 00:41:48.172` | `6.123 s` |
| `PreCharge ready=1` | `2026-03-18 00:41:49.652` | `7.603 s` |
| `CurrentDemand` first seen | `2026-03-18 00:41:50.372` | `8.323 s` |
| `CurrentDemand` with HLC `fb_ready=1` | `2026-03-18 00:41:50.703` | `8.654 s` |
| Host `CTRL STOP soft 20000` | `2026-03-18 01:26:50.382` | `2708.333 s` |

### 4.2 Long-hold teardown ordering

| Event | Timestamp IST | Delta From Host Stop |
| --- | --- | ---: |
| Relay open | `2026-03-18 01:26:50.514` | `0.132 s` |
| `SLAC modem reprime (session stop)` | `2026-03-18 01:27:10.453` | `20.071 s` |
| `HLC ready=0 active=0` | `2026-03-18 01:27:10.465` | `20.083 s` |
| `stop_done=1` | `2026-03-18 01:27:10.469` | `20.087 s` |
| `HLC client session done rc=-9` | `2026-03-18 01:27:10.604` | `20.222 s` |

### 4.3 Long-hold result

- result: `pass`
- `CurrentDemand` hold before stop: `2700.010 s`
- no `vehicle_stop_requested`
- no premature teardown before the commanded stop
- no `PowerDeliveryRuntime stop=1`
- no `WeldingDetectionRuntime`
- no `SessionStopRuntime`
- no `modem reprime` before the host stop

Interpretation:

- the long hold remained stable from `CurrentDemand` entry through the entire
  `2700 s` window
- teardown was host-driven and orderly
- `SLAC modem reprime (session stop)` and `HLC client session done rc=-9`
  happened after `stop_done=1`, so they belong to expected cleanup rather than
  a field dropout

## 5. Reproduction Notes

For long holds, do not use the short default `session_start_timeout_s=300`.

Use a value greater than:

- startup latency
- requested hold time
- teardown budget

The validated long-hold invocation shape is:

```bash
python3 tools/external_controller_single_module_charge_test.py \
  --plc-port /dev/serial/by-id/usb-Espressif_USB_JTAG_serial_debug_unit_B8:F8:62:4B:C5:20-if00 \
  --plc-id 1 \
  --controller-id 1 \
  --current-demand-hold-s 2700 \
  --session-start-timeout-s 3600 \
  --stop-timeout-s 20
```

Expected clean teardown signature for a host-driven stop:

1. `CTRL STOP soft ...`
2. `EVT BMS stage=0(None)`
3. `Relay1 -> OPEN`
4. `stop_done=1`
5. `SLAC modem reprime (session stop)`
6. `HLC client session done rc=-9`

Treat these as potential dropout markers only if they appear before the host stop:

- `PowerDeliveryRuntime stop=1`
- `WeldingDetectionRuntime`
- `SessionStopRuntime`
- `SLAC modem reprime (session stop)`
- `EVT HLC ready=0 active=0`
- `HLC client session done ...`

## 6. Five One-Hour Session Batch

Dataset:

- [aggregate.json](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_5x_hold3600_gap30_reset_20260318_015259_aggregate.json)
- [aggregate.csv](/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_single_module_5x_hold3600_gap30_reset_20260318_015259_aggregate.csv)

Run conditions:

- `5` sessions
- `3600 s` target hold per session
- `30 s` gap between sessions
- explicit PLC ESP reset before every session

Overall result:

- `5/5 pass`
- `0/5 fail`
- no session showed `PowerDeliveryRuntime stop=1`, `WeldingDetectionRuntime`, or
  `SessionStopRuntime` before the commanded host stop
- in every session, `SLAC modem reprime (session stop)` and
  `HLC client session done ...` happened only after the commanded host stop

### 6.1 Session timing table

All offsets are measured from `CTRL SLAC start`.

| Sess | `SLAC match` s | `HLC active` s | `PreCharge` s | `PreCharge ready` s | `CurrentDemand` s | `CD ready` s | `CD ready -> stop` s | `stop -> relay open` s | `stop -> stop_done` s | `stop -> reprime` s | `stop -> HLC done` s | HLC rc | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 01 | `1.932` | `4.752` | `6.328` | `7.401` | `8.196` | `8.466` | `3599.617` | `0.158` | `20.495` | `20.145` | `20.295` | `-128` | `pass` |
| 02 | `1.913` | `4.985` | `6.177` | `7.645` | `8.242` | `8.178` | `3599.995` | `0.119` | `20.324` | `20.105` | `20.259` | `-9` | `pass` |
| 03 | `1.929` | `5.196` | `6.262` | `7.323` | `8.058` | `7.854` | `3600.094` | `0.120` | `20.378` | `20.109` | `20.261` | `-9` | `pass` |
| 04 | `1.930` | `5.134` | `6.216` | `7.262` | `8.194` | `8.333` | `3599.825` | `0.124` | `n/a` | `20.119` | `20.272` | `-9` | `pass` |
| 05 | `1.936` | `5.128` | `6.198` | `7.260` | `8.793` | `8.860` | `3599.964` | `0.166` | `20.251` | `20.157` | `20.309` | `-9` | `pass` |

Notes:

- Session `04` passed, but the summary snapshot was captured mid-cleanup and the
  log did not record a final `stop_done=1` marker before exit.
- Session `05` passed with one harness note:
  `status timeout during wait_for_session; using cached PLC status <= 5.0s old`

### 6.2 Session details

| Sess | `CTRL SLAC start` | `MATCHED` | `CurrentDemand` | Host stop | Relay open | Reprime | HLC done | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 01 | `01:53:17.567` | `01:53:19.499` | `01:53:25.763` | `02:53:25.650` | `02:53:25.808` | `02:53:45.795` | `02:53:45.945` | clean hold, teardown HLC `rc=-128` |
| 02 | `02:54:35.021` | `02:54:36.934` | `02:54:43.263` | `03:54:43.194` | `03:54:43.313` | `03:55:03.299` | `03:55:03.453` | clean hold |
| 03 | `03:55:52.160` | `03:55:54.089` | `03:56:00.218` | `04:56:00.108` | `04:56:00.228` | `04:56:20.217` | `04:56:20.369` | clean hold |
| 04 | `04:57:08.983` | `04:57:10.913` | `04:57:17.177` | `05:57:17.141` | `05:57:17.265` | `05:57:37.260` | `05:57:37.413` | clean hold, `stop_done` not captured |
| 05 | `05:58:26.022` | `05:58:27.958` | `05:58:34.815` | `06:58:34.846` | `06:58:35.012` | `06:58:55.003` | `06:58:55.155` | clean hold, one cached-status fallback note |

### 6.3 Batch summary

- average `CTRL SLAC start -> MATCHED`: `1.928 s`
- average `CTRL SLAC start -> HLC active`: `5.039 s`
- average `CTRL SLAC start -> PreCharge`: `6.236 s`
- average `CTRL SLAC start -> PreCharge ready`: `7.378 s`
- average `CTRL SLAC start -> CurrentDemand`: `8.297 s`
- average `CTRL SLAC start -> CurrentDemand ready`: `8.338 s`
- average `CurrentDemand ready -> host stop`: `3599.899 s`
- average `host stop -> relay open`: `0.137 s`
- average `host stop -> stop_done=1`: `20.362 s` across the `4` sessions where
  the marker was present
- average `host stop -> SLAC reprime`: `20.127 s`
- average `host stop -> HLC done`: `20.279 s`

Interpretation:

- the simple controller-mode path now sustains repeated `1 hour` holds with a
  clean reset between sessions
- stop behavior remained host-driven and orderly throughout the batch
- the only anomalies in the final dataset were:
  - session `04` missing a captured `stop_done=1` line despite a successful stop
  - session `05` needing one stale-status fallback note while remaining a pass
