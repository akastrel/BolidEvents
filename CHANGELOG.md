# Changelog

## 1.0.2

- Added `zone_data_unavailable` health reason.
- Health now detects sustained mass loss of raw Bolid zone states: at least 5 `sensor.bolid_zone_*_raw` entities unavailable for 60 seconds.
- Added `unavailable_zone_count` and `unavailable_zones` health attributes.
- Production health check does not require deep/private Modbus hooks.

## 1.0.1

- Made both custom JSONL files DEBUG-only.
- Normal INFO/WARNING/ERROR operation creates no integration-owned JSONL files.
- Deep Modbus instrumentation remains DEBUG-only.

## 1.0.0

- First production-oriented release.
- Added `binary_sensor.bolid_health`.
- Added FIFO heartbeat and event-gap detection.
- Preserved FIFO read/publish/ACK deduplication logic.
- Added recovery after Home Assistant `modbus.reload` by rebinding to the current Modbus hub.
- Deep diagnostics controlled by the effective Home Assistant logger level.

## 0.2.5

- Added deep Modbus diagnostics for troubleshooting.
- Captures slow calls, failed calls and raw pymodbus responses before Home Assistant reduces them to generic errors.

## 0.2.4

- Fixed stale `ModbusHub` reference after `modbus.reload`.
- Reader now detects hub replacement and rebinds automatically.

## 0.2.0

- Switched the reader to the `С2000-ПП` oldest-unread FIFO: read `46264`, publish/persist, ACK exact event number through `46163`.
