# Bolid Events

Home Assistant custom integration for reading the unread-event FIFO of a Bolid `С2000-ПП` through the existing Home Assistant Modbus hub.

Current version: **1.0.2**.

## What it does

The integration implements the event FIFO of `С2000-ПП`:

```text
46264 (oldest unread event) -> publish to Home Assistant -> 46163 (ACK exact event number)
```

Key properties:

- uses the same Home Assistant Modbus hub as the regular Bolid entities;
- publishes live Bolid events to the Home Assistant event bus as `bolid_event`;
- does not publish the same event twice when ACK has to be retried;
- handles `modbus.reload` by rebinding to the newly created `ModbusHub`;
- checks event-number continuity, including the 16-bit wrap `65535 -> 0`;
- provides a compact health entity: `binary_sensor.bolid_health`.

The integration is intended as an HA integration/dispatch layer. It is not a replacement for autonomous security/fire logic in the Bolid system itself.

## Health

The integration creates:

```text
binary_sensor.bolid_health
```

Device class: `connectivity`.

Health is `ON` when the critical event path is progressing normally and raw zone data is not broadly degraded.

Health becomes `OFF` for one of these reasons:

- `communication_timeout` — no successful FIFO progress for 30 seconds;
- `event_gap` — a gap in event numbering was detected;
- `zone_data_unavailable` — at least 5 existing `sensor.bolid_zone_*_raw` entities remain `unavailable` continuously for 60 seconds.

During initial startup the reason is `starting`; normal operation uses `ok`.

Attributes:

- `reason`;
- `last_success`;
- `last_event_number`;
- `unread_events`;
- `unavailable_zone_count`;
- `unavailable_zones`.

An `event_gap` is intentionally sticky until integration/Home Assistant restart: a confirmed missing event cannot be reconstructed by the reader.

## Production logging

Normal production mode does **not** install deep Modbus hooks and does **not** create custom JSONL log files.

Recommended logger setting:

```yaml
logger:
  logs:
    custom_components.bolid_events: warning
```

For troubleshooting, enable DEBUG and restart Home Assistant:

```yaml
logger:
  default: info
  logs:
    custom_components.bolid_events: debug
```

DEBUG mode additionally enables:

- `bolid_events.jsonl` — decoded event log;
- `bolid_events_diagnostic.jsonl` — Modbus timing/failure diagnostics;
- deep Modbus instrumentation used to capture slow calls and raw pymodbus error responses.

The DEBUG decision is made when the integration starts, so changing the logger level requires a Home Assistant restart to enable/disable the deep instrumentation.

## Configuration

Minimal YAML:

```yaml
bolid_events:
  hub: bolid
  slave: 1
  poll_interval: 1
```

Optional settings with their default values:

```yaml
bolid_events:
  hub: bolid
  slave: 1
  poll_interval: 1
  retry_delay: 0.5
  read_retries: 3
  max_batch: 32
  status_interval: 30
  event_log_file: bolid_events.jsonl
  diagnostic_log_file: bolid_events_diagnostic.jsonl
  replay_on_start: false
```

`event_log_file` and `diagnostic_log_file` are used only when DEBUG diagnostics are enabled.

## Installation

Copy:

```text
custom_components/bolid_events/
```

into:

```text
/config/custom_components/bolid_events/
```

and perform a full Home Assistant restart.

## Notes about the Bolid Modbus path

This integration assumes that one reader owns the `С2000-ПП` unread-event FIFO. ACK at register `46163` consumes an unread event from the point of view of other Modbus consumers.

The production event path is intentionally kept separate from optional numeric/analog polling experiments. Deep diagnostics can be enabled temporarily without changing the FIFO protocol itself.
