"""Continuous С2000-ПП event queue consumer."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from time import monotonic
from typing import Any
from uuid import uuid4

from homeassistant.components.modbus import get_hub
from homeassistant.components.modbus.const import (
    CALL_TYPE_REGISTER_HOLDING,
    CALL_TYPE_WRITE_REGISTER,
)
from homeassistant.core import HomeAssistant

from .const import (
    EVENT_BOLID,
    EVENT_NUMBER_MODULUS,
    EVENT_REG_COUNT,
    HEALTH_TIMEOUT_SECONDS,
    REG_EVENT_READ_ACK,
    REG_NEWEST_EVENT,
    REG_OLDEST_EVENT,
    REG_OLDEST_UNREAD_EVENT,
    REG_UNREAD_EVENTS,
    VERSION,
)
from .file_log import BolidFileLogs
from .load_monitor import ModbusHubLoadMonitor
from .parser import parse_event

_LOGGER = logging.getLogger(__name__)


class ModbusHubUnavailable(RuntimeError):
    """The configured HA Modbus hub is temporarily unavailable."""


def _utc_now() -> str:
    """UTC timestamp suitable for JSON."""
    return datetime.now(timezone.utc).isoformat()


class BolidEventReader:
    """Consume the С2000-ПП unread-event queue using 46264 -> 46163."""

    def __init__(
        self,
        hass: HomeAssistant,
        hub_name: str,
        slave: int,
        poll_interval: float,
        retry_delay: float,
        read_retries: int,
        max_batch: int,
        status_interval: float,
        replay_on_start: bool,
        deep_diagnostics: bool,
        load_monitor_interval: float,
        load_summary_interval: float,
        file_logs: BolidFileLogs,
    ) -> None:
        self.hass = hass
        self.hub_name = hub_name
        self.slave = slave
        self.poll_interval = poll_interval
        self.retry_delay = retry_delay
        self.read_retries = read_retries
        self.max_batch = max_batch
        self.status_interval = status_interval
        self.replay_on_start = replay_on_start
        self.deep_diagnostics = deep_diagnostics
        self.load_monitor_interval = load_monitor_interval
        self.load_summary_interval = load_summary_interval
        self.file_logs = file_logs

        self.hub = get_hub(hass, hub_name)
        self.session_id = str(uuid4())

        self._task: asyncio.Task | None = None
        self._load_monitor: ModbusHubLoadMonitor | None = None
        self._stopping = False
        self._initialized = False
        self._last_status_time = 0.0

        # Home Assistant recreates ModbusHub objects during modbus.reload.
        # Never assume that the object obtained at component startup remains
        # current for the lifetime of this reader.
        self._hub_generation = 0
        self._hub_unavailable = False
        self._hub_rebind_pending = False
        self._logs_ready = False

        # Health layer is deliberately based on FIFO progress, not on every
        # polling sensor. A completed empty read or a successfully ACK'ed event
        # proves that the critical HA -> gateway -> PP path is functioning.
        self._health_started = monotonic()
        self._health_last_success_monotonic: float | None = None
        self._health_last_success_at: str | None = None
        self._health_unread_events: int | None = None
        self._health_last_seen_event: int | None = None
        self._event_gap_detected = False

        # Отдельно контролируем массовую недоступность raw-зон в HA.
        # Это production-safe признак режима, который мы наблюдали при
        # массовых Modbus Exception 15: FIFO ещё жив, но состояния зон
        # перестают нормально обновляться.
        self._zone_data_unavailable = False
        self._zone_unavailable_count = 0
        self._zone_unavailable_entities: tuple[str, ...] = ()

        # При replay_on_start=false события, уже лежавшие в ПП при старте,
        # логируются и ACK'аются, но в HA event bus не публикуются.
        self._startup_unread = 0
        self._startup_drained = 0
        self._startup_cutoff_event: int | None = None
        self._startup_backlog_done = False

        # Если event bus уже получил событие, но ACK не прошёл, при следующей
        # попытке то же событие повторно в HA не публикуем.
        self._pending_event_number: int | None = None
        self._pending_was_published = False
        self._pending_was_logged = False
        self._pending_was_startup_backlog = False

        self._last_acked_event: int | None = None
        self._last_published_event: int | None = None

    @property
    def health_state(self) -> bool | None:
        """True=healthy, False=fault, None=startup grace period."""
        if self._event_gap_detected:
            return False

        if self._zone_data_unavailable:
            return False

        if self._health_last_success_monotonic is None:
            if monotonic() - self._health_started >= HEALTH_TIMEOUT_SECONDS:
                return False
            return None

        return (
            monotonic() - self._health_last_success_monotonic
            < HEALTH_TIMEOUT_SECONDS
        )

    @property
    def health_reason(self) -> str:
        """Compact machine-readable health reason."""
        if self._event_gap_detected:
            return "event_gap"
        if self._health_last_success_monotonic is None:
            return (
                "communication_timeout"
                if monotonic() - self._health_started >= HEALTH_TIMEOUT_SECONDS
                else "starting"
            )
        if monotonic() - self._health_last_success_monotonic >= HEALTH_TIMEOUT_SECONDS:
            return "communication_timeout"
        if self._zone_data_unavailable:
            return "zone_data_unavailable"
        return "ok"

    @property
    def health_last_success(self) -> str | None:
        return self._health_last_success_at

    @property
    def health_last_event_number(self) -> int | None:
        return self._last_acked_event

    @property
    def health_unread_events(self) -> int | None:
        return self._health_unread_events

    @property
    def health_zone_unavailable_count(self) -> int:
        return self._zone_unavailable_count

    @property
    def health_zone_unavailable_entities(self) -> tuple[str, ...]:
        return self._zone_unavailable_entities

    def set_zone_data_health(
        self,
        unavailable: bool,
        entity_ids: list[str],
    ) -> None:
        """Update aggregate raw-zone data health from HA entity states."""
        self._zone_data_unavailable = unavailable
        self._zone_unavailable_count = len(entity_ids)
        self._zone_unavailable_entities = tuple(entity_ids)

    def _mark_health_success(self) -> None:
        """Record successful FIFO progress."""
        self._health_last_success_monotonic = monotonic()
        self._health_last_success_at = _utc_now()

    async def _check_event_sequence(self, event_number: int) -> None:
        """Detect a missing event, including 16-bit event-number wraparound."""
        previous = self._health_last_seen_event
        if previous is not None:
            expected = (previous + 1) % EVENT_NUMBER_MODULUS
            if event_number != expected:
                self._event_gap_detected = True
                await self._error(
                    "event_gap",
                    previous_event_number=previous,
                    expected_event_number=expected,
                    actual_event_number=event_number,
                )
        self._health_last_seen_event = event_number

    def start(self) -> None:
        """Start reader background task."""
        if self._task is not None:
            return

        self._task = self.hass.async_create_background_task(
            self._run(),
            "bolid-events-reader",
        )

    async def stop(self) -> None:
        """Stop reader background task."""
        self._stopping = True

        if self._task is None:
            return

        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass

        self._task = None

        if self._load_monitor is not None:
            await self._load_monitor.stop()
            self._load_monitor = None

    async def _diag(
        self,
        level: str,
        message: str,
        **data: Any,
    ) -> None:
        """Write structured diagnostics only when DEBUG was enabled at startup."""
        if not self.deep_diagnostics:
            return

        record = {
            "timestamp": _utc_now(),
            "level": level,
            "message": message,
            "reader_version": VERSION,
            "session_id": self.session_id,
            **data,
        }
        await self.file_logs.diagnostics.write(record)

    async def _warn(self, message: str, **data: Any) -> None:
        """Warning: keep it visible in Core HA and duplicate into diagnostics."""
        _LOGGER.warning("%s | %s", message, data if data else "")
        await self._diag("warning", message, **data)

    async def _error(self, message: str, **data: Any) -> None:
        """Error: keep it visible in Core HA and duplicate into diagnostics."""
        _LOGGER.error("%s | %s", message, data if data else "")
        await self._diag("error", message, **data)

    async def _start_load_monitor(self) -> None:
        """Attach the load monitor to the currently active HA Modbus hub."""
        if not self.deep_diagnostics or self._load_monitor is not None:
            return

        self._load_monitor = ModbusHubLoadMonitor(
            hub=self.hub,
            diagnostic_callback=self._diag,
            report_interval=self.load_monitor_interval,
            summary_interval=self.load_summary_interval,
        )
        await self._load_monitor.start()

    async def _mark_hub_unavailable(self, reason: str) -> None:
        """Log one outage record and then wait quietly for the hub to return."""
        if self._hub_unavailable:
            return

        self._hub_unavailable = True

        _LOGGER.warning(
            "Bolid Events: Modbus hub '%s' temporarily unavailable (%s); "
            "waiting for Home Assistant to recreate/reconnect it",
            self.hub_name,
            reason,
        )

        if self._logs_ready:
            await self._diag(
                "warning",
                "modbus_hub_unavailable",
                hub=self.hub_name,
                reason=reason,
                hub_generation=self._hub_generation,
            )

    async def _ensure_current_hub(self) -> bool:
        """Resolve the current HA Modbus hub and survive modbus.reload.

        Home Assistant closes the old ModbusHub during reload and then creates
        a new object under the same hub name. The reader must therefore compare
        object identity instead of keeping the startup object forever.
        """
        try:
            current_hub = get_hub(self.hass, self.hub_name)
        except (KeyError, TypeError):
            await self._mark_hub_unavailable("hub_not_registered")
            return False

        hub_changed = current_hub is not self.hub

        if hub_changed:
            # Stop wrappers on the old object before switching to the new one.
            # Pending event state is intentionally NOT cleared: if reload occurs
            # after HA publication but before ACK, the same unread event will be
            # ACK'ed after rebind without duplicate publication/logging.
            if self._load_monitor is not None:
                await self._load_monitor.stop()
                self._load_monitor = None

            self.hub = current_hub
            self._hub_generation += 1
            self._hub_rebind_pending = True

        # async_close() leaves the old object registered briefly but clears
        # _client. Do not call async_pb_call() in that state: it returns None
        # immediately and would otherwise look like a PP communication failure.
        if getattr(self.hub, "_client", None) is None:
            await self._mark_hub_unavailable("client_not_ready")
            return False

        connected = getattr(self.hub, "event_connected", None)
        if connected is not None and not connected.is_set():
            await self._mark_hub_unavailable("waiting_for_connection")
            return False

        # After initialization, monitoring follows the active hub too.
        if self._initialized and self.deep_diagnostics:
            await self._start_load_monitor()

        if self._hub_rebind_pending:
            _LOGGER.info(
                "Bolid Events: rebound to current Modbus hub '%s' "
                "(generation %s)",
                self.hub_name,
                self._hub_generation,
            )

            if self._logs_ready:
                await self._diag(
                    "info",
                    "modbus_hub_rebound",
                    hub=self.hub_name,
                    hub_generation=self._hub_generation,
                    configured_message_wait_ms=(
                        self._load_monitor.configured_message_wait_ms
                        if self._load_monitor is not None
                        else None
                    ),
                )

            self._hub_rebind_pending = False
            self._hub_unavailable = False

        elif self._hub_unavailable:
            # Same ModbusHub object may also recover after reconnect/restart.
            _LOGGER.info(
                "Bolid Events: Modbus hub '%s' is available again",
                self.hub_name,
            )

            if self._logs_ready:
                await self._diag(
                    "info",
                    "modbus_hub_recovered",
                    hub=self.hub_name,
                    hub_generation=self._hub_generation,
                )

            self._hub_unavailable = False

        return True

    async def _read_register(self, address: int) -> int | None:
        """Read one holding register through the current shared HA Modbus hub."""
        if not await self._ensure_current_hub():
            return None
        result = await self.hub.async_pb_call(
            self.slave,
            address,
            1,
            CALL_TYPE_REGISTER_HOLDING,
        )

        registers = (
            getattr(result, "registers", None)
            if result is not None
            else None
        )

        if not registers:
            return None

        return int(registers[0])

    async def _read_oldest_unread(self) -> dict[str, Any] | None:
        """Read the oldest unread event from 46264.

        An all-zero 28-byte record means there are no unread events.
        """
        for attempt in range(1, self.read_retries + 1):
            if not await self._ensure_current_hub():
                raise ModbusHubUnavailable(
                    f"Modbus hub '{self.hub_name}' is temporarily unavailable"
                )

            result = await self.hub.async_pb_call(
                self.slave,
                REG_OLDEST_UNREAD_EVENT,
                EVENT_REG_COUNT,
                CALL_TYPE_REGISTER_HOLDING,
            )

            registers = (
                getattr(result, "registers", None)
                if result is not None
                else None
            )

            if registers and len(registers) >= EVENT_REG_COUNT:
                return parse_event(list(registers[:EVENT_REG_COUNT]))

            if attempt < self.read_retries:
                await asyncio.sleep(self.retry_delay)

        raise RuntimeError(
            f"С2000-ПП did not return 14 registers from {REG_OLDEST_UNREAD_EVENT}"
        )

    async def _ack_event(self, event_number: int) -> bool:
        """Mark exactly this event as read: 46163 = event_number."""
        for attempt in range(1, self.read_retries + 1):
            if not await self._ensure_current_hub():
                raise ModbusHubUnavailable(
                    f"Modbus hub '{self.hub_name}' is temporarily unavailable"
                )

            result = await self.hub.async_pb_call(
                self.slave,
                REG_EVENT_READ_ACK,
                event_number,
                CALL_TYPE_WRITE_REGISTER,
            )

            if result is not None:
                return True

            if attempt < self.read_retries:
                await asyncio.sleep(self.retry_delay)

        return False

    def _is_startup_backlog(self, event_number: int) -> bool:
        """Return True while draining events that existed before reader startup."""
        if self.replay_on_start or self._startup_backlog_done:
            return False

        # Primary boundary: newest event number that existed at startup.
        if self._startup_cutoff_event is not None:
            return True

        # Fallback for unusual PP responses where newest could not be read.
        return self._startup_drained < self._startup_unread

    def _finish_startup_backlog_if_needed(self, event_number: int) -> None:
        """Finish backlog suppression at the startup newest-event marker."""
        if self.replay_on_start or self._startup_backlog_done:
            return

        self._startup_drained += 1

        if (
            self._startup_cutoff_event is not None
            and event_number == self._startup_cutoff_event
        ):
            self._startup_backlog_done = True
            return

        # Safety fallback: never suppress more successfully ACK'ed events than
        # the unread count observed at startup.
        if self._startup_drained >= self._startup_unread:
            self._startup_backlog_done = True

    async def _log_event_record(
        self,
        event: dict[str, Any],
        startup_backlog: bool,
        published: bool,
    ) -> None:
        """Persist full event before ACK when DEBUG JSONL logging is enabled."""
        record = {
            **event,
            "reader_version": VERSION,
            "session_id": self.session_id,
            "read_at": _utc_now(),
            "startup_backlog": startup_backlog,
            "published_to_ha": published,
        }
        await self.file_logs.events.write(record)

    async def _publish_event(
        self,
        event: dict[str, Any],
        startup_backlog: bool,
    ) -> None:
        """Log and, for live events, publish before ACK (at-least-once)."""
        event_number = event["event_number"]

        # New pending event: validate sequence once, then determine whether it
        # is startup backlog. Re-reading the same un-ACK'ed event is not a gap.
        if self._pending_event_number != event_number:
            await self._check_event_sequence(event_number)
            self._pending_event_number = event_number
            self._pending_was_published = False
            self._pending_was_logged = False
            self._pending_was_startup_backlog = startup_backlog

        if not self._pending_was_logged:
            await self._log_event_record(
                event,
                startup_backlog=self._pending_was_startup_backlog,
                published=not self._pending_was_startup_backlog,
            )
            self._pending_was_logged = True

        if (
            not self._pending_was_startup_backlog
            and not self._pending_was_published
        ):
            event_data = {
                **event,
                "received_at": _utc_now(),
                "source_hub": self.hub_name,
                "source_slave": self.slave,
                "reader_version": VERSION,
            }

            self.hass.bus.async_fire(EVENT_BOLID, event_data)
            self._pending_was_published = True
            self._last_published_event = event_number

            _LOGGER.info(
                "Bolid event #%s: code=%s (%s), zone=%s, section=%s, relay=%s, time=%s",
                event_number,
                event["event_code"],
                event["event_name"],
                event.get("zone"),
                event.get("section"),
                event.get("relay"),
                event.get("timestamp"),
            )

    async def _initialize(self) -> bool:
        """Capture queue boundary and initialize files."""
        # Monitor the SAME HA Modbus hub used by normal entities and this reader.
        # It adds no Modbus traffic of its own: only transparent timing wrappers.
        await self._start_load_monitor()

        unread = await self._read_register(REG_UNREAD_EVENTS)
        newest = await self._read_register(REG_NEWEST_EVENT)
        oldest = await self._read_register(REG_OLDEST_EVENT)

        if unread is None:
            return False

        self._startup_unread = unread
        self._health_unread_events = unread
        self._startup_cutoff_event = newest if unread > 0 else None
        self._startup_backlog_done = (
            self.replay_on_start or unread == 0
        )
        self._last_status_time = monotonic()
        self._initialized = True

        _LOGGER.info(
            "Bolid Events v%s started: hub=%s slave=%s newest=%s oldest=%s "
            "unread=%s replay_on_start=%s. Consumer mode: 46264 -> 46163.",
            VERSION,
            self.hub_name,
            self.slave,
            newest,
            oldest,
            unread,
            self.replay_on_start,
        )

        await self._diag(
            "info",
            "reader_started",
            hub=self.hub_name,
            slave=self.slave,
            newest=newest,
            oldest=oldest,
            unread=unread,
            replay_on_start=self.replay_on_start,
            startup_cutoff_event=self._startup_cutoff_event,
        )

        if unread and not self.replay_on_start:
            _LOGGER.info(
                "Bolid Events: draining %s startup unread event(s) without "
                "publishing them to HA%s",
                unread,
                "; DEBUG event log enabled" if self.deep_diagnostics else "",
            )

        return True

    async def _log_status_if_due(self) -> None:
        """Refresh unread count; write detailed status only in DEBUG mode."""
        now = monotonic()
        if now - self._last_status_time < self.status_interval:
            return

        unread = await self._read_register(REG_UNREAD_EVENTS)
        if unread is not None:
            self._health_unread_events = unread

        if self.deep_diagnostics:
            newest = await self._read_register(REG_NEWEST_EVENT)
            oldest = await self._read_register(REG_OLDEST_EVENT)

            _LOGGER.debug(
                "Bolid event reader status: last_acked=%s last_published=%s "
                "newest=%s oldest=%s unread=%s startup_done=%s",
                self._last_acked_event,
                self._last_published_event,
                newest,
                oldest,
                unread,
                self._startup_backlog_done,
            )

            await self._diag(
                "info",
                "reader_status",
                last_acked=self._last_acked_event,
                last_published=self._last_published_event,
                newest=newest,
                oldest=oldest,
                unread=unread,
                startup_backlog_done=self._startup_backlog_done,
                startup_drained=self._startup_drained,
                startup_unread=self._startup_unread,
                health_reason=self.health_reason,
            )

        self._last_status_time = now

    def _clear_pending(self) -> None:
        self._pending_event_number = None
        self._pending_was_published = False
        self._pending_was_logged = False
        self._pending_was_startup_backlog = False

    async def _consume_batch(self) -> int:
        """Consume up to max_batch events and return count successfully ACK'ed."""
        consumed = 0

        for _ in range(self.max_batch):
            event = await self._read_oldest_unread()

            # Queue empty: a successful 46264 response is our normal heartbeat.
            if event is None:
                self._health_unread_events = 0
                self._mark_health_success()
                break

            event_number = event["event_number"]
            startup_backlog = self._is_startup_backlog(event_number)

            # Persist/publish before ACK so a successful ACK cannot silently lose
            # an event due to a crash between ACK and Home Assistant delivery.
            await self._publish_event(
                event,
                startup_backlog=startup_backlog,
            )

            if not await self._ack_event(event_number):
                await self._warn(
                    "event_ack_failed",
                    event_number=event_number,
                    register=REG_EVENT_READ_ACK,
                    published_to_ha=self._pending_was_published,
                    startup_backlog=self._pending_was_startup_backlog,
                )
                # Keep pending state. The same oldest unread event will be retried
                # without duplicate HA publication or duplicate event-file record.
                return consumed

            self._last_acked_event = event_number
            if self._health_unread_events is not None and self._health_unread_events > 0:
                self._health_unread_events -= 1
            self._mark_health_success()
            consumed += 1

            if startup_backlog:
                self._finish_startup_backlog_if_needed(event_number)

                if (
                    self._startup_backlog_done
                    or self._startup_drained % 32 == 0
                ):
                    _LOGGER.info(
                        "Bolid startup backlog: drained=%s/%s done=%s",
                        self._startup_drained,
                        self._startup_unread,
                        self._startup_backlog_done,
                    )

            self._clear_pending()

            # Do not monopolize HA's event loop while draining a large backlog.
            if consumed % 8 == 0:
                await asyncio.sleep(0)

        return consumed

    async def _run(self) -> None:
        """Reader loop."""
        # Both custom JSONL logs are DEBUG-only; no-op sinks are used in production.
        await self.file_logs.initialize()
        self._logs_ready = self.deep_diagnostics

        while not self._stopping:
            try:
                # Re-resolve on every loop. During modbus.reload the old hub is
                # closed before the new one is registered; that gap is expected.
                if not await self._ensure_current_hub():
                    await asyncio.sleep(self.poll_interval)
                    continue

                if not self._initialized:
                    if not await self._initialize():
                        await asyncio.sleep(self.poll_interval)
                        continue

                consumed = await self._consume_batch()
                await self._log_status_if_due()

                # Full batch means there may still be queued events. Continue
                # immediately instead of waiting poll_interval.
                if consumed >= self.max_batch:
                    await asyncio.sleep(0)
                    continue

            except asyncio.CancelledError:
                raise

            except ModbusHubUnavailable:
                # Expected transient state during modbus.reload/reconnect.
                # _ensure_current_hub() already emitted one lifecycle diagnostic.
                pass

            except RuntimeError as err:
                await self._warn(
                    "event_queue_read_failed",
                    error=str(err),
                    register=REG_OLDEST_UNREAD_EVENT,
                )

            except Exception as err:
                _LOGGER.exception("Unexpected error in Bolid event reader")
                await self._error(
                    "unexpected_reader_error",
                    error=repr(err),
                )

            # Empty queue or transient error: normal low-rate polling.
            await asyncio.sleep(self.poll_interval)
