"""Bolid Events integration."""

from __future__ import annotations

import logging
from pathlib import Path

import voluptuous as vol

from homeassistant.const import EVENT_HOMEASSISTANT_STOP, Platform
from homeassistant.core import Event, HomeAssistant
from homeassistant.helpers import config_validation as cv, discovery
from homeassistant.helpers.typing import ConfigType

from .const import (
    CONF_DIAGNOSTIC_LOG_FILE,
    CONF_EVENT_LOG_FILE,
    CONF_HUB,
    CONF_MAX_BATCH,
    CONF_POLL_INTERVAL,
    CONF_READ_RETRIES,
    CONF_REPLAY_ON_START,
    CONF_RETRY_DELAY,
    CONF_SLAVE,
    CONF_STATUS_INTERVAL,
    DEFAULT_DIAGNOSTIC_LOG_FILE,
    DEFAULT_EVENT_LOG_FILE,
    DEFAULT_HUB,
    DEFAULT_MAX_BATCH,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_READ_RETRIES,
    DEFAULT_REPLAY_ON_START,
    DEFAULT_RETRY_DELAY,
    DEFAULT_SLAVE,
    DEFAULT_STATUS_INTERVAL,
    DOMAIN,
    LOAD_MONITOR_INTERVAL,
    LOAD_SUMMARY_INTERVAL,
    VERSION,
)
from .file_log import BolidFileLogs
from .reader import BolidEventReader

_LOGGER = logging.getLogger(__name__)

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Optional(CONF_HUB, default=DEFAULT_HUB): cv.string,
                vol.Optional(CONF_SLAVE, default=DEFAULT_SLAVE): vol.All(vol.Coerce(int), vol.Range(min=1, max=247)),
                vol.Optional(CONF_POLL_INTERVAL, default=DEFAULT_POLL_INTERVAL): vol.All(vol.Coerce(float), vol.Range(min=0.5, max=60.0)),
                vol.Optional(CONF_RETRY_DELAY, default=DEFAULT_RETRY_DELAY): vol.All(vol.Coerce(float), vol.Range(min=0.1, max=10.0)),
                vol.Optional(CONF_READ_RETRIES, default=DEFAULT_READ_RETRIES): vol.All(vol.Coerce(int), vol.Range(min=1, max=10)),
                vol.Optional(CONF_MAX_BATCH, default=DEFAULT_MAX_BATCH): vol.All(vol.Coerce(int), vol.Range(min=1, max=256)),
                vol.Optional(CONF_STATUS_INTERVAL, default=DEFAULT_STATUS_INTERVAL): vol.All(vol.Coerce(float), vol.Range(min=5.0, max=3600.0)),
                vol.Optional(CONF_EVENT_LOG_FILE, default=DEFAULT_EVENT_LOG_FILE): cv.string,
                vol.Optional(CONF_DIAGNOSTIC_LOG_FILE, default=DEFAULT_DIAGNOSTIC_LOG_FILE): cv.string,
                vol.Optional(CONF_REPLAY_ON_START, default=DEFAULT_REPLAY_ON_START): cv.boolean,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up Bolid Events from YAML."""
    conf = config.get(DOMAIN)
    if conf is None:
        return True

    # Production mode is intentionally light. Deep Modbus instrumentation is
    # installed only when the integration logger is effectively DEBUG.
    deep_diagnostics = _LOGGER.isEnabledFor(logging.DEBUG)

    try:
        event_path = (
            Path(hass.config.path(conf[CONF_EVENT_LOG_FILE]))
            if deep_diagnostics
            else None
        )
        diagnostic_path = (
            Path(hass.config.path(conf[CONF_DIAGNOSTIC_LOG_FILE]))
            if deep_diagnostics
            else None
        )

        file_logs = BolidFileLogs(event_path=event_path, diagnostic_path=diagnostic_path)
        reader = BolidEventReader(
            hass=hass,
            hub_name=conf[CONF_HUB],
            slave=conf[CONF_SLAVE],
            poll_interval=conf[CONF_POLL_INTERVAL],
            retry_delay=conf[CONF_RETRY_DELAY],
            read_retries=conf[CONF_READ_RETRIES],
            max_batch=conf[CONF_MAX_BATCH],
            status_interval=conf[CONF_STATUS_INTERVAL],
            replay_on_start=conf[CONF_REPLAY_ON_START],
            deep_diagnostics=deep_diagnostics,
            load_monitor_interval=LOAD_MONITOR_INTERVAL,
            load_summary_interval=LOAD_SUMMARY_INTERVAL,
            file_logs=file_logs,
        )
    except KeyError:
        _LOGGER.exception("Cannot find Modbus hub '%s'. Check bolid_events.hub", conf[CONF_HUB])
        return False

    hass.data[DOMAIN] = reader
    await discovery.async_load_platform(
        hass,
        Platform.BINARY_SENSOR,
        DOMAIN,
        {},
        config,
    )
    reader.start()

    _LOGGER.info(
        "Bolid Events v%s started: deep diagnostics and JSONL logging %s",
        VERSION,
        "enabled" if deep_diagnostics else "disabled",
    )

    async def _stop_reader(event: Event) -> None:
        await reader.stop()

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _stop_reader)
    return True
