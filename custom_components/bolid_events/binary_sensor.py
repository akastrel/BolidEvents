"""Binary sensors for Bolid Events."""

from __future__ import annotations

from homeassistant.components.binary_sensor import BinarySensorDeviceClass, BinarySensorEntity
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

from datetime import timedelta
from time import monotonic
import re

from .const import (
    DOMAIN,
    HEALTH_REFRESH_SECONDS,
    ZONE_UNAVAILABLE_HOLD_SECONDS,
    ZONE_UNAVAILABLE_THRESHOLD,
)

_ZONE_RAW_RE = re.compile(r"^sensor\.bolid_zone_(\d+)_raw$")


class BolidHealthBinarySensor(BinarySensorEntity):
    """Connectivity and event-integrity health of the critical Bolid path."""

    _attr_name = "Болид — связь"
    _attr_unique_id = "bolid_health"
    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _attr_should_poll = False

    def __init__(self, reader) -> None:
        self.reader = reader
        # Стабильный entity_id: на него можно безопасно завязать availability панели.
        self.entity_id = "binary_sensor.bolid_health"

        # Порог должен держаться непрерывно 60 секунд. Это отсекает обычные
        # единичные ошибки/перечитывания Modbus и стартовую инициализацию HA.
        self._zone_unavailable_since: float | None = None

    @property
    def is_on(self) -> bool | None:
        return self.reader.health_state

    @property
    def extra_state_attributes(self) -> dict:
        return {
            "reason": self.reader.health_reason,
            "last_success": self.reader.health_last_success,
            "last_event_number": self.reader.health_last_event_number,
            "unread_events": self.reader.health_unread_events,
            "unavailable_zone_count": self.reader.health_zone_unavailable_count,
            "unavailable_zones": list(self.reader.health_zone_unavailable_entities),
        }

    def _update_zone_data_health(self) -> None:
        """Detect sustained mass unavailability of Bolid raw-zone sensors."""
        unavailable: list[tuple[int, str]] = []

        for state in self.hass.states.async_all("sensor"):
            match = _ZONE_RAW_RE.match(state.entity_id)
            if match is None or state.state != "unavailable":
                continue
            unavailable.append((int(match.group(1)), state.entity_id))

        unavailable.sort()
        entity_ids = [entity_id for _, entity_id in unavailable]
        now = monotonic()

        if len(entity_ids) >= ZONE_UNAVAILABLE_THRESHOLD:
            if self._zone_unavailable_since is None:
                self._zone_unavailable_since = now
            fault = now - self._zone_unavailable_since >= ZONE_UNAVAILABLE_HOLD_SECONDS
        else:
            self._zone_unavailable_since = None
            fault = False

        self.reader.set_zone_data_health(fault, entity_ids)

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self._update_zone_data_health()

        @callback
        def _refresh(_now) -> None:
            self._update_zone_data_health()
            self.async_write_ha_state()

        self.async_on_remove(
            async_track_time_interval(
                self.hass,
                _refresh,
                timedelta(seconds=HEALTH_REFRESH_SECONDS),
            )
        )


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Set up the Bolid health entity discovered by the integration."""
    reader = hass.data[DOMAIN]
    async_add_entities([BolidHealthBinarySensor(reader)])
