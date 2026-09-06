"""Parser for С2000-ПП event records."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .const import EVENT_BYTE_COUNT, EVENT_NAMES, FIELD_DATETIME, FIELD_NAMES


def registers_to_bytes(registers: list[int]) -> list[int]:
    """Convert 16-bit Modbus registers to bytes in Modbus wire order."""
    data: list[int] = []
    for register in registers:
        data.append((register >> 8) & 0xFF)
        data.append(register & 0xFF)
    return data


def _uint16(data: list[int]) -> int:
    """Decode a big-endian two-byte unsigned integer."""
    return (data[0] << 8) | data[1]


def _parse_datetime(data: list[int]) -> str | None:
    """Decode Bolid event time: hour, minute, second, day, month, year."""
    if len(data) != 6:
        return None

    hour, minute, second, day, month, year = data
    try:
        return datetime(2000 + year, month, day, hour, minute, second).isoformat()
    except ValueError:
        return None


def parse_event(registers: list[int]) -> dict[str, Any] | None:
    """Parse one 28-byte event record.

    If there are no unread events, С2000-ПП returns an all-zero event.
    """
    if len(registers) < 14:
        raise ValueError(f"Expected 14 registers, got {len(registers)}")

    raw = registers_to_bytes(registers[:14])
    if len(raw) != EVENT_BYTE_COUNT:
        raise ValueError(f"Expected {EVENT_BYTE_COUNT} bytes, got {len(raw)}")

    if not any(raw):
        return None

    event_number = _uint16(raw[0:2])
    description_length = raw[2]
    event_code = raw[3]

    significant_field_bytes = max(0, min(24, description_length - 1))
    fields_data = raw[4 : 4 + significant_field_bytes]

    parsed: dict[str, Any] = {
        "event_number": event_number,
        "event_code": event_code,
        "event_name": EVENT_NAMES.get(event_code, f"Неизвестное событие {event_code}"),
        "description_length": description_length,
        "fields": [],
        "raw_hex": " ".join(f"{byte:02X}" for byte in raw),
        "raw_registers": registers[:14],
    }

    offset = 0
    while offset + 2 <= len(fields_data):
        field_type = fields_data[offset]
        field_length = fields_data[offset + 1]
        start = offset + 2
        end = start + field_length

        if end > len(fields_data):
            parsed["fields"].append(
                {
                    "type": field_type,
                    "name": FIELD_NAMES.get(field_type, f"field_{field_type}"),
                    "length": field_length,
                    "raw_hex": " ".join(
                        f"{byte:02X}" for byte in fields_data[start:]
                    ),
                    "malformed": True,
                }
            )
            break

        value_bytes = fields_data[start:end]
        field_name = FIELD_NAMES.get(field_type, f"field_{field_type}")

        field: dict[str, Any] = {
            "type": field_type,
            "name": field_name,
            "length": field_length,
            "raw_hex": " ".join(f"{byte:02X}" for byte in value_bytes),
        }

        if field_type == FIELD_DATETIME:
            value = _parse_datetime(value_bytes)
            field["value"] = value
            if value is not None:
                parsed["timestamp"] = value
        elif field_length == 2:
            value = _uint16(value_bytes)
            field["value"] = value
            parsed[field_name] = value
        else:
            # Неизвестное поле сохраняем lossless.
            field["value"] = value_bytes

        parsed["fields"].append(field)
        offset = end

    return parsed
