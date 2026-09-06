"""Constants for the Bolid Events integration."""

DOMAIN = "bolid_events"
VERSION = "1.0.2"
EVENT_BOLID = "bolid_event"

CONF_HUB = "hub"
CONF_SLAVE = "slave"
CONF_POLL_INTERVAL = "poll_interval"
CONF_RETRY_DELAY = "retry_delay"
CONF_READ_RETRIES = "read_retries"
CONF_MAX_BATCH = "max_batch"
CONF_STATUS_INTERVAL = "status_interval"
CONF_EVENT_LOG_FILE = "event_log_file"
CONF_DIAGNOSTIC_LOG_FILE = "diagnostic_log_file"
CONF_REPLAY_ON_START = "replay_on_start"

DEFAULT_HUB = "bolid"
DEFAULT_SLAVE = 1
DEFAULT_POLL_INTERVAL = 1.0
DEFAULT_RETRY_DELAY = 0.50
DEFAULT_READ_RETRIES = 3
DEFAULT_MAX_BATCH = 32
DEFAULT_STATUS_INTERVAL = 30.0
DEFAULT_EVENT_LOG_FILE = "bolid_events.jsonl"
DEFAULT_DIAGNOSTIC_LOG_FILE = "bolid_events_diagnostic.jsonl"
DEFAULT_REPLAY_ON_START = False

# Health layer: FIFO должен успешно продвигаться хотя бы раз за этот интервал.
HEALTH_TIMEOUT_SECONDS = 30.0
HEALTH_REFRESH_SECONDS = 5.0

# Массовая недоступность raw-зон — отдельный признак деградации данных.
# Единичные transient unavailable не считаем аварией.
ZONE_UNAVAILABLE_THRESHOLD = 5
ZONE_UNAVAILABLE_HOLD_SECONDS = 60.0
EVENT_NUMBER_MODULUS = 65536
LOAD_MONITOR_INTERVAL = 30.0
LOAD_SUMMARY_INTERVAL = 300.0

# С2000-ПП: функции работы с кольцевым буфером событий.
REG_NEWEST_EVENT = 46160
REG_OLDEST_EVENT = 46161
REG_UNREAD_EVENTS = 46162
REG_EVENT_READ_ACK = 46163
REG_OLDEST_UNREAD_EVENT = 46264

EVENT_REG_COUNT = 14
EVENT_BYTE_COUNT = 28

# Типы дополнительных полей события.
FIELD_USER = 1
FIELD_SECTION = 2
FIELD_ZONE = 3
FIELD_RELAY = 5
FIELD_RELAY_STATE = 7
FIELD_DATETIME = 11
FIELD_SECTION_ID = 24

FIELD_NAMES = {
    FIELD_USER: "user",
    FIELD_SECTION: "section",
    FIELD_ZONE: "zone",
    FIELD_RELAY: "relay",
    FIELD_RELAY_STATE: "relay_state",
    FIELD_DATETIME: "timestamp",
    FIELD_SECTION_ID: "section_id",
}

# Пока это намеренно неполная таблица событий.
# Неизвестный код никогда не отбрасывается: остаются event_code и raw_hex.
EVENT_NAMES = {
    1: "Восстановление сети 220 В",
    2: "Авария сети 220 В",
    3: "Тревога проникновения",
    17: "Неудачное взятие",
    23: "Задержка взятия",
    24: "Взятие входа на охрану",
    34: "Идентификация",
    35: "Восстановление технологического входа",
    36: "Нарушение технологического входа",
    37: "Пожар",
    40: "Пожар 2",
    41: "Неисправность оборудования",
    44: "Внимание",
    45: "Обрыв входа",
    46: "Обрыв ДПЛС",
    47: "Восстановление ДПЛС",
    58: "Тихая тревога",
    76: "Повышение температуры",
    78: "Температура в норме",
    79: "Тревога затопления",
    80: "Восстановление датчика затопления",
    82: "Неисправность термометра",
    83: "Восстановление термометра",
    84: "Начало локального программирования",
    109: "Снятие входа с охраны",
    110: "Сброс тревоги",
    111: "Включение ШС",
    112: "Отключение ШС",
    117: "Восстановление снятого входа",
    118: "Тревога входа",
    119: "Нарушение снятого входа",
    121: "Обрыв выхода",
    122: "КЗ выхода",
    123: "Восстановление выхода",
    126: "Потеря связи с выходом",
    127: "Восстановление связи с выходом",
    128: "Изменение состояния выхода",
    149: "Взлом корпуса прибора",
    152: "Восстановление корпуса прибора",
    165: "Ошибка параметров входа",
    187: "Потеря связи со входом",
    188: "Восстановление связи со входом",
    203: "Перезапуск прибора",
    241: "Раздел взят",
    242: "Раздел снят",
    243: "Удаленный запрос на взятие",
    244: "Удаленный запрос на снятие",
    250: "Потеряна связь с прибором",
    251: "Восстановлена связь с прибором",
    253: "Включение пульта С2000М",
    254: "Прошел день — отметка времени",
}
