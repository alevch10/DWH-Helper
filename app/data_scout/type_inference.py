import uuid
import ipaddress
import re
import pycountry
import pendulum

from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from pydantic import TypeAdapter
from pydantic_extra_types.color import Color, COLORS_BY_NAME
from pydantic_extra_types.ulid import ULID
from pydantic_extra_types.dsn import (
    PostgresDsn,
    RedisDsn,
    MongoDsn,
    ClickHouseDsn,
    KafkaDsn,
    AmqpDsn,
    MariaDBDsn,
    NatsDsn,
    SnowflakeDsn,
    CockroachDsn,
)
from pydantic_extra_types.phone_numbers import PhoneNumber
from pydantic_extra_types.mac_address import MacAddress
from pydantic_extra_types.mime_types import MimeType
from pydantic_extra_types.cron import CronStr
from pydantic_extra_types.script_code import ISO_15924
from pydantic_extra_types.country import CountryAlpha2, CountryAlpha3
from pydantic_extra_types.currency_code import Currency
from pydantic_extra_types.iban import IBAN
from pydantic_extra_types.isbn import ISBN
from pydantic_extra_types.routing_number import ABARoutingNumber
from pydantic_extra_types.payment import PaymentCardNumber
from pydantic_extra_types.semantic_version import SemanticVersion
from pydantic_extra_types.timezone_name import TimeZoneName
from pydantic_extra_types.epoch import Integer as EpochInteger, Number as EpochNumber
from pydantic_extra_types.pendulum_dt import DateTime as PendulumDateTime
from pydantic_extra_types.coordinate import Coordinate
from pydantic_extra_types.domain import DomainStr

# ----------------------------------------------------------------------
# Регулярные выражения для предварительной валидации подходящих типов
# ----------------------------------------------------------------------
UUID_REGEX = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)
ULID_REGEX = re.compile(r"^[0-9A-Z]{26}$")
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
URL_REGEX = re.compile(r"^https?://", re.IGNORECASE)
DOMAIN_REGEX = re.compile(
    r"^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$"
)
MAC_REGEX = re.compile(r"^([0-9A-Fa-f]{2}[:.-]){5}[0-9A-Fa-f]{2}$")
IPV4_REGEX = re.compile(
    r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}"
    r"(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
)
S3PATH_REGEX = re.compile(r"^s3://[^/]+/.+$")
COORDINATE_REGEX = re.compile(r"^-?\d+(\.\d+)?,-?\d+(\.\d+)?$")
SEMVER_PREFIX_REGEX = re.compile(r"^\d+\.\d+\.\d+")
COLOR_NAMES = set(COLORS_BY_NAME.keys())

# ----------------------------------------------------------------------
# Множества допустимых кодов (вычисляются один раз)
# ----------------------------------------------------------------------
LANGUAGE_ALPHA2: set = {
    lang.alpha_2.lower()
    for lang in pycountry.languages
    if hasattr(lang, "alpha_2") and lang.alpha_2
}
LANGUAGE_ALPHA3: set = {
    lang.alpha_3.lower()
    for lang in pycountry.languages
    if hasattr(lang, "alpha_3") and lang.alpha_3
}

COUNTRY_ALPHA2: set = {c.alpha_2.upper() for c in pycountry.countries}
COUNTRY_ALPHA3: set = {c.alpha_3.upper() for c in pycountry.countries}

# ВАЖНО: в pycountry для валют трёхбуквенный код хранится в атрибуте alpha_3
CURRENCY_SET: set = {
    c.alpha_3.upper()
    for c in pycountry.currencies
    if hasattr(c, "alpha_3") and c.alpha_3
}


# ----------------------------------------------------------------------
# Вспомогательные функции
# ----------------------------------------------------------------------
def _is_valid_isbn10(s: str) -> bool:
    """Проверка контрольной суммы ISBN-10."""
    if len(s) != 10:
        return False
    total = 0
    for i, ch in enumerate(s):
        if ch == "X" and i == 9:
            digit = 10
        elif ch.isdigit():
            digit = int(ch)
        else:
            return False
        total += digit * (10 - i)
    return total % 11 == 0


def _is_valid_isbn13(s: str) -> bool:
    """Проверка контрольной суммы ISBN-13."""
    if len(s) != 13 or not s.isdigit():
        return False
    total = 0
    for i, ch in enumerate(s):
        digit = int(ch)
        weight = 1 if i % 2 == 0 else 3
        total += digit * weight
    return total % 10 == 0


def looks_like_datetime_iso(s: str) -> bool:
    return len(s) >= 20 and "T" in s and (s.endswith("Z") or "+" in s)


def looks_like_datetime_custom(s: str) -> bool:
    return len(s) >= 19 and s[4] == "-" and s[7] == "-" and s[10] == " "


def looks_like_pendulum(s: str) -> bool:
    return "T" in s and not looks_like_datetime_iso(s)


def looks_like_color(s: str) -> bool:
    return (
        s.startswith("#")
        or s.startswith("rgb")
        or s.startswith("hsl")
        or s in COLOR_NAMES
    )


# ----------------------------------------------------------------------
# Основная функция
# ----------------------------------------------------------------------
def infer_type(value: Any) -> Tuple[str, Optional[Dict]]:
    if value is None:
        return "null", None
    if isinstance(value, bool):
        return "boolean", None
    if isinstance(value, int):
        return "integer", None
    if isinstance(value, float):
        return "float", None
    if isinstance(value, dict):
        return "object", None
    if isinstance(value, list):
        return "array", None
    if not isinstance(value, str):
        return "string", None

    s = value
    length = len(s)

    # ----- ULID -----
    if length == 26 and ULID_REGEX.match(s):
        try:
            TypeAdapter(ULID).validate_python(s)
            return "ulid", None
        except:
            pass

    # ----- UUID -----
    if length == 36 and UUID_REGEX.match(s):
        try:
            u = uuid.UUID(s)
            if u.version == 4:
                return "uuid4", None
            elif u.version == 1:
                return "uuid1", None
            else:
                return "uuid", None
        except ValueError:
            pass

    # ----- IP / CIDR -----
    if IPV4_REGEX.match(s):
        try:
            ipaddress.IPv4Address(s)
            return "ipv4", None
        except:
            pass
    if ":" in s and not any(c not in "0123456789abcdefABCDEF:" for c in s):
        try:
            ipaddress.IPv6Address(s)
            return "ipv6", None
        except:
            pass
    if "/" in s:
        try:
            net = ipaddress.ip_network(s, strict=False)
            return ("cidr_v4", None) if net.version == 4 else ("cidr_v6", None)
        except:
            pass

    # ----- Email -----
    if EMAIL_REGEX.match(s):
        return "email", None

    # ----- URL -----
    if URL_REGEX.match(s):
        return "url", None

    # ----- Domain -----
    if DOMAIN_REGEX.match(s):
        try:
            DomainStr(s)
            return "domain", None
        except:
            pass

    # ----- MAC -----
    if MAC_REGEX.match(s):
        try:
            MacAddress(s)
            return "mac_address", None
        except:
            pass

    # ----- S3 Path -----
    if S3PATH_REGEX.match(s):
        return "s3_path", None

    # ----- DSN -----
    if "://" in s:
        for dsn_type, name in [
            (PostgresDsn, "dsn_postgres"),
            (RedisDsn, "dsn_redis"),
            (MongoDsn, "dsn_mongo"),
            (ClickHouseDsn, "dsn_clickhouse"),
            (KafkaDsn, "dsn_kafka"),
            (AmqpDsn, "dsn_amqp"),
            (MariaDBDsn, "dsn_mariadb"),
            (NatsDsn, "dsn_nats"),
            (SnowflakeDsn, "dsn_snowflake"),
            (CockroachDsn, "dsn_cockroach"),
        ]:
            try:
                TypeAdapter(dsn_type).validate_python(s)
                return name, None
            except:
                pass
        return "string", None

    # ----- PhoneNumber -----
    if length >= 5 and length <= 20 and s[0] == "+":
        try:
            PhoneNumber(s)
            return "phone", None
        except:
            pass

    # ----- TimeZoneName -----
    if 3 <= length <= 50 and "/" in s:
        try:
            TypeAdapter(TimeZoneName).validate_python(s)
            return "timezone_name", None
        except:
            pass

    # ----- MIME Type -----
    if length <= 127 and "/" in s:
        try:
            MimeType(s)
            return "mime_type", None
        except:
            pass

    # ----- Cron -----
    if length <= 100 and s.count(" ") == 4:
        try:
            CronStr(s)
            return "cron", None
        except:
            pass

    # ----- Color -----
    if 3 <= length <= 24 and looks_like_color(s):
        try:
            Color(s)
            return "color", None
        except:
            pass

    # ----- ISO 15924 -----
    if length == 4 and s.isalpha():
        try:
            pycountry.scripts.lookup(s.upper())
            ISO_15924(s.upper())
            return "script_code", None
        except:
            pass

    # ----- Двухбуквенные: язык → страна -----
    if length == 2:
        low = s.lower()
        if low in LANGUAGE_ALPHA2:
            return "language_alpha2", None
        if s.upper() in COUNTRY_ALPHA2:
            return "country_alpha2", None

    # ----- Трёхбуквенные: страна → валюта → язык -----
    if length == 3 and s.isalpha():
        upper = s.upper()
        lower = s.lower()
        if upper in COUNTRY_ALPHA3:
            return "country_alpha3", None
        if upper in CURRENCY_SET:
            return "currency", None
        if lower in LANGUAGE_ALPHA3:
            return "language_alpha3", None

    # ----- Pendulum DateTime -----
    if looks_like_pendulum(s):
        try:
            pendulum.parse(s)
            return "pendulum_datetime", None
        except:
            pass

    # ----- Datetime ISO -----
    if looks_like_datetime_iso(s):
        try:
            datetime.fromisoformat(s.replace("Z", "+00:00"))
            return "datetime_iso", {"format": "iso"}
        except:
            pass

    # ----- Datetime custom -----
    if looks_like_datetime_custom(s):
        if "." in s:
            try:
                datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
                return "datetime_custom", {"format": "custom"}
            except:
                pass
        try:
            datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            return "datetime_custom", {"format": "custom"}
        except:
            pass

    # ----- ISBN -----
    if length == 10 and (s[:-1].isdigit() and (s[-1].isdigit() or s[-1] == "X")):
        if _is_valid_isbn10(s):
            return "isbn", None
    if length == 13 and s.isdigit() and _is_valid_isbn13(s):
        return "isbn", None

    # ----- IBAN -----
    if length >= 15 and s[:2].isalpha() and not any(c in s for c in " \t\n\r"):
        try:
            IBAN(s)
            return "iban", None
        except:
            pass

    # ----- ABA Routing -----
    if length == 9 and s.isdigit():
        try:
            ABARoutingNumber(s)
            return "aba_routing", None
        except:
            pass

    # ----- Payment Card -----
    if 12 <= length <= 19 and s.isdigit():
        try:
            PaymentCardNumber(s)
            return "payment_card", None
        except:
            pass

    # ----- Epoch int -----
    if length <= 19 and s.isdigit():
        try:
            val = int(s)
            if 0 <= val <= 2_534_023_007_99:
                TypeAdapter(EpochInteger).validate_python(s)
                return "epoch_int", None
        except:
            pass

    # ----- Epoch float -----
    if "." in s and s.replace(".", "").isdigit():
        try:
            TypeAdapter(EpochNumber).validate_python(s)
            return "epoch_float", None
        except:
            pass

    # ----- Mongo ObjectId -----
    if length == 24 and s.isalnum():
        return "mongo_object_id", None

    # ----- Semantic Version -----
    if 3 <= length <= 50 and "." in s and SEMVER_PREFIX_REGEX.match(s):
        try:
            TypeAdapter(SemanticVersion).validate_python(s)
            return "semantic_version", None
        except:
            return "semantic_version", None

    # ----- Coordinate -----
    if COORDINATE_REGEX.match(s):
        try:
            TypeAdapter(Coordinate).validate_python(s)
            return "coordinate", None
        except:
            pass

    return "string", None
