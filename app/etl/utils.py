import re
import ipaddress
import json
import csv
from datetime import datetime
from typing import Any, Optional, List, Dict, Iterable
from uuid import UUID


def natural_sort_key(s: str) -> List[str]:
    """Ключ для естественной сортировки (file_2 < file_10)."""
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r"(\d+)", s)]


def extract_value_by_path(obj: Dict, path: str) -> Any:
    """Извлекает значение по точечному пути (user_properties.EHR_ID)."""
    parts = path.split(".")
    current = obj
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
            if current is None:
                return None
        else:
            return None
    return current


def apply_value_map(value: Any, mapping: Dict[str, Any], null_values: List[str]) -> Any:
    """Преобразует значение согласно value_map и null_values."""
    if value is None:
        return None
    str_val = str(value)
    for nv in null_values:
        if nv.endswith("*"):
            # маска-префикс: всё, что начинается с nv[:-1], считается null
            if str_val.startswith(nv[:-1]):
                return None
        else:
            if str_val == nv:
                return None
    if mapping and str_val in mapping:
        return mapping[str_val]
    return value


def convert_type(
    value: Any, target_type: str, field_format: Optional[str] = None
) -> Any:
    if value is None:
        return None
    try:
        if target_type == "string":
            return str(value)
        elif target_type == "integer":
            return int(value)
        elif target_type == "float":
            return float(value)
        elif target_type == "boolean":
            return bool(value)
        elif target_type == "datetime":
            if isinstance(value, datetime):
                return value
            if field_format == "iso":
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            # Пытаемся распарсить как ISO-8601 с 'T'
            value_str = str(value).strip()
            if "T" in value_str:
                return datetime.fromisoformat(value_str.replace("Z", "+00:00"))
            # Пробуем с микросекундами (старый стандартный формат)
            try:
                return datetime.strptime(value_str, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                pass
            # Основной формат AppMetrica и других: без микросекунд
            return datetime.strptime(value_str, "%Y-%m-%d %H:%M:%S")

        elif target_type == "json":
            # Преобразуем dict или list в JSON-строку
            if isinstance(value, (dict, list)):
                return json.dumps(value, ensure_ascii=False)
            elif isinstance(value, str):
                return value
            else:
                return str(value)
        elif target_type == "array":
            if not isinstance(value, list):
                return [value]
            return value
        elif target_type == "inet":
            ip = ipaddress.ip_address(value)
            return str(ip) + "/32" if ip.version == 4 else str(ip)
            cleaned = str(value).strip()
            if cleaned.startswith("[") and cleaned.endswith("]"):
                cleaned = cleaned[1:-1]
            ip = ipaddress.ip_address(cleaned)
            return str(ip)
        elif target_type == "uuid":
            return str(UUID(value))
    except Exception:
        raise ValueError(f"Cannot convert value '{value}' to {target_type}")
    return value


def find_unknown_keys(
    obj: Any,
    known_paths: set,
    jsonb_source_prefixes: set = None,
    current_path: str = "",
) -> list:
    """
    Рекурсивно ищет ключи, которые не являются префиксами ни одного пути в known_paths.
    Параметр jsonb_source_prefixes содержит множества source-путей для полей типа "json".
    Для путей, которые лежат внутри любого из этих префиксов, проверка не выполняется.
    """
    unknown = []
    if jsonb_source_prefixes is None:
        jsonb_source_prefixes = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            full_path = f"{current_path}.{k}" if current_path else k
            # Если текущий путь находится внутри JSONB-поля, пропускаем всё поддерево
            inside_jsonb = any(
                full_path == prefix or full_path.startswith(prefix + ".")
                for prefix in jsonb_source_prefixes
            )
            if inside_jsonb:
                continue
            # Проверяем, является ли full_path известным (равен или префикс известного пути)
            is_known = any(
                known == full_path or known.startswith(full_path + ".")
                for known in known_paths
            )
            if not is_known:
                unknown.append(full_path)
            else:
                # Если это известный контейнер (например, user_properties), рекурсивно обходим его
                if isinstance(v, dict):
                    unknown.extend(
                        find_unknown_keys(
                            v, known_paths, jsonb_source_prefixes, full_path
                        )
                    )
    return unknown


def parse_delimited_stream(
    lines: Iterable[str], headers: List[str], delimiter: str = "\t"
) -> List[Dict[str, str]]:
    """
    Разбирает строки с разделителями в список словарей.
    Пустые строки пропускаются.
    """
    result = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        reader = csv.reader([line], delimiter=delimiter)
        try:
            values = next(reader)
        except Exception:
            continue
        # Дополняем недостающие значения пустыми строками
        if len(values) < len(headers):
            values.extend([""] * (len(headers) - len(values)))
        row = dict(zip(headers, values))
        result.append(row)
    return result
