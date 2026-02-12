import re
from uuid import UUID
from datetime import datetime
from typing import Optional, Dict, Any, Literal, Tuple, List

from app.db.schemas import (
    PermanentUserProperties,
    ChangeableUserProperties,
)
from app.etl import MAPPINGS
from app.config.logger import get_logger

logger = get_logger(__name__)

SourceType = Literal["amplitude", "tmp_table"]


def safe_dict(value):
    """Возвращает словарь, если value — dict, иначе пустой dict."""
    return value if isinstance(value, dict) else {}


def transform_single_record(
    raw_record: Dict[str, Any], source_type: SourceType, mappings: Dict = MAPPINGS
) -> Tuple[
    Optional[PermanentUserProperties], Optional[ChangeableUserProperties], List[Dict]
]:
    errors = []
    permanent_data = {}
    changeable_data = {}

    # --- UUID ---
    uuid_raw = raw_record.get("uuid")
    if isinstance(uuid_raw, str):
        try:
            uuid = UUID(uuid_raw)
        except ValueError:
            errors.append(
                {"key": "uuid", "value": uuid_raw, "reason": "Invalid UUID format"}
            )
            return None, None, errors
    elif isinstance(uuid_raw, UUID):
        uuid = uuid_raw
    else:
        errors.append(
            {"key": "uuid", "value": uuid_raw, "reason": "Expected str or UUID"}
        )
        return None, None, errors

    # --- event_time ---
    event_time_raw = raw_record.get("event_time")
    if event_time_raw is None:
        errors.append(
            {"key": "event_time", "value": None, "reason": "Missing event_time"}
        )
        return None, None, errors
    if isinstance(event_time_raw, str):
        try:
            event_time = datetime.fromisoformat(event_time_raw.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            errors.append(
                {
                    "key": "event_time",
                    "value": event_time_raw,
                    "reason": "Invalid ISO datetime",
                }
            )
            return None, None, errors
    elif isinstance(event_time_raw, datetime):
        event_time = event_time_raw
    else:
        errors.append(
            {
                "key": "event_time",
                "value": str(event_time_raw),
                "reason": "Unsupported type",
            }
        )
        return None, None, errors

    language = raw_record.get("language")
    session_id = raw_record.get("session_id")
    start_version = raw_record.get("start_version")

    # --- user_properties ---
    if source_type == "amplitude":
        user_props = safe_dict(raw_record.get("user_properties"))
    elif source_type == "tmp_table":
        user_props = safe_dict(raw_record.get("user_properties_json"))
    else:
        raise ValueError(f"Unknown source_type: {source_type}")

    # --- Сбор известных ключей ---
    known_keys = set()
    for section in ["permanent", "changeable"]:
        for field in mappings.get(section, []):
            known_keys.update(field["sources"])
    known_keys.add("EHR_ID")

    # --- Проверка неизвестных ключей ---
    unknown_keys = set(user_props.keys()) - known_keys
    if unknown_keys:
        for key in unknown_keys:
            errors.append(
                {"key": key, "value": user_props[key], "reason": "Unknown key"}
            )
        logger.error("Unknown keys found: %s", unknown_keys)
        return None, None, errors

    # --- EHR_ID ---
    ehr_id_raw = user_props.get("EHR_ID")
    if ehr_id_raw in [None, "N/A", "no ehr", "no_ehr"]:
        ehr_id = None
    else:
        try:
            ehr_id = int(ehr_id_raw)
        except (ValueError, TypeError):
            ehr_id = None
            errors.append(
                {"key": "EHR_ID", "value": ehr_id_raw, "reason": "Invalid integer"}
            )
            # не прерываем, но ehr_id останется None

    # --- Вспомогательная функция извлечения ---
    def extract_value(field: Dict) -> Any:
        value = None
        for source in field["sources"]:
            raw_value = (
                user_props.get(source)
                if source in known_keys
                else raw_record.get(source)
            )
            if raw_value is not None and raw_value != "N/A":
                value = raw_value
                break

        if value is None:
            return None

        field_type = field["type"]
        if field_type == "string":
            if "transform" in field and field["transform"] == "lowercase_first":
                value = value.lower()
            if "value_map" in field:
                value = field["value_map"].get(value, value)
        elif field_type == "integer":
            if "extract_regex" in field:
                match = re.search(field["extract_regex"], str(value))
                if match:
                    value = match.group(0)
            try:
                value = int(value)
            except (ValueError, TypeError):
                errors.append(
                    {
                        "key": field["target"],
                        "value": value,
                        "reason": "Invalid integer",
                    }
                )
                value = None
        elif field_type == "boolean":
            true_vals = field.get("true_values", [])
            false_vals = field.get("false_values", [])
            null_vals = field.get("null_values", [])
            if value in true_vals:
                value = True
            elif value in false_vals:
                value = False
            elif value in null_vals:
                value = None
            else:
                errors.append(
                    {
                        "key": field["target"],
                        "value": value,
                        "reason": "Invalid boolean",
                    }
                )
                value = None

        return value

    # --- Заполнение permanent ---
    for field in mappings.get("permanent", []):
        permanent_data[field["target"]] = extract_value(field)

    # --- Заполнение changeable ---
    for field in mappings.get("changeable", []):
        changeable_data[field["target"]] = extract_value(field)

    # --- Сборка permanent модели ---
    permanent = None
    if ehr_id is not None:
        permanent_data["ehr_id"] = ehr_id
        permanent_data["first_login_at"] = event_time
        try:
            permanent = PermanentUserProperties(**permanent_data)
        except Exception as e:
            errors.append({"key": "permanent", "value": None, "reason": str(e)})
            permanent = None

    # --- Сборка changeable модели ---
    changeable_data.update(
        {
            "ehr_id": ehr_id,
            "uuid": uuid,
            "event_time": event_time,
            "language": language,
            "session_id": session_id,
            "start_version": start_version,
        }
    )
    try:
        changeable = ChangeableUserProperties(**changeable_data)
    except Exception as e:
        errors.append({"key": "changeable", "value": None, "reason": str(e)})
        changeable = None

    return permanent, changeable, errors
