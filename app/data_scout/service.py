"""Business logic for data analysis from S3 NDJSON files."""

import io
import json
import zipfile
from typing import Any, Dict, Optional, Set, Tuple

from app.config.logger import get_logger
from app.config.settings import settings
from app.s3.client import S3Client

logger = get_logger(__name__)


class FieldStats:
    """Statistics for a single field."""

    def __init__(self):
        self.type: Optional[str] = None
        self.has_null = False
        self.min: Optional[float] = None
        self.max: Optional[float] = None
        self.min_length: Optional[int] = None
        self.max_length: Optional[int] = None
        self.items_type: Optional[str] = None
        self.unique_samples: Set[Any] = set()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values and empty sets."""
        result = {"type": self.type, "has_null": self.has_null}

        if self.min is not None:
            result["min"] = self.min
        if self.max is not None:
            result["max"] = self.max
        if self.min_length is not None:
            result["min_length"] = self.min_length
        if self.max_length is not None:
            result["max_length"] = self.max_length
        if self.items_type is not None:
            result["items_type"] = self.items_type

        if self.unique_samples:
            result["unique_samples"] = sorted(
                list(self.unique_samples),
                key=lambda x: (type(x).__name__, str(x)),
            )

        return result


def get_json_type(value: Any) -> Optional[str]:
    """Determine the JSON type of a value."""
    if value is None:
        return "null"
    elif isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "integer"
    elif isinstance(value, float):
        return "float"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, list):
        return "array"
    elif isinstance(value, dict):
        return "object"
    else:
        return "undefined"


def is_json_like(value: str) -> bool:
    """Check if a string looks like JSON (dict or list)."""
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return (
        stripped.startswith(("{", "[")) and stripped.endswith(("}", "]"))
    )


def try_parse_json(value: str) -> Tuple[bool, Optional[Any]]:
    """Try to parse a string as JSON."""
    try:
        return True, json.loads(value)
    except (json.JSONDecodeError, TypeError, ValueError):
        return False, None


def update_field_stats(
    stats: FieldStats, value: Any, is_array_length: bool = False, skip_type_update: bool = False
) -> None:
    """Update statistics for a field with a new value."""
    value_type = get_json_type(value)

    if value is None:
        stats.has_null = True
        if stats.type is None:
            stats.type = "null"
        return

    if not skip_type_update:
        if stats.type is None:
            stats.type = value_type
        elif stats.type != value_type and value_type != "null":
            stats.type = "undefined"

    if isinstance(value, (int, float)):
        if stats.min is None or value < stats.min:
            stats.min = value
        if stats.max is None or value > stats.max:
            stats.max = value

    if isinstance(value, str):
        length = len(value)
        if stats.min_length is None or length < stats.min_length:
            stats.min_length = length
        if stats.max_length is None or length > stats.max_length:
            stats.max_length = length

    if not is_array_length and len(stats.unique_samples) < 100:
        try:
            stats.unique_samples.add(value if not isinstance(value, list) else tuple(value))
        except TypeError:
            pass


def traverse(
    data: Any, current_path: Tuple[str, ...], stats: Dict[str, FieldStats]
) -> None:
    """Recursively traverse data structure and collect statistics."""
    path_str = ".".join(current_path) if current_path else ""

    if isinstance(data, dict):
        for key, value in data.items():
            new_path = current_path + (key,)
            new_path_str = ".".join(new_path)

            if new_path_str not in stats:
                stats[new_path_str] = FieldStats()

            if isinstance(value, dict):
                if stats[new_path_str].type is None:
                    stats[new_path_str].type = "object"
                traverse(value, new_path, stats)
            elif isinstance(value, list):
                traverse(value, new_path, stats)
            elif isinstance(value, str) and is_json_like(value):
                success, parsed = try_parse_json(value)
                if success:
                    stats[new_path_str].type = "json"
                    update_field_stats(
                        stats[new_path_str], value, skip_type_update=True
                    )
                    traverse_json_content(parsed, new_path, stats)
                else:
                    stats[new_path_str].type = "broken_json"
                    update_field_stats(stats[new_path_str], value, skip_type_update=True)
            else:
                update_field_stats(stats[new_path_str], value)

    elif isinstance(data, list):
        if path_str not in stats:
            stats[path_str] = FieldStats()

        array_stats = stats[path_str]
        array_stats.type = "array"
        
        if len(data) > 0:
            length = len(data)
            if array_stats.min_length is None or length < array_stats.min_length:
                array_stats.min_length = length
            if array_stats.max_length is None or length > array_stats.max_length:
                array_stats.max_length = length

        element_types = set()
        for item in data:
            if isinstance(item, dict):
                traverse(item, current_path, stats)
            elif isinstance(item, list):
                traverse(item, current_path, stats)
            elif item is not None:
                element_types.add(get_json_type(item))
            elif item is None:
                array_stats.has_null = True

        if element_types and len(element_types) == 1:
            array_stats.items_type = element_types.pop()
        elif element_types and len(element_types) > 1:
            array_stats.items_type = "undefined"

    elif data is not None:
        if path_str not in stats:
            stats[path_str] = FieldStats()
        update_field_stats(stats[path_str], data)


def traverse_json_content(
    data: Any, current_path: Tuple[str, ...], stats: Dict[str, FieldStats]
) -> None:
    """Traverse content of a JSON string (virtual children)."""
    path_str = ".".join(current_path) if current_path else ""

    if isinstance(data, dict):
        for key, value in data.items():
            new_path = current_path + (key,)
            new_path_str = ".".join(new_path)

            if new_path_str not in stats:
                stats[new_path_str] = FieldStats()

            if isinstance(value, dict):
                stats[new_path_str].type = "object"
                traverse_json_content(value, new_path, stats)
            elif isinstance(value, list):
                traverse_json_content(value, new_path, stats)
            else:
                update_field_stats(stats[new_path_str], value)

    elif isinstance(data, list):
        if path_str not in stats:
            stats[path_str] = FieldStats()

        array_stats = stats[path_str]
        array_stats.type = "array"
        
        if len(data) > 0:
            length = len(data)
            if array_stats.min_length is None or length < array_stats.min_length:
                array_stats.min_length = length
            if array_stats.max_length is None or length > array_stats.max_length:
                array_stats.max_length = length

        element_types = set()
        for item in data:
            if isinstance(item, dict):
                traverse_json_content(item, current_path, stats)
            elif isinstance(item, list):
                traverse_json_content(item, current_path, stats)
            elif item is not None:
                element_types.add(get_json_type(item))
            elif item is None:
                array_stats.has_null = True

        if element_types and len(element_types) == 1:
            array_stats.items_type = element_types.pop()
        elif element_types and len(element_types) > 1:
            array_stats.items_type = "undefined"


def build_yaml_schema(stats: Dict[str, FieldStats]) -> Dict[str, Any]:
    """Build nested YAML schema from flat stats dictionary."""
    result = {}
    
    paths_by_depth = sorted(stats.keys(), key=lambda x: x.count("."))
    
    for path_str in paths_by_depth:
        if not path_str:
            continue
        
        parts = path_str.split(".")
        current = result
        
        for i, part in enumerate(parts):
            if i == len(parts) - 1:
                field_dict = stats[path_str].to_dict()
                
                if field_dict.get("type") in ("object", "array"):
                    if "children" not in field_dict:
                        field_dict["children"] = {}
                    
                    prefix = ".".join(parts)
                    for other_path in stats.keys():
                        if other_path.startswith(f"{prefix}."):
                            remaining = other_path[len(prefix) + 1:]
                            if "." not in remaining:
                                child_name = remaining
                                if child_name not in field_dict["children"]:
                                    field_dict["children"][child_name] = (
                                        stats[other_path].to_dict()
                                    )
                
                current[part] = field_dict
            else:
                if part not in current:
                    current[part] = {
                        "type": "object",
                        "has_null": False,
                        "children": {},
                    }
                elif "children" not in current[part]:
                    current[part]["children"] = {}
                
                current = current[part]["children"]
    
    return result


def format_schema_for_output(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Format schema for YAML output, excluding json unique_samples."""
    result = {}

    for key, value in schema.items():
        if isinstance(value, dict):
            formatted = {
                "type": value.get("type", "object"),
                "has_null": value.get("has_null", False),
            }

            if "min" in value:
                formatted["min"] = value["min"]
            if "max" in value:
                formatted["max"] = value["max"]
            if "min_length" in value:
                formatted["min_length"] = value["min_length"]
            if "max_length" in value:
                formatted["max_length"] = value["max_length"]
            if "items_type" in value:
                formatted["items_type"] = value["items_type"]

            if value.get("type") != "json" and "unique_samples" in value:
                formatted["unique_samples"] = value["unique_samples"]

            if "children" in value and value["children"]:
                formatted["children"] = format_schema_for_output(
                    value["children"]
                )

            result[key] = formatted

    return result


def analyze_data(path: str) -> Dict[str, Any]:
    """Analyze S3 data at given path and return schema profile."""
    logger.info(f"Starting analysis for path: {path}")

    s3 = S3Client()
    bucket = settings.s3.bucket_name

    objects = s3.list_objects(prefix=path)
    zip_files = [obj for obj in objects if obj["Key"].endswith(".zip")]

    if not zip_files:
        logger.warning(f"No zip files found at path: {path}")
        raise ValueError(
            f"No zip files found at path: {path}"
        )

    stats: Dict[str, FieldStats] = {}

    for zip_obj in zip_files:
        try:
            logger.info(f"Processing ZIP file: {zip_obj['Key']}")
            zip_bytes = s3.get_object(zip_obj["Key"])

            try:
                with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_file:
                    for file_info in zip_file.filelist:
                        if file_info.filename.endswith(".ndjson"):
                            logger.info(f"Processing NDJSON: {file_info.filename}")
                            ndjson_content = zip_file.read(file_info.filename)

                            for line_num, line in enumerate(
                                ndjson_content.decode("utf-8").splitlines(), 1
                            ):
                                if not line.strip():
                                    continue

                                try:
                                    obj = json.loads(line)
                                    traverse(obj, (), stats)
                                except json.JSONDecodeError as e:
                                    logger.warning(
                                        f"Failed to parse JSON in {file_info.filename}:{line_num}: {e}"
                                    )
                            logger.info(f"Finished processing {file_info.filename}")

            except zipfile.BadZipFile as e:
                logger.warning(f"Bad ZIP file {zip_obj['Key']}: {e}")
            except Exception as e:
                logger.warning(f"Error processing ZIP file {zip_obj['Key']}: {e}")

        except Exception as e:
            logger.warning(f"Failed to download {zip_obj['Key']}: {e}")

    logger.info(f"Analysis complete. Found {len(stats)} unique paths")
    schema = build_yaml_schema(stats)
    formatted_schema = format_schema_for_output(schema)

    return {"path": path, "schema": formatted_schema}
