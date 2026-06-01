import io
import json
import zipfile
from typing import Dict, Any, List, Optional, Set, Tuple

from app.config.logger import get_logger
from app.s3.client import S3Client
from .type_inference import infer_type

logger = get_logger(__name__)


class FieldStats:
    def __init__(self, sample_limit: int = 100):
        self.has_null = False
        self.possible_types = {}  # {type_name: {"count": int, "metadata": dict}}
        self.samples_by_type = {}  # {type_name: set()}
        self.min = None
        self.max = None
        self.min_length = None
        self.max_length = None
        self.items_type = None
        self.children = {}
        self.sample_limit = sample_limit
        self.items_stats = None

    def update(self, value: Any):
        if value is None:
            self.has_null = True
            return

        type_name, metadata = infer_type(value)
        if type_name not in self.possible_types:
            self.possible_types[type_name] = {"count": 0, "metadata": metadata or {}}
            self.samples_by_type[type_name] = set()
        self.possible_types[type_name]["count"] += 1

        # Сохраняем уникальные образцы (для object не сохраняем)
        if type_name not in ("object",):
            # Для массивов – сохраняем JSON-представление (обрезанное)
            if type_name == "array":
                sample_repr = json.dumps(
                    value, ensure_ascii=False, separators=(",", ":")
                )[:200]
                self.samples_by_type[type_name].add(sample_repr)
            else:
                # Для примитивов и строк – само значение
                self.samples_by_type[type_name].add(value)

        # Обновляем метрики min/max/length
        if isinstance(value, (int, float)):
            if self.min is None or value < self.min:
                self.min = value
            if self.max is None or value > self.max:
                self.max = value
        elif isinstance(value, str):
            length = len(value)
            if self.min_length is None or length < self.min_length:
                self.min_length = length
            if self.max_length is None or length > self.max_length:
                self.max_length = length
        elif isinstance(value, list):
            length = len(value)
            if self.min_length is None or length < self.min_length:
                self.min_length = length
            if self.max_length is None or length > self.max_length:
                self.max_length = length
            # Определяем items_type для массива
            elem_types = set()
            for item in value:
                if item is None:
                    continue
                t, _ = infer_type(item)
                elem_types.add(t)
            if len(elem_types) == 1:
                self.items_type = next(iter(elem_types))
            else:
                self.items_type = "mixed"
            # Статистика элементов массива
            if self.items_stats is None:
                self.items_stats = FieldStats(self.sample_limit)
            for item in value:
                self.items_stats.update(item)
        elif isinstance(value, dict):
            # Дочерние поля будут добавлены при обходе
            pass


def traverse_and_collect(data: Any, stats: FieldStats, sample_limit: int):
    stats.update(data)

    if isinstance(data, dict):
        for key, value in data.items():
            if key not in stats.children:
                stats.children[key] = FieldStats(sample_limit)
            traverse_and_collect(value, stats.children[key], sample_limit)
    elif isinstance(data, list):
        # элементы уже обработаны через items_stats
        pass


def build_hierarchical_report(
    stats: FieldStats, max_samples: int, is_root: bool = False
) -> Dict[str, Any]:
    """
    Рекурсивно строит отчёт.
    Если is_root=True, возвращает словарь children (поля верхнего уровня).
    """
    if is_root:
        result = {}
        for child_key, child_stats in stats.children.items():
            result[child_key] = build_hierarchical_report(
                child_stats, max_samples, False
            )
        return result

    possible_types_list = []
    for tname, info in stats.possible_types.items():
        samples = list(stats.samples_by_type.get(tname, set()))[:max_samples]
        # Преобразуем кортежи в списки (для YAML)
        samples = [list(s) if isinstance(s, tuple) else s for s in samples]
        possible_types_list.append(
            {"type": tname, "count": info["count"], "samples": samples}
        )

    node = {"has_null": stats.has_null, "possible_types": possible_types_list}
    if stats.min is not None:
        node["min"] = stats.min
    if stats.max is not None:
        node["max"] = stats.max
    if stats.min_length is not None:
        node["min_length"] = stats.min_length
    if stats.max_length is not None:
        node["max_length"] = stats.max_length
    if stats.items_type:
        node["items_type"] = stats.items_type

    # Для массивов – информация об элементах
    if (
        "array" in stats.possible_types
        and stats.items_stats
        and stats.items_stats.possible_types
    ):
        node["items"] = build_hierarchical_report(stats.items_stats, max_samples, False)

    # Дочерние поля (для объектов)
    if stats.children:
        children = {}
        for child_key, child_stats in stats.children.items():
            children[child_key] = build_hierarchical_report(
                child_stats, max_samples, False
            )
        node["children"] = children

    return node


def _read_ndjson_sample(s3_client, file_key: str, sample_size: int) -> List[str]:
    zip_bytes = s3_client.get_object(file_key)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        ndjson_files = [name for name in zf.namelist() if name.endswith(".ndjson")]
        if not ndjson_files:
            return []
        with zf.open(ndjson_files[0]) as f:
            lines = []
            for i, line in enumerate(f):
                if i >= sample_size:
                    break
                lines.append(line.decode("utf-8").strip())
            return lines


def analyze_sample(
    bucket: str, prefix: str, sample_size: int = 1000, max_samples_per_field: int = 100
) -> Dict[str, Any]:
    s3 = S3Client()
    objects = s3.list_objects(prefix=prefix)
    zip_files = [obj for obj in objects if obj["Key"].endswith(".zip")]
    if not zip_files:
        raise ValueError(f"No ZIP files found in {bucket}/{prefix}")
    first_zip = zip_files[0]["Key"]
    lines = _read_ndjson_sample(s3, first_zip, sample_size)

    root_stats = FieldStats(max_samples_per_field)

    for line in lines:
        if not line:
            continue
        try:
            record = json.loads(line)
        except:
            continue
        traverse_and_collect(record, root_stats, max_samples_per_field)

    report = {
        "path": prefix,
        "schema": build_hierarchical_report(
            root_stats, max_samples_per_field, is_root=True
        ),
    }
    return report
