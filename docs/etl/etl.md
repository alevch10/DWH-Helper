# Универсальный ETL-загрузчик (модуль `etl`)

## 📌 Описание
Модуль `etl` предоставляет **единый, конфигурируемый через YAML** механизм для загрузки данных из S3 (ZIP-архивы с NDJSON) и Яндекс.Метрики (потоковая выгрузка через Logs API) в целевые таблицы PostgreSQL. Вся логика трансформации, маппинга полей, стратегий вставки и обработки ошибок описывается в YAML-конфиге, что позволяет подключать новые источники данных без изменения кода.

**Ключевые особенности:**
- ✅ Два типа источника: `s3` (ZIP с NDJSON) и `yandex_metrika` (Logs API → TSV).
- ✅ Потоковая обработка данных Яндекс.Метрики по дням с транзакциями.
- ✅ Рекурсивный маппинг полей с поддержкой точечных путей (например, `user_properties.EHR_ID`).
- ✅ Типизация данных (`string`, `integer`, `float`, `boolean`, `datetime`, `json`, `array`, `inet`, `uuid`).
- ✅ Гибкая обработка `null` и `value_map`.
- ✅ Batch-вставка с настраиваемым размером батча.
- ✅ Поддержка `ON CONFLICT` (`DO NOTHING`, `DO UPDATE`).
- ✅ Стратегии обновления: `always` (всегда вставка) или `on_change` (только при изменении значений).
- ✅ Дедупликация буфера для таблиц с `ON CONFLICT DO UPDATE`.
- ✅ Для S3 – жёсткая валидация неизвестных полей; для Яндекс.Метрики – проверка только запрошенных полей.
- ✅ Возобновление после ошибки с указанием файла/строки (S3) или даты (Яндекс.Метрика).
- ✅ Естественная сортировка имён файлов (S3).

---

## 🔌 Эндпоинт

`POST /etl/transformer`

### Заголовки
- `Authorization: Bearer <токен>` или `OAuth <токен>` (требуется `write`‑доступ).
  - Для источника `yandex_metrika` токен используется как OAuth-токен Метрики.
- `Content-Type: application/x-yaml`

### Параметры запроса (query)
| Параметр            | Тип    | Описание                                                                 |
|---------------------|--------|--------------------------------------------------------------------------|
| `start_after_file`  | string | **S3**: S3-ключ файла, с которого продолжить (опционально)                |
| `start_after_line`  | int    | **S3**: Номер строки внутри файла (0‑based, по умолчанию 0)               |
| `start_date`        | string | **Яндекс.Метрика**: дата в формате YYYY-MM-DD, с которой начать загрузку |

### Тело запроса (YAML)
Содержит конфигурацию ETL (описание источника, таблиц и маппинга полей). Формат зависит от типа источника.

### Ответ
**Успех (200):**
```json
{
  "status": "success",
  "message": "ETL completed",
  "statistics": {
    "files_processed": 7,
    "lines_processed": 15234,
    "batches": {
      "yandex_metrika.events": 15234,
      "yandex_metrika.devices": 15234
    }
  }
}
```

**Прерывание (200, но status = "interrupted"):**
Для S3:
```json
{
  "status": "interrupted",
  "message": "Unknown keys in record: $insert_id",
  "failed_file": "amplitude_exports/2022_week_3.zip",
  "failed_line": 42,
  "last_successful_file": "amplitude_exports/2022_week_3.zip",
  "last_successful_line": 41
}
```
Для Яндекс.Метрики:
```json
{
  "status": "interrupted",
  "message": "Transformation errors: ...",
  "failed_date": "2026-02-03",
  "last_successful_date": "2026-02-02"
}
```

**Ошибка валидации конфига (422):** детали ошибок Pydantic.

---

## 📄 Формат YAML-конфигурации

### Корневая структура
```yaml
source:
  type: s3                   # или yandex_metrika
  # ... параметры источника ...

tables:
  - name: schema.table_name
    # ... параметры таблицы ...
    fields: [...]
```

### Источник `s3`
```yaml
source:
  type: s3
  bucket: my-bucket
  prefix: data/
  file_pattern: "*.zip"      # опционально, по умолчанию "*.zip"
  sort:
    by: key                  # key или last_modified
    natural: true
    order: asc               # asc или desc
```

### Источник `yandex_metrika`
```yaml
source:
  type: yandex_metrika
  counter_id: 106613495
  date_from: "2026-02-01"
  date_to: "2026-02-07"
  source: hits               # hits или visits
  fields:                    # список полей (если не указан – из настроек)
    - ym:pv:watchID
    - ym:pv:clientID
    # ...
  chunk_days: 7              # размер интервала для логирования, по умолчанию 7
```
**Особенности:**
- Данные запрашиваются и обрабатываются по дням (один день – одна транзакция).
- Все запрошенные поля должны быть либо замаплены в таблицы, либо быть в списке `fields` – иначе ошибка "Unexpected fields".
- Для полей с некорректным JSON (например, `params`) рекомендуется тип `string`.

### Описание таблиц и маппинга – без изменений
(Параметры `TableConfig` и `FieldMapping` остаются прежними, как в оригинальном документе.)

### Пример конфигурации для Яндекс.Метрики (нормализованные таблицы)
```yaml
source:
  type: yandex_metrika
  counter_id: 106613495
  date_from: "2026-02-01"
  date_to: "2026-02-07"
  source: hits
  fields:
    - ym:pv:watchID
    - ym:pv:pageViewID
    - ym:pv:visitID
    - ym:pv:clientID
    - ym:pv:dateTime
    # ... все необходимые поля ...

tables:
  - name: yandex_metrika.events
    batch_size: 5000
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: client_id
        sources: ["clientID"]
        type: integer
      # ... маппинг полей events ...

  - name: yandex_metrika.event_params
    batch_size: 5000
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: params
        sources: ["params"]
        type: string   # не json из-за битого JSON

  # ... остальные таблицы ...
```

### Пример конфигурации для Amplitude Web (S3)
(Оставить существующий пример без изменений.)

---

## ⚙️ Как это работает (внутреннее устройство)

1. **Чтение конфига** – парсинг YAML и валидация через Pydantic-модели.
2. **Ветвление по типу источника:**
   - **S3**: _process_s3_files (синхронная, запускается в executor’е)
   - **yandex_metrika**: _process_yandex_metrika_async (асинхронная)
3. **S3** – как прежде.
4. **Яндекс.Метрика**:
   - Получение токена из заголовка.
   - Для каждого дня из диапазона:
     - Создание/ожидание logrequest, потоковое чтение частей через асинхронный генератор `stream_metrika_lines`.
     - Каждая TSV-строка → словарь → валидация `MetrikaHitRow` → `model_dump(by_alias=True)`.
     - Проверка, что все ключи словаря входят в нормализованный список запрошенных полей.
     - Трансформация через `_transform_record`, накопление в буферы.
     - При достижении `batch_size` – сброс в БД в рамках одной транзакции.
     - После успешной обработки дня – коммит, при ошибке – откат.
5. **Возобновление** – при ошибке возвращаются координаты (`failed_date`, `last_successful_date`).

---

## 🧪 Примеры использования

### S3 (curl)
```bash
curl -X POST "http://localhost:8000/etl/transformer?start_after_file=amplitude_exports/2022_week_3.zip&start_after_line=1500" \
  -H "Authorization: Bearer <токен>" \
  -H "Content-Type: application/x-yaml" \
  --data-binary @amplitude_web_etl.yaml
```

### Яндекс.Метрика (curl)
```bash
curl -X POST "http://localhost:8000/etl/transformer?start_date=2026-02-05" \
  -H "Authorization: OAuth <токен>" \
  -H "Content-Type: application/x-yaml" \
  --data-binary @ym_normalized.yaml
```

---

## 📚 Связанные документы
- [Модуль DB](../db/db.md)
- [Модуль S3](../s3/s3.md)
- [Модуль Яндекс.Метрики](../yandex_metrica/yandex_metrica.md)
- [Конфигурация приложения](../config/config.md)
- [Модуль авторизации](../config/auth.md)
```
