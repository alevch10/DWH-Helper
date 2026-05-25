# Универсальный ETL-загрузчик (модуль `etl`)

## 📌 Описание
Модуль `etl` предоставляет **единый, конфигурируемый через YAML** механизм для загрузки данных из S3 (ZIP-архивы с NDJSON) в целевые таблицы PostgreSQL. Вся логика трансформации, маппинга полей, стратегий вставки и обработки ошибок описывается в YAML-конфиге, что позволяет подключать новые источники данных без изменения кода.

**Ключевые особенности:**
- ✅ Чтение ZIP-файлов из S3 (один NDJSON на архив).
- ✅ Рекурсивный маппинг полей с поддержкой точечных путей (например, `user_properties.EHR_ID`).
- ✅ Типизация данных (`string`, `integer`, `float`, `boolean`, `datetime`, `json`, `array`, `inet`, `uuid`).
- ✅ Гибкая обработка `null` и `value_map`.
- ✅ Batch-вставка с настраиваемым размером батча.
- ✅ Поддержка `ON CONFLICT` (`DO NOTHING`, `DO UPDATE`).
- ✅ Стратегии обновления: `always` (всегда вставка) или `on_change` (только при изменении значений).
- ✅ Дедупликация буфера для таблиц с `ON CONFLICT DO UPDATE` (предотвращает ошибку "cannot affect row a second time").
- ✅ Жёсткая валидация неизвестных полей (кроме полей типа `json`, которые игнорируются).
- ✅ Возобновление после ошибки с указанием файла и строки (`start_after_file`, `start_after_line`).
- ✅ Естественная сортировка имён файлов (например, `file_2.zip` < `file_10.zip`).

---

## 🔌 Эндпоинт

`POST /etl/transformer`

### Заголовки
- `Authorization: Bearer <JWT_токен>` (требуется `write`-доступ)
- `Content-Type: application/x-yaml` (или `application/json` с обёрткой `{"config": "..."}` – для Swagger)

### Параметры запроса (query)
| Параметр            | Тип    | Описание                                                       |
|---------------------|--------|----------------------------------------------------------------|
| `start_after_file`  | string | S3-ключ файла, с которого продолжить (опционально)             |
| `start_after_line`  | int    | Номер строки внутри файла (0‑based, по умолчанию 0)            |

### Тело запроса (YAML)
Содержит конфигурацию ETL (описание источника, таблиц и маппинга полей).

### Ответ
**Успех (200):**
```json
{
  "status": "success",
  "message": "ETL completed",
  "statistics": {
    "files_processed": 2,
    "lines_processed": 15234,
    "batches": {
      "amplitude_web.events": 15234,
      "amplitude_web.devices": 124,
      "amplitude_web.locations": 15234,
      "amplitude_web.event_properties": 15234,
      "amplitude_web.users": 15234
    }
  }
}
```

**Прерывание (200, но status = "interrupted"):**
```json
{
  "status": "interrupted",
  "message": "Unknown keys in record: $insert_id",
  "failed_file": "amplitude_exports/2022_week_3.zip",
  "failed_line": 42,
  "last_successful_file": "amplitude_exports/2022_week_3.zip",
  "last_successful_line": 41,
  "error_details": null
}
```

**Ошибка валидации конфига (422):** вернёт детали ошибок Pydantic.

---

## 📄 Формат YAML-конфигурации

### Корневая структура
```yaml
source:
  type: s3                     # только s3 (поддержка расширения)
  bucket: my-bucket
  prefix: data/
  file_pattern: "*.zip"        # опционально, по умолчанию "*.zip"
  sort:
    by: key                    # key или last_modified
    natural: true              # естественная сортировка (file_2 < file_10)
    order: asc                 # asc или desc

tables:
  - name: schema.table_name    # полное имя таблицы
    primary_key: [col1, col2]  # опционально, необходимо для ON CONFLICT и on_change
    on_conflict: DO NOTHING    # или DO UPDATE, опционально
    update_strategy: always    # или on_change (по умолчанию always)
    ignore_fields_for_diff: [] # поля, игнорируемые при сравнении (для on_change)
    batch_size: 1000           # размер батча (по умолч. 1000)
    fields: [...]              # список полей (см. ниже)
```

### Описание поля `fields`

```yaml
- target: column_name          # имя колонки в целевой таблице
  sources:                     # список путей в сырых данных (первый не-NULL)
    - "field"
    - "nested.field"
  type: string|integer|float|boolean|datetime|json|array|inet|uuid
  required: true|false         # по умолчанию false
  default: <значение>          # значение по умолчанию, если все источники None
  format: iso                  # только для datetime (iso или omit)
  null_values: ["", "N/A"]     # значения, которые преобразуются в NULL
  value_map:                   # отображение значений
    "true": true
    "1": true
  ignore: true|false           # поле не вставляется, но его источники считаются "известными"
```

### Пример конфигурации для Amplitude Web

```yaml
source:
  type: s3
  bucket: dwh-helper-data
  prefix: amplitude_exports/
  sort:
    by: key
    natural: true
    order: asc

tables:
  - name: amplitude_web.events
    primary_key: [uuid, event_time]
    on_conflict: DO NOTHING
    batch_size: 5000
    fields:
      - target: uuid
        sources: ["uuid"]
        type: uuid
        required: true
      - target: event_time
        sources: ["event_time"]
        type: datetime
        required: true
      - target: session_id
        sources: ["session_id"]
        type: integer
        required: true
      - target: user_id
        sources: ["user_id"]
        type: integer
        null_values: ["", "N/A", "-1"]
      - target: event_type
        sources: ["event_type"]
        type: string
        required: true
      - target: event_id
        sources: ["event_id"]
        type: integer
        required: true
      - target: device_id
        sources: ["device_id"]
        type: string
        required: true

  - name: amplitude_web.devices
    primary_key: [device_id]
    on_conflict: DO UPDATE
    update_strategy: on_change
    ignore_fields_for_diff: [device_id]
    batch_size: 1000
    fields:
      - target: device_id
        sources: ["device_id"]
        type: string
        required: true
      - target: device_family
        sources: ["device_family"]
        type: string
      - target: device_type
        sources: ["device_type"]
        type: string
      - target: os_name
        sources: ["os_name"]
        type: string
      - target: os_version
        sources: ["os_version"]
        type: string

  - name: amplitude_web.locations
    primary_key: [uuid, event_time]
    on_conflict: DO NOTHING
    fields:
      - target: uuid
        sources: ["uuid"]
        type: uuid
        required: true
      - target: event_time
        sources: ["event_time"]
        type: datetime
        required: true
      - target: city
        sources: ["city"]
        type: string
      - target: country
        sources: ["country"]
        type: string
      - target: ip_address
        sources: ["ip_address"]
        type: inet
        required: true
      - target: language
        sources: ["language"]
        type: string
      - target: region
        sources: ["region"]
        type: string

  - name: amplitude_web.event_properties
    primary_key: [uuid, event_time]
    on_conflict: DO NOTHING
    fields:
      - target: uuid
        sources: ["uuid"]
        type: uuid
        required: true
      - target: event_time
        sources: ["event_time"]
        type: datetime
        required: true
      - target: event_properties
        sources: ["event_properties", "properties"]
        type: json
        required: true

  - name: amplitude_web.users
    primary_key: [uuid, event_time]
    fields:
      - target: uuid
        sources: ["uuid"]
        type: uuid
        required: true
      - target: event_time
        sources: ["event_time"]
        type: datetime
        required: true
      - target: user_id
        sources: ["user_id"]
        type: integer
        null_values: ["", "N/A", "-1"]
      - target: ehr_id
        sources: ["user_properties.EHR_ID"]
        type: integer
        null_values: ["", "N/A", "no_ehr", "null"]
      # ... остальные поля
```

---

## ⚙️ Как это работает (внутреннее устройство)

1. **Чтение конфига** – парсинг YAML и валидация через Pydantic-модели (`ETLConfig`, `TableConfig`, `FieldMapping`).
2. **Сканирование S3** – получение списка файлов по префиксу, сортировка (естественная или по дате).
3. **Итерация по файлам** – для каждого ZIP-архива:
   - Поиск первого `.ndjson` файла.
   - Построчная загрузка NDJSON.
   - Для каждой строки:
     - Проверка неизвестных полей (рекурсивно, но игнорируя содержимое JSONB-полей).
     - Трансформация записи по каждому описанию таблицы (через `_transform_record`).
     - Накопление данных в буфере для каждой таблицы.
     - При достижении `batch_size` – вызов `flush_table_buffer`.
4. **Вставка в БД** – использование `DBRepository.insert_batch` или `upsert_batch` (в зависимости от `on_conflict` и `update_strategy`).
5. **Дедупликация** для таблиц с `ON CONFLICT DO UPDATE` – в буфере оставляется только последняя запись по каждому первичному ключу (по `event_time` или порядку).
6. **Возобновление** – при ошибке выбрасывается `ProcessingInterrupted` с координатами (файл, строка). Клиент может повторить запрос с теми же параметрами `start_after_file`/`start_after_line`.

---

## 🧪 Примеры использования

### Через curl (чистый YAML)
```bash
curl -X POST "http://localhost:8000/etl/transformer?start_after_file=amplitude_exports/2022_week_3.zip&start_after_line=1500" \
  -H "Authorization: Bearer <токен>" \
  -H "Content-Type: application/x-yaml" \
  --data-binary @amplitude_web_etl.yaml
```

### Через Swagger UI
1. Открыть `http://localhost:8000/docs`.
2. Найти `POST /etl/transformer`, нажать "Try it out".
3. В поле "Request body" вставить YAML-конфиг.
4. Добавить query-параметры (опционально).
5. Выполнить запрос.

---

## 🐞 Логирование и отладка

- Логи уровня `INFO` показывают процесс обработки файлов, количество строк, успешные вставки.
- Ошибки уровня `WARNING` и `ERROR` логируются с полной трассировкой.
- Для детальной отладки установите `LOGGING_LEVEL=DEBUG` в `.env`.

---

## 📚 Связанные документы

- [Модуль DB](../db/db.md) – целевые таблицы и репозиторий.
- [Модуль S3](../s3/s3.md) – работа с объектным хранилищем.
- [Конфигурация приложения](../config/config.md) – переменные окружения для ETL (`ETL_QUERY_PARAMS_TO_REMOVE` и др.).
- [Модуль авторизации](../config/auth.md) – требования к токену.

