# Универсальный ETL-загрузчик (модуль `etl`)
 >* Версия 2.0.0
## 📌 Описание
Модуль `etl` предоставляет **единый, конфигурируемый через YAML** механизм для загрузки данных в целевые таблицы PostgreSQL из трёх источников:
- **S3** – ZIP-архивы с NDJSON
- **Яндекс.Метрика** – потоковая выгрузка через Logs API
- **AppMetrica** – экспорт событий через Logs API (CSV/JSON)

Вся логика трансформации, маппинга полей, стратегий вставки и обработки ошибок описывается в YAML-конфиге, что позволяет подключать новые источники данных без изменения кода.

**Ключевые особенности:**
- ✅ Три типа источника: `s3`, `yandex_metrika`, `appmetrica`
- ✅ Потоковая обработка данных API-источников по дням/чанкам с транзакциями
- ✅ Рекурсивный маппинг полей с поддержкой точечных путей
- ✅ Типизация данных (`string`, `integer`, `float`, `boolean`, `datetime`, `json`, `array`, `inet`, `uuid`)
- ✅ Гибкая обработка `null` и `value_map`
- ✅ Batch-вставка с настраиваемым размером батча
- ✅ Поддержка `ON CONFLICT` (`DO NOTHING`, `DO UPDATE`)
- ✅ Стратегии обновления: `always` или `on_change`
- ✅ Дедупликация буфера для `ON CONFLICT DO UPDATE`
- ✅ Для S3 – жёсткая валидация неизвестных полей; для API-источников – проверка только запрошенных полей
- ✅ Возобновление после ошибки с указанием координат (файл/строка для S3, дата для API-источников)

---

## 🔌 Эндпоинт

`POST /etl/transformer`

### Заголовки
- `Authorization: Bearer <токен>` или `OAuth <токен>` (требуется `write`‑доступ)
- `Content-Type: application/x-yaml`

### Параметры запроса (query)

| Параметр | Тип | Описание |
|----------|-----|----------|
| `start_after_file` | string | **S3**: S3-ключ файла, с которого продолжить |
| `start_after_line` | int | **S3**: Номер строки внутри файла (0‑based) |
| `start_date` | string | **Яндекс.Метрика / AppMetrica**: дата в формате `YYYY-MM-DD`, с которой начать обработку |

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

**Прерывание (200, status = "interrupted"):**
Для S3:
```json
{
  "status": "interrupted",
  "message": "Unknown keys in record: $insert_id",
  "failed_file": "amplitude_exports/2022_week_3.zip",
  "failed_line": 42,
  "last_successful_file": "...",
  "last_successful_line": 41
}
```
Для API-источников:
```json
{
  "status": "interrupted",
  "message": "Transformation errors: ...",
  "failed_date": "2026-02-03",
  "last_successful_date": "2026-02-02"
}
```

---

## 📄 Формат YAML-конфигурации

### Корневая структура
```yaml
source:
  type: s3 | yandex_metrika | appmetrica
  # ... параметры источника ...

tables:
  - name: schema.table_name
    primary_key: [col1, col2]   # опционально
    on_conflict: DO NOTHING     # или DO UPDATE
    update_strategy: always     # или on_change
    ignore_fields_for_diff: []  # для on_change
    batch_size: 1000
    fields:                     # список маппингов полей
      - target: column_name
        sources: ["field_name"]
        type: string|integer|...
        required: true|false
        # ... остальные параметры поля
```

### Источник `s3`
```yaml
source:
  type: s3
  bucket: my-bucket
  prefix: data/
  file_pattern: "*.zip"
  sort:
    by: key            # key или last_modified
    natural: true
    order: asc
```

### Источник `yandex_metrika`
```yaml
source:
  type: yandex_metrika
  counter_id: 106613495
  date_from: "2026-02-01"
  date_to: "2026-02-07"
  source: hits         # hits или visits
  fields:              # список полей (если не указан – из настроек)
    - ym:pv:watchID
    - ym:pv:clientID
  chunk_days: 7
```
**Особенности:** обработка по дням, один день – одна транзакция. Все поля, полученные в ответе, должны быть перечислены в `fields`. Поле `params` рекомендуется маппить как `string` из-за возможного битого JSON.

### Источник `appmetrica`
```yaml
source:
  type: appmetrica
  application_id: 473434         # необязательно, если задан в настройках
  date_since: "2026-05-01"
  date_until: "2026-05-07"
  export_format: csv             # csv или json
  fields:                        # полный список полей (если не указан – все поля по умолчанию)
    - profile_id
    - event_datetime
    # ...
  chunk_days: 7
  # прочие параметры API: date_dimension, skip_unavailable_shards, use_utf8_bom
```
**Особенности:** запросы разбиваются на интервалы по `chunk_days`. Ожидание готовности экспорта может быть долгим – сервер ждёт без таймаута. Даты приходят в формате `YYYY-MM-DD HH:MM:SS`, ETL корректно их конвертирует.

### Примеры конфигураций

Полные примеры для Amplitude Web (S3), нормализованных таблиц Яндекс.Метрики и AppMetrica приведены в Swagger UI (`/docs`) в описании эндпоинта.

---

## ⚙️ Как это работает

1. **Чтение конфига** – парсинг YAML, валидация Pydantic-моделью `ETLConfig`.
2. **Ветвление по типу источника** – вызывается соответствующий обработчик.
   - **S3**: синхронная обработка файлов в executor’е.
   - **Яндекс.Метрика / AppMetrica**: асинхронные генераторы/запросы с разбиением по дням/чанкам.
3. **Для API-источников**:
   - Получается OAuth-токен из заголовка.
   - Для каждого дня/чанка формируется запрос к API, данные потоково (Метрика) или целиком (AppMetrica) загружаются.
   - Для AppMetrica CSV ответ парсится в список словарей с очисткой заголовков.
   - Для Метрики строки TSV нормализуются и валидируются через модель `MetrikaHitRow`.
   - Проверяется, что все ключи записи есть в списке запрошенных полей.
   - Производится трансформация через `_transform_record`, накопление в буферы.
   - При достижении `batch_size` буфер сбрасывается в БД в одной транзакции (используется `get_raw_connection`).
   - При успешной обработке чанка транзакция коммитится, при ошибке – откатывается.
4. **Возобновление** – при ошибке возвращаются координаты (`failed_date`, `last_successful_date`), клиент может повторно вызвать эндпоинт с параметром `start_date`.

---

## 🧪 Примеры использования

### S3 (curl)
```bash
curl -X POST "http://localhost:8000/etl/transformer?start_after_file=data.zip&start_after_line=1500" \
  -H "Authorization: Bearer <токен>" \
  -H "Content-Type: application/x-yaml" \
  --data-binary @config_s3.yaml
```

### Яндекс.Метрика (curl)
```bash
curl -X POST "http://localhost:8000/etl/transformer?start_date=2026-02-05" \
  -H "Authorization: OAuth <токен>" \
  -H "Content-Type: application/x-yaml" \
  --data-binary @config_ym.yaml
```

### AppMetrica (curl)
```bash
curl -X POST "http://localhost:8000/etl/transformer?start_date=2026-05-03" \
  -H "Authorization: OAuth <токен>" \
  -H "Content-Type: application/x-yaml" \
  --data-binary @config_appmetrica.yaml
```

---

## 📚 Связанные документы
- [Модуль DB](../db/db.md)
- [Модуль S3](../s3/s3.md)
- [Модуль Яндекс.Метрики](../yandex_metrica/yandex_metrica.md)
- [Модуль AppMetrica](../appmetrica/appmetrica.md)
- [Конфигурация приложения](../config/config.md)
