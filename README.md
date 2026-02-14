# О приложении:
Python приложение для работы с данными продуктовой аналитики. 
Цель приложения: собирать данные из разных систем и приводить их в удобный формат для хранения внутри DWH компании. 

Название: DWH Helper
Описание: Приложение для работы с данными продуктовой аналитики. 
Версия: 1.0.0
Создатель: Андрей Левченко aa.levchenko@severmed.com

## Стек: 
- Python 3.13
- FastAPI
- uvicorn
- httpx
- Pydantic
- psycopg2
- boto3

## Структура

```
app/
├── amplitude/             # Интеграция с Amplitude API
│ ├── init.py
│ ├── client.py            # Асинхронный клиент для выгрузки событий из Amplitude
│ ├── router.py            # Эндпоинты для экспорта данных Amplitude
│ ├── export_utils.py      # Собирает данные за временной промежуток в .ndjson
│ └── init.py
│
├── appmetrica/            # Интеграция с AppMetrica API
│ ├── init.py
│ ├── client.py            # Асинхронный клиент для AppMetrica
│ ├── router.py            # Эндпоинты для работы с AppMetrica
│ └── init.py
│
├── auth/                  # Аутентификация и авторизация
│ ├── init.py
│ ├── deps.py              # Зависимости FastAPI (require_read, require_write)
│ ├── schemas.py           # Pydantic-модели для токенов
│ └── init.py
│
├── config/                # Конфигурация приложения
│ ├── init.py
│ ├── settings.py          # Pydantic BaseSettings с вложенными блоками (DB, S3, AppMetrica…)
│ ├── logger.py            # Настройка логирования
│ └── init.py
│
├── db /                   # Работа с таблицами DB (PostgreSQL)
│ ├── init.py              # Экспорт router
│ ├── repository.py        # DBRepository – пул соединений, общие и специфические методы
│ ├── router.py            # FastAPI‑эндпоинты для 8 таблиц (GET/POST)
│ └── schemas.py           # Pydantic‑модели всех таблиц DB
│
├── etl/                   # ETL‑процессор: трансформация и загрузка в DB
│ ├── init.py              # Загрузка field_mappings.yaml → MAPPINGS
│ ├── transformer.py       # transform_single_record – преобразование сырых данных в модели DB
│ ├── schemas.py           # Pydantic-модели для процессора
│ ├── orchestrator.py      # process_source, _process_record, compare_changeable, ProcessingInterrupted
│ ├── field_mappings.yaml  # Конфиг маппинга (permanent + changeable)
│ └── router.py            # POST /transform/user-properties
│
├── s3/                    # Операции с S3‑совместимым хранилищем
│ ├── init.py
│ ├── client.py            # Boto3 S3‑клиент (upload, download, list, delete, patch)
│ ├── router.py            # Эндпоинты для работы с объектами S3
│ └── init.py
│
└── main.py                # Точка входа: создание FastAPI, подключение роутеров
```

## Конфигурация
Переменные задаются в  `.env`, пример заполнения в `.env.example`

## Модули приложения


### Amplitude Integration (`/amplitude`)

Интеграция с сервисом [amplitude](http://app.amplitude.com/):
- Позволяет выгружать сырые исторические данные. 
- Отдает данные в формате файла, доступного для скачивания. 
-- Скачивает данные посуточно, накапливает в единый .ndjson файл и архивирует в .zip для загрузки.

**Endpoints:**
```bash
GET /amplitude/export?start=YYYYMMDD&end=YYYYMMDD&source=web|mobile
```
- `start` — Start date (YYYYMMDD), hours default to 00
- `end` — End date (YYYYMMDD), hours default to 23
- `source` — `web` or `mobile` (selects credentials)

**Example:**
```bash
curl -OJ "http://localhost:8000/amplitude/export?start=20240201&end=20240207&source=web"
```

### AppMetrica Integration (`/appmetrica`)

Интеграция с сервисом [appmetrica](https://appmetrica.yandex.ru/):
- Позволяет выгружать сырые исторические данные. 
- Отдает данные в формате файла, доступного для скачивания. 
-- Скачивает данные посуточно, накапливает в единый .json / .csv файл и архивирует в .zip для загрузки.

**Endpoints:**
```bash
GET /appmetrica/ping
GET /appmetrica/export?skip_unavailable_shards=false&date_since=2026-02-12%2000%3A00%3A00&date_until=2026-02-12%2018%3A00%3A00&date_dimension=default&use_utf8_bom=true
```

### Auth

Сервис аутентификации:
- Использует OAuth токен Яндекс.ID для проверки личности. 
-- Токен выписывается по адресу: https://oauth.yandex.ru/authorize?response_type=token&client_id={CLIENT_ID} 
- Разделяет и проверяет доступы:
-- require_read - доступ на чтение
-- require_write - доступ на запись / удаление / редактирование
- Права выдаются путем прописывания логина в переменные:
-- AUTH_READ_ACCESS
-- AUTH_WRITE_ACCESS

### Config

Пакет для конфигурации приложения:
- Настройка переменных
- Настройка логирования


### DB (`/db`)

Модуль для работы с 8 таблицами PostgreSQL:
Ключевые особенности реализации:
- PostgreSQL — подключение через psycopg2.pool.ThreadedConnectionPool
- Единый репозиторий — все общие методы (insert_batch, select) + специфические (get_latest_changeable_for_ehrs, update_migrated_tmp)
- Динамический расчёт лимита строк — на основе количества колонок в Pydantic-модели и протокольного лимита PostgreSQL (65 535 параметров). Используется SAFETY_FACTOR для запаса.
- Автоматическое чанкование — разбиение по строкам, никаких проверок по байтам.
- Возврат ID вставленных записей — через RETURNING (значения приводятся к строке).
- Реальный подсчёт количества батчей — возвращается в поле batches.
- Строгая валидация дат — event_time принимает только ISO 8601, невалидные данные вызывают HTTP 422.
- Поддержка ON CONFLICT — для permanent_user_properties и changeable_user_properties.

Обеспечивает **единый репозиторий** с пулом соединений, динамическим расчётом лимитов и строгой валидацией.

**Таблицы:**
- `events_part` — события пользователей с устройством и сессией
- `mobile_devices` — метаданные мобильных устройств
- `permanent_user_properties` — статические свойства пользователя
- `changeable_user_properties` — изменяемые свойства с историей
- `technical_data` — техническая информация Amplitude
- `tmp_event_properties` — свойства событий (JSONB)
- `tmp_user_properties` — сырые свойства пользователей (JSONB) с флагом `migrated`. 
- `user_locations` — геолокационные данные

**GET Endpoints** — выборка с фильтрацией и сортировкой:
```bash
GET /db/events-part
GET /db/mobile-devices
GET /db/permanent-user-properties
GET /db/changeable-user-properties
GET /db/technical-data
GET /db/event-properties
GET /db/user-properties
GET /db/user-locations
```
Параметры запроса (все опциональны):
- pk — фильтр по первичному ключу (uuid, device_id, ehr_id)
- limit — ограничение количества строк
- sort_by — колонка для сортировки
- sort_dir — направление: asc/desc (по умолч. asc)
- migrated — (только для /user-properties) фильтр по флагу migrated
Ответ:
``` json
{
  "rows": [...],
  "count": 10
}
```
**POST Endpoints** — пакетная вставка:

``` bash
POST /db/events-part
POST /db/mobile-devices
POST /db/permanent-user-properties
POST /db/changeable-user-properties
POST /db/technical-data
POST /db/event-properties
POST /db/user-properties
POST /db/user-locations
```
Тело запроса:
``` json
{
  "data": [{...}, {...}, ...]
}
```
Ответ:
```
json
{
  "inserted_ids": ["3fa85f64-5717-4562-b3fc-2c963f66afa6", ...],
  "count": 42,
  "batches": 2
}
```

### ETL (`/etl`)

Модуль для трансформации данных, задача которого, переложить данные системы аналитики в формат, который соответствует базе данных. 

### S3 (`/s3`)

AWS S3 and S3-compatible storage operations with full CRUD support.

**GET Endpoints:**
```bash
GET /s3/objects?prefix=folder/
GET /s3/object-info?key=object-key
GET /s3/download?key=object-key
```

**POST Endpoints:**
```bash
POST /s3/upload?key=path/to/file
Content-Type: multipart/form-data
Body: [file content]
```

**PUT Endpoints:**
```bash
PUT /s3/update?key=path/to/file
Content-Type: multipart/form-data
Body: [file content]
```

**PATCH Endpoints:**
```bash
PATCH /s3/patch?key=path/to/file&offset=0
Content-Type: multipart/form-data
Body: [file content]
```

**DELETE Endpoints:**
```bash
DELETE /s3/delete?key=path/to/file
```

**Response Examples:**

List objects:
```json
{
  "prefix": "folder/",
  "objects": [
    {
      "Key": "folder/file1.txt",
      "LastModified": "2024-01-15T10:00:00Z"
    }
  ],
  "count": 1
}
```

Upload response:
```json
{
  "key": "path/to/file",
  "etag": "\"abc123\"",
  "version_id": "v123",
  "size": 1024
}
```

## API Documentation

Interactive API documentation available at:

- **Swagger UI:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc
- **OpenAPI JSON:** http://localhost:8000/openapi.json

---

## Development

### Syntax check
```bash
poetry run python -m py_compile app/**/*.py
```

### Load configuration
```bash
poetry run python -c "from app.config.settings import settings; print(settings)"
```

## Запуск 
### Локально

1. Установите Poetry и зависимости:

```bash
pip install poetry
poetry install
cp .env.example .env # Отредактируйте .env под свои ключи и параметры
poetry run uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```