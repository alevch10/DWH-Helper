# DWH Helper

FastAPI application for product-analytics data ingestion with DWH integration, AppMetrica support, and S3 storage management.

**Version:** 1.0.0

---

## Structure

```
app/
├── config/
│   ├── settings.py      (Pydantic BaseSettings with nested config blocks)
│   ├── logger.py        (Logging configuration)
│   └── __init__.py
├── dwh_tables_worker/   (7 DWH table operations)
│   ├── schemas.py       (Pydantic validation models)
│   ├── router.py        (FastAPI endpoints)
│   └── __init__.py
├── appmetrica/          (AppMetrica API integration)
│   ├── client.py        (Async AppMetrica client)
│   ├── router.py        (FastAPI endpoints)
│   └── __init__.py
├── s3/                  (AWS S3 storage operations)
│   ├── client.py        (Boto3 S3 client)
│   ├── router.py        (FastAPI endpoints)
│   └── __init__.py
└── main.py              (FastAPI app initialization)
```

---

## Installation & Running

### 1. Install dependencies

```bash
poetry install
```

### 2. Configure environment

Copy `.env.example` to `.env` and fill in your settings:

```bash
cp .env.example .env
```

### 3. Start the server

**Development (with auto-reload):**
```bash
poetry run uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

**Production:**
```bash
poetry run uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## Configuration

Settings are loaded from `.env` using Pydantic BaseSettings with nested configuration blocks:

### Application Settings
```env
TITLE="DWH Helper"
VERSION="1.0.0"
DESCRIPTION="Product analytics data ingestion and DWH integration"
DEBUG=false
```

### Database (DWH)
```env
DWH_DATABASE_URL="sqlite:///./dwh.db"
DWH_MAX_WRITE_BATCH_BYTES=10000000        # 10MB batch limit
DWH_MAX_ROWS_PER_INSERT=1000              # Max rows per INSERT statement
```

### AppMetrica API
```env
APPMETRICA_BASE_URL="https://api.appmetrica.yandex.net"
APPMETRICA_API_KEY="your-api-key"
APPMETRICA_APPLICATION_ID=123456789
APPMETRICA_POLL_INTERVAL_SECONDS=5
APPMETRICA_POLL_TIMEOUT_SECONDS=300
```

### AWS S3 Storage
```env
S3_ACCESS_KEY_ID="your-access-key"
S3_SECRET_ACCESS_KEY="your-secret-key"
S3_REGION="us-east-1"
S3_ENDPOINT_URL="https://s3.amazonaws.com"    # Leave empty for AWS, or use custom URL
S3_BUCKET_NAME="your-bucket-name"
```

### Logging
```env
LOGGING_LEVEL=INFO
```

---

## Modules

### 1. DWH Tables Worker (`/dwh`)

Manages 7 DWH tables with batch insert support:

**Tables:**
- `events_part` - User events with device/session info
- `mobile_devices` - Mobile device metadata
- `permanent_user_properties` - Static user properties
- `technical_data` - Amplitude technical info
- `tmp_event_properties` - Event properties (JSON)
- `tmp_user_properties` - User properties (JSON)
- `user_locations` - Geolocation data

**GET Endpoints** - Retrieve records:
```bash
GET /dwh/events-part
GET /dwh/mobile-devices
GET /dwh/permanent-user-properties
GET /dwh/technical-data
GET /dwh/event-properties
GET /dwh/user-properties
GET /dwh/user-locations
```

Response format:
```json
{
  "rows": [...],
  "count": 10
}
```

**POST Endpoints** - Batch insert with automatic chunking:
```bash
POST /dwh/events-part
POST /dwh/mobile-devices
POST /dwh/permanent-user-properties
POST /dwh/technical-data
POST /dwh/event-properties
POST /dwh/user-properties
POST /dwh/user-locations
```

Request format:
```json
{
  "data": [
    {...},
    {...},
    ...
  ]
}
```

Response format:
```json
{
  "inserted_ids": [1, 2, 3],
  "count": 3,
  "batches": 1
}
```

**Features:**
- ✅ Automatic chunking when exceeding `max_rows_per_insert` or `max_write_batch_bytes`
- ✅ Simplified response format (no wrapper objects)
- ✅ Error handling for empty data arrays

---

### 2. AppMetrica Integration (`/appmetrica`)

Async client for AppMetrica API data export.

**Endpoints:**
```bash
GET /appmetrica/ping
GET /appmetrica/fetch-export?export_id=123&poll_interval=5&poll_timeout=300
```

**Features:**
- ✅ Async HTTP client with httpx
- ✅ Configurable polling with interval and timeout
- ✅ Support for export monitoring

---

### 3. S3 Storage (`/s3`)

AWS S3 and S3-compatible storage operations with full CRUD support.

**GET Endpoints:**
```bash
# List objects with optional prefix
GET /s3/objects?prefix=folder/

# Get object metadata (size, exists)
GET /s3/object-info?key=object-key

# Download object
GET /s3/download?key=object-key
```

**POST Endpoints:**
```bash
# Upload file
POST /s3/upload?key=path/to/file
Content-Type: multipart/form-data
Body: [file content]
```

**PUT Endpoints:**
```bash
# Update existing object
PUT /s3/update?key=path/to/file
Content-Type: multipart/form-data
Body: [file content]
```

**PATCH Endpoints:**
```bash
# Patch (partial update) object
PATCH /s3/patch?key=path/to/file&offset=0
Content-Type: multipart/form-data
Body: [file content]
```

**DELETE Endpoints:**
```bash
# Delete object
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

---

### 4. Amplitude Integration (`/amplitude`)

Integration with Amplitude analytics API for exporting event data (web/mobile sources).

**Endpoints:**
```bash
GET /amplitude/export?start=YYYYMMDD&end=YYYYMMDD&source=web|mobile
```
- `start` — Start date (YYYYMMDD), hours default to 00
- `end` — End date (YYYYMMDD), hours default to 23
- `source` — `web` or `mobile` (selects credentials)

**Features:**
- ✅ Two credential pairs: web and mobile (set in `.env`)
- ✅ Basic Auth (client_id:secret_key)
- ✅ Downloads .zip archive with .gz files for each hour
- ✅ Unpacks, merges, and re-zips as `{year}_week_{week}.zip` with NDJSON inside
- ✅ Returns ready-to-download archive for the requested week

**Example:**
```bash
curl -OJ "http://localhost:8000/amplitude/export?start=20240201&end=20240207&source=web"
```

**.env config:**
```env
AMPLITUDE_WEB_SECRET_KEY="your-web-secret-key"
AMPLITUDE_WEB_CLIENT_ID="your-web-client-id"
AMPLITUDE_MOBILE_SECRET_KEY="your-mobile-secret-key"
AMPLITUDE_MOBILE_CLIENT_ID="your-mobile-client-id"
```

---

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

### Test batch operations
```bash
# Upload batch of 1500 records (will chunk into batches)
curl -X POST http://localhost:8000/dwh/user-locations \
  -H "Content-Type: application/json" \
  -d @large_batch.json
```

---

## Features

✅ **Modular Configuration** - Nested DWHSettings, AppMetricaSettings, LoggingSettings, S3Settings  
✅ **Batch Operations** - Automatic chunking respects DB write limits and row limits  
✅ **S3 Integration** - Full CRUD operations with AWS S3 and S3-compatible services  
✅ **AppMetrica Integration** - Async export polling with configurable timeouts  
✅ **7 DWH Tables** - Pre-configured schemas for product analytics data  
✅ **Simplified API** - Clean response format without wrapper objects  
✅ **Error Handling** - Proper HTTP status codes and error messages  
✅ **OpenAPI Support** - Full Swagger/ReDoc documentation  

---

## Environment Variables Reference

See `.env.example` for all available configuration options.

Key points:
- Use `env_nested_delimiter="_"` format: `DWH_DATABASE_URL`, `S3_ACCESS_KEY_ID`, etc.
- All settings loaded automatically from `.env` on startup
- Settings accessible via `from app.config.settings import settings`
- Extra environment variables ignored gracefully

---

## Запуск локально

1. Установите Poetry и зависимости:

```bash
pip install poetry
poetry install
```

2. Скопируйте и настройте переменные окружения:

```bash
cp .env.example .env
# Отредактируйте .env под свои ключи и параметры
```

3. Запустите сервер:

```bash
poetry run uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

4. Для production:

```bash
poetry run uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## Docker

1. Соберите образ:

```bash
docker build -t dwh-helper .
```

2. Запустите контейнер:

```bash
docker run --env-file .env -p 8000:8000 dwh-helper
```

---

## GitLab/GitHub Actions и деплой

- Пример workflow для деплоя: `.github/workflows/deploy.yaml`
- Секреты и переменные окружения должны обновляться через CI/CD и .env
- Для production рекомендуется запуск через systemd (см. deploy.md)

---

## Production: запуск через systemd

1. Создайте unit-файл для systemd:

```bash
sudo nano /etc/systemd/system/fastapi-app.service
```

2. Пример содержимого:

```
[Unit]
Description=FastAPI app for DWH Helper
After=network.target

[Service]
User=root
WorkingDirectory=/root/code/amplitude_downloader  # путь к проекту
ExecStart=/usr/bin/env poetry run uvicorn app.main:app --host 0.0.0.0 --port 8000
Restart=always
Environment="PATH=/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

[Install]
WantedBy=multi-user.target
```

3. Перезагрузите systemd и запустите сервис:

```bash
sudo systemctl daemon-reload
sudo systemctl start fastapi-app
sudo systemctl enable fastapi-app
```

4. Для перезапуска после обновления кода или переменных окружения:

```bash
sudo systemctl restart fastapi-app
```

5. Проверить статус:

```bash
sudo systemctl status fastapi-app
```

6. Логи приложения:

```bash
journalctl -u fastapi-app -f
```

---

## Production: запуск через Docker и systemd

1. На сервере должен быть установлен Docker.
2. Скопируйте .env в /root/.env (или другой путь, используемый в docker run).
3. Пример systemd unit-файла для контейнера:

```ini
[Unit]
Description=DWH Helper Docker container
After=docker.service
Requires=docker.service

[Service]
Restart=always
ExecStartPre=-/usr/bin/docker stop dwh-helper
ExecStartPre=-/usr/bin/docker rm dwh-helper
ExecStartPre=/usr/bin/docker pull <ВАШ_РЕГИСТРИ_ИМЯ_ОБРАЗА>
ExecStart=/usr/bin/docker run --name dwh-helper --env-file /root/.env -p 8000:8000 <ВАШ_РЕГИСТРИ_ИМЯ_ОБРАЗА>
ExecStop=/usr/bin/docker stop dwh-helper

[Install]
WantedBy=multi-user.target
```

4. После обновления образа или переменных окружения:

```bash
sudo systemctl restart fastapi-app
```

---
