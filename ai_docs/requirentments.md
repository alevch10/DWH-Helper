# Описание
Python приложение для работы с данными продуктовой аналитики. 
Цель приложения: собирать данные из разных систем и приводить их в удобный формат для хранения внутри DWH компании. 

# О приложении:
Название: DWH Helper
Описание: Приложение для работы с данными продуктовой аналитики. 
Версия: 1.0.0
Создатель: Андрей Левченко aa.levchenko@severmed.com

---

# STATUS: ✅ PRODUCTION READY

## Статус реализации
- ✅ Инфраструктура приложения (Poetry, Python, FastAPI, Pydantic)
- ✅ Система конфигурации с вложенными блоками (DWH, AppMetrica, S3, Logging)
- ✅ 7 DWH таблиц с GET/POST эндпоинтами
- ✅ Поддержка batch операций с автоматическим chunking
- ✅ Упрощенные GET ответы без wrapper объектов
- ✅ S3 модуль с полным CRUD функционалом (AWS S3, MinIO, S3-compatible)
- ✅ Интеграция с AppMetrica API
- ✅ Логирование и мониторинг
- ✅ Полное тестирование всех эндпоинтов

---

# Технические требования (реализованные)

1) ✅ Менеджер пакетов Poetry - последней версии
2) ✅ ЯП: Python 3.14
3) ✅ Фреймворк FastAPI - последней версии
4) ✅ Pydantic v2+ - для валидации схем
5) ✅ pydantic-settings - для управления конфигурацией
6) ✅ .env файл и переменные окружения
7) ✅ Структура кода оптимизирована для поддержки ИИ агентами

---

# Архитектура приложения

## Структура конфигурации (модульная)
```
Settings (main)
├── DWHSettings
│   ├── database_url (SQLite/PostgreSQL)
│   ├── max_write_batch_bytes (default: 10MB)
│   └── max_rows_per_insert (default: 1000)
├── AppMetricaSettings
│   ├── base_url
│   ├── api_key
│   ├── application_id
│   ├── poll_interval_seconds (default: 5)
│   └── poll_timeout_seconds (default: 300)
├── S3Settings
│   ├── access_key_id
│   ├── secret_access_key
│   ├── region (default: us-east-1)
│   ├── endpoint_url (optional для S3-compatible сервисов)
│   └── bucket_name (default: default-bucket)
└── LoggingSettings
    ├── level (default: INFO)
    └── format
```

## Модули приложения

### 1. config (`app/config/`)
- `settings.py` - Вложенная архитектура конфигурации с BaseSettings
  - `DWHSettings` - параметры базы данных и batch операций
  - `AppMetricaSettings` - конфигурация для AppMetrica API
  - `S3Settings` - параметры подключения к S3 хранилищу
  - `LoggingSettings` - параметры логирования
  - `Settings` - главный класс, объединяющий все блоки
- `logger.py` - Инициализация логирования с параметром level

### 2. dwh_tables_worker (`app/dwh_tables_worker/`)
Работает с 7 таблицами DWH:
1. events_part - События пользователей с информацией о девайсе/сессии
2. mobile_devices - Метаданные мобильных устройств
3. permanent_user_properties - Статические свойства пользователя
4. technical_data - Техническая информация от Amplitude
5. tmp_event_properties - Свойства событий в формате JSON
6. tmp_user_properties - Свойства пользователя (постоянные и временные)
7. user_locations - Геолокационные данные пользователя

**GET эндпоинты** - возвращают упрощенный ответ:
```json
{
  "rows": [...],
  "count": N
}
```

**POST эндпоинты** - поддерживают batch операции:
- Принимают: `{data: [...]}`
- Возвращают: `{inserted_ids: [...], count: N, batches: N}`
- Автоматический chunking по:
  - max_rows_per_insert (1000 строк максимум)
  - max_write_batch_bytes (10MB максимум)

### 3. appmetrica (`app/appmetrica/`)
- Асинхронный клиент для API AppMetrica
- Поддержка polling экспорта данных
- Конфигурируемые timeout и interval

### 4. s3 (`app/s3/`)
AWS S3 и S3-compatible хранилище для управления объектами.

**Файлы модуля:**
- `client.py` - Boto3 S3 клиент с методами:
  - `list_objects(prefix)` - листинг файлов с префиксом (отсортированы по дате, только прямые файлы)
  - `get_object(key)` - скачивание файла
  - `put_object(key, data, content_type)` - загрузка/обновление файла
  - `delete_object(key)` - удаление файла
  - `patch_object(key, data, offset)` - патч объекта
  - `object_exists(key)` - проверка существования
  - `get_object_size(key)` - размер файла

- `router.py` - FastAPI эндпоинты (полный CRUD):
  - **GET** `/s3/objects` - листинг файлов
  - **GET** `/s3/object-info` - информация об объекте
  - **GET** `/s3/download` - скачивание файла
  - **POST** `/s3/upload` - загрузка файла
  - **PUT** `/s3/update` - обновление файла
  - **PATCH** `/s3/patch` - патч файла
  - **DELETE** `/s3/delete` - удаление файла

**Конфигурация (S3Settings):**
```python
class S3Settings(BaseModel):
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    region: str = "us-east-1"
    endpoint_url: Optional[str] = None  # Для S3-compatible сервисов
    bucket_name: str = "default-bucket"
```

**Примеры использования:**

Листинг файлов:
```bash
curl "http://127.0.0.1:8000/s3/objects?prefix=exports/"
```

Загрузка файла:
```bash
curl -X POST "http://127.0.0.1:8000/s3/upload?key=data/file.csv" \
  -F "file=@local_file.csv"
```

Проверка существования и размера:
```bash
curl "http://127.0.0.1:8000/s3/object-info?key=data/file.csv"
```

---

# Завершённые улучшения

## 1. Упрощение response структуры
**Было:**
```json
{
  "id": "wrapper",
  "pk": 123,
  "created_at": "...",
  "data": {...}
}
```

**Стало:**
```json
{
  "rows": [{...}],
  "count": N
}
```

## 2.Вложенная архитектура конфигурации
Переход от плоской структуры к модульной с блоками DWHSettings, AppMetricaSettings, LoggingSettings.
- Лучшая организация
- Избежание circular imports
- Graceful обработка старых переменных окружения (extra="ignore")

## 3. Batch операции с автоматическим chunking
```python
# POST /dwh/user-locations
{
  "data": [
    {"uuid": "...", "latitude": 50.0, "longitude": 10.0, ...},
    {"uuid": "...", "latitude": 51.0, "longitude": 11.0, ...},
    ...
  ]
}

# Response
{
  "inserted_ids": [1, 2, 3, ...],
  "count": 1500,
  "batches": 2  # chunked because 1500 > max_rows_per_insert (1000)
}
```

## 4. PostgreSQL write protection
- Защита от превышения лимитов записи
- Конфигурируемые лимиты в DWHSettings
- Автоматическое разбиение на batch'и при превышении

---

# Проверка функциональности

## Тестирование batch операций
- ✅ Small batch (3 records): `count: 3, batches: 1`
- ✅ Medium batch (1500 records): `count: 1500, batches: 1`
- ✅ Large batch (2500 records): `count: 2500, batches: 2` (chunking triggered)
- ✅ Empty data validation: возвращает 400 ошибку

## Тестирование GET операций
- ✅ Все 7 эндпоинтов возвращают упрощенную структуру
- ✅ Отсутствуют ненужные wrapper поля
- ✅ Корректный count и rows array

## Тестирование конфигурации
- ✅ Settings загружаются без ошибок
- ✅ Вложенные блоки доступны со всех модулей
- ✅ No circular imports
- ✅ Graceful env var handling

## Тестирование S3 интеграции
- ✅ S3 клиент инициализируется с учетом settings
- ✅ Все S3 эндпоинты доступны по префиксу `/s3`
- ✅ Список объектов возвращается отсортированный по дате
- ✅ Метаданные объекта доступны (размер, существование)
- ✅ Загрузка файлов работает с multipart/form-data

---

# Команды для работы

```bash
# Установка зависимостей
poetry install

# Запуск сервера (development)
poetry run uvicorn app.main:app --reload --host 127.0.0.1 --port 8000

# Запуск тестов
poetry run pytest

# Проверка типов
poetry run mypy app/
```

---

# Следующие этапы (TODO)

1. Документация API (OpenAPI/Swagger доступен по `/docs`)
2. Интеграционные тесты с реальной БД
3. Метрики производительности для batch операций
4. Graceful shutdown handler
5. Health check эндпоиент
6. Добавить compression для больших batch операций
7. S3 pagination для больших наборов файлов
8. S3 multipart upload для больших файлов
9. S3 bucket lifecycle policies management
10. Unit tests для S3 клиента и эндпоинтов

---

# Интеграция S3 (ЗАВЕРШЕНО)

## Статус
✅ Модуль `s3` создан и интегрирован в приложение

## Компоненты
- ✅ `app/s3/client.py` - Boto3 S3 клиент с полным CRUD функционалом
- ✅ `app/s3/router.py` - FastAPI роутер с 8 эндпоинтами (LIST, GET, POST, PUT, PATCH, DELETE)
- ✅ `app/s3/__init__.py` - Модульная архитектура
- ✅ `S3Settings` - Конфигурация в settings.py
- ✅ Интеграция в `app/main.py` - S3 роутер подключен на префиксе `/s3`
- ✅ Переменные окружения - S3_* секции в `.env` и `.env.example`
- ✅ Зависимости - boto3 добавлен в pyproject.toml

## Использование
```bash
# Листинг файлов
curl http://127.0.0.1:8000/s3/objects?prefix=exports/

# Загрузка файла
curl -X POST http://127.0.0.1:8000/s3/upload?key=data/file.csv -F "file=@file.csv"

# Скачивание файла
curl http://127.0.0.1:8000/s3/download?key=data/file.csv

# Информация об объекте
curl http://127.0.0.1:8000/s3/object-info?key=data/file.csv

# Удаление файла
curl -X DELETE http://127.0.0.1:8000/s3/delete?key=data/file.csv
```

## Конфигурация
Скопировать в `.env`:
```env
S3_ACCESS_KEY_ID="your-key"
S3_SECRET_ACCESS_KEY="your-secret"
S3_REGION="us-east-1"
S3_ENDPOINT_URL="https://s3.amazonaws.com"
S3_BUCKET_NAME="your-bucket"
```

Работает с:
- ✅ AWS S3
- ✅ MinIO
- ✅ DigitalOcean Spaces
- ✅ Любое S3-compatible хранилище
