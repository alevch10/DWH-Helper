# DWH Helper – системные требования и документация

## 📌 Описание
**DWH Helper** – Python-приложение для работы с данными продуктовой аналитики.  
Цель: собирать данные из различных внешних систем (Amplitude, AppMetrica, Яндекс.Метрика), трансформировать их в единый формат и загружать в корпоративное хранилище данных (DWH) на базе PostgreSQL.

Приложение предоставляет:
- REST API для выгрузки сырых событий из аналитических систем.
- Асинхронные клиенты для взаимодействия с внешними API.
- **Универсальный ETL-загрузчик** (единственный эндпоинт `/etl/transformer`), который по YAML-конфигурации выполняет:
  - Чтение данных из S3 (ZIP + NDJSON).
  - Трансформацию записей по заданному маппингу полей.
  - Batch-вставку в целевые таблицы PostgreSQL с поддержкой `ON CONFLICT` и стратегий обновления.
  - Возобновление после ошибок с точной позицией (файл, строка).
- Модуль анализа структуры данных (Data Scout) для автоматического профилирования JSON/NDJSON.
- Полноценный доступ к S3‑совместимому хранилищу.
- Аутентификацию и авторизацию через Яндекс OAuth + JWT.
- Гибкую конфигурацию через переменные окружения (Pydantic Settings).

---

## 🧰 Стек технологий

| Компонент            | Технологии                                                                 |
|----------------------|----------------------------------------------------------------------------|
| Язык                 | Python ≥3.13, <4.0                                                         |
| Dependency Manager   | Poetry                                                                     |
| Веб-фреймворк        | FastAPI (^0.101.0), uvicorn (^0.23.2)                                     |
| Валидация данных     | Pydantic (^2.7.0), pydantic-settings (^2.0.0)                             |
| HTTP-клиент          | httpx (^0.24.0)                                                            |
| Работа с S3          | boto3 (^1.34.0)                                                            |
| PostgreSQL           | psycopg2 (^2.9.11)                                                         |
| Аутентификация       | pyjwt (^2.11.0)                                                            |
| Конфигурация         | python-dotenv (^1.0.0), pyyaml (^6.0.3)                                   |
| Обработка файлов     | python-multipart (^0.0.22)                                                 |

Актуальные версии зависимостей указаны в `pyproject.toml`.

---

## 📂 Структура документации

```
docs/
├── amplitude/              # Интеграция с Amplitude API
│   └── amplitude.md        # Клиент, экспорт данных, обработка ZIP/GZ, формат ответа
├── appmetrica/             # Интеграция с AppMetrica API
│   └── appmetrica.md       # Клиент, polling, JSON/CSV, ZIP-архив
├── config/                 # Конфигурация и логирование
│   ├── config.md           # Pydantic-settings, переменные окружения, классы настроек
│   └── auth.md             # Аутентификация через Яндекс OAuth, JWT, права доступа
├── data_scout/             # Анализ структуры данных в S3
│   └── data_scout.md       # Сканирование ZIP/NDJSON, построение YAML-профиля
├── db/                     # Работа с PostgreSQL (DWH)
│   └── db.md               # Пул соединений, репозиторий, API для 8 таблиц, batch-вставка
├── etl/                    # Универсальный ETL-загрузчик
│   └── etl.md              # YAML-конфигурация, эндпоинт /etl/transformer, маппинг, стратегии
├── s3/                     # S3-совместимое хранилище
│   └── s3.md               # Клиент boto3, API (GET/POST/PUT/PATCH/DELETE), подписи запросов
├── yandex_metrica/         # Интеграция с Яндекс.Метрикой
│   ├── yandex_metrica.md   # Logs API, logrequest, построение витрин (ad_efficiency)
│   └── ym_table_creation.sql # DDL для таблиц
└── requirements.md         # Данный файл – входная точка в документацию
```

---

## 🔌 Модули приложения – краткий обзор

### 1. Amplitude Integration (`/amplitude`)
- **Назначение:** выгрузка сырых событий из Amplitude (Export API v2).
- **Эндпоинт:** `GET /amplitude/export?start=YYYYMMDD&end=YYYYMMDD&source=web|mobile`
- **Документация:** [amplitude.md](amplitude/amplitude.md)

### 2. AppMetrica Integration (`/appmetrica`)
- **Назначение:** выгрузка событий из AppMetrica (Logs API).
- **Эндпоинт:** `GET /appmetrica/export?date_since=...&date_until=...&export_format=csv|json`
- **Документация:** [appmetrica.md](appmetrica/appmetrica.md)

### 3. Yandex.Metrica Integration (`/yandex_metrika`)
- **Назначение:** загрузка логов хитов, построение маркетинговых витрин.
- **Эндпоинт:** `POST /yandex_metrika/ad_efficiency`
- **Документация:** [yandex_metrica.md](yandex_metrica/yandex_metrica.md)

### 4. Data Scout (`/scout`)
- **Назначение:** анализ структуры данных в S3.
- **Эндпоинт:** `GET /scout/data_analysis?path=<prefix>`
- **Документация:** [data_scout.md](data_scout/data_scout.md)

### 5. S3 Storage (`/s3`)
- **Назначение:** универсальный доступ к S3-совместимому хранилищу.
- **Эндпоинты:** `GET /s3/objects`, `GET /s3/download`, `POST /s3/upload`, `PUT /s3/update`, `PATCH /s3/patch`, `DELETE /s3/delete`
- **Документация:** [s3.md](s3/s3.md)

### 6. Database (DWH) (`/db`)
- **Назначение:** работа с 8 таблицами PostgreSQL.
- **Эндпоинты:** GET/POST для каждой таблицы.
- **Документация:** [db.md](db/db.md)

### 7. Универсальный ETL (`/etl`)
- **Назначение:** загрузка данных из S3 в DWH по YAML-конфигурации.
- **Единственный эндпоинт:** `POST /etl/transformer` (принимает YAML в теле с `Content-Type: application/x-yaml` или JSON-обёртку).
- **Возможности:** 
  - Чтение ZIP-архивов с NDJSON.
  - Маппинг полей (включая вложенные JSON и точечные пути).
  - Batch-вставка с автоматическим чанкованием и дедупликацией.
  - Поддержка `ON CONFLICT` и стратегий обновления (`always` / `on_change`).
  - Возобновление после ошибок (параметры `start_after_file`, `start_after_line`).
- **Документация:** [etl.md](etl/etl.md)

### 8. Authentication (`/auth` – сквозная)
- **Назначение:** аутентификация через Яндекс OAuth, разграничение прав READ/WRITE.
- **Документация:** [auth.md](config/auth.md)

### 9. Configuration & Logging (`config`)
- **Назначение:** управление настройками (Pydantic Settings) и логированием.
- **Документация:** [config.md](config/config.md)

---

## ⚙️ Конфигурация (переменные окружения)

Все настройки загружаются из файла `.env` (пример – `.env.example`).  
Основные группы:

| Группа          | Назначение                                   |
|-----------------|----------------------------------------------|
| `db`            | Подключение к PostgreSQL                     |
| `s3`            | S3-хранилище                                 |
| `amplitude`     | Учётные данные Amplitude (web + mobile)      |
| `appmetrica`    | Базовый URL, application_id, таймауты        |
| `yandexmetrica` | Настройки Яндекс.Метрики                     |
| `yandex`        | OAuth client_id/secret для аутентификации    |
| `logging`       | Уровень логирования                          |
| `etl`           | Параметры ETL (batch_size, query_remove)     |
| `auth`          | Списки доступа (READ_ACCESS, WRITE_ACCESS)   |

Полные списки переменных – в [config.md](config/config.md).

---

## 🚀 Запуск и разработка

### Локально (без Docker)
```bash
pip install poetry
poetry install
cp .env.example .env   # отредактируйте .env
poetry run uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

### В Docker
```bash
docker compose -f docker-compose.local.yml up -d --build
```

### Проверка синтаксиса
```bash
poetry run python -m py_compile app/**/*.py
```

### Загрузка конфигурации
```bash
poetry run python -c "from app.config.settings import settings; print(settings)"
```

---

## 📚 Для разработчиков и ИИ‑агентов

- **Перед добавлением нового функционала** ознакомьтесь с существующими модулями и их документацией.
- **Все внешние вызовы** должны использовать асинхронные клиенты (`httpx.AsyncClient`).
- **Для работы с БД** используйте `DBRepository` (синглтон через `get_repository()`).
- **Для S3** используйте `S3Client`.
- **Логирование** – через `get_logger(__name__)`.
- **Добавление новой таблицы** в DWH: создайте Pydantic-модель в `db/schemas.py`, добавьте запись в `TABLE_MODEL_MAP` в `repository.py`, реализуйте эндпоинты в `router.py`.
- **Универсальный ETL** – вся логика загрузки описывается в YAML-конфиге. Не нужно писать новый код для каждого источника.
