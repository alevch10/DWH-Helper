# Модуль интеграции с Amplitude
> *Версия модуля: 1.0.0*  
## 📌 Описание
Модуль предназначен для выгрузки сырых событий из **Amplitude** и их упаковки в удобный для дальнейшей загрузки в DWH формат.  
Поддерживает два источника данных: **Web** и **Mobile** (разные наборы учётных данных).  

**Основные возможности:**
- Получение данных за произвольный диапазон дат (от 1 дня до нескольких недель).
- Автоматическая распаковка ZIP‑архивов от Amplitude и сжатых GZ‑файлов.
- Объединение всех событий в один **NDJSON**‑файл (каждая строка – отдельное событие JSON).
- Упаковка результата в ZIP‑архив и отдача через FastAPI эндпоинт.
- Фоновая очистка временных файлов.

---

## 🧩 Требования к модулю
1. Модуль должен иметь свой API‑эндпоинт, использующий все необходимые запросы к Amplitude.
2. Модуль должен выполнять HTTP‑запросы к Amplitude с токеном (Basic Auth), полученным из переменных окружения.
3. Поддерживаются **две пары** учётных данных:
   - для **Web** – `AMPLITUDE_WEB_CLIENT_ID`, `AMPLITUDE_WEB_SECRET_KEY`
   - для **Mobile** – `AMPLITUDE_MOBILE_CLIENT_ID`, `AMPLITUDE_MOBILE_SECRET_KEY`
4. Эндпоинт должен позволять выбрать источник (`source`) – `web` или `mobile`.
5. Запрос к Amplitude выполняется по `start` и `end` в формате `YYYYMMDDTHH`. На вход эндпоинт принимает даты в формате `YYYYMMDD`, часы фиксируются:
   - `start` → `00`
   - `end` → `23`
6. Ответ от Amplitude – `.zip` архив, внутри до 24 `.gz` файлов (по одному на час), каждый содержит строки NDJSON.
7. **Обработка ответа:**
   - Скачать архив.
   - Распаковать и извлечь все `.gz` файлы.
   - Распаковать каждый `.gz` в строки NDJSON.
   - Склеить все строки за запрошенный период в единый NDJSON‑файл.
   - Упаковать NDJSON в ZIP‑архив с именем `amplitude_export_{start}_{end}.zip`.
   - Отдать ZIP пользователю и удалить временные файлы после отправки.

---

## ⚙️ Конфигурация (переменные окружения)

Все настройки задаются через переменные окружения или `.env` файл.

| Переменная                         | Описание                                    | Обязательная |
|------------------------------------|---------------------------------------------|--------------|
| `AMPLITUDE_WEB_CLIENT_ID`          | Client ID для Web‑источника                 | ✅           |
| `AMPLITUDE_WEB_SECRET_KEY`         | Secret Key для Web‑источника                | ✅           |
| `AMPLITUDE_MOBILE_CLIENT_ID`       | Client ID для Mobile‑источника              | ✅           |
| `AMPLITUDE_MOBILE_SECRET_KEY`      | Secret Key для Mobile‑источника             | ✅           |

> **Примечание:** Если переменные для выбранного `source` отсутствуют, клиент выбросит исключение.

Пример `.env`:
```env
AMPLITUDE_WEB_CLIENT_ID=abc123
AMPLITUDE_WEB_SECRET_KEY=xyz789
AMPLITUDE_MOBILE_CLIENT_ID=mob123
AMPLITUDE_MOBILE_SECRET_KEY=mob789
```

---

## 🔌 API эндпоинт

### `GET /amplitude/export`

Выгружает события Amplitude за указанный диапазон дат и возвращает ZIP‑архив с NDJSON.

#### Параметры запроса (Query)

| Параметр | Тип                          | Описание                                                        | По умолчанию |
|----------|------------------------------|-----------------------------------------------------------------|--------------|
| `start`  | string (YYYYMMDD)            | Начальная дата включительно (например `20240201`)               | **обязателен** |
| `end`    | string (YYYYMMDD)            | Конечная дата включительно                                      | **обязателен** |
| `source` | `web` или `mobile`           | Источник данных                                                 | `web`        |

#### Требования к авторизации
Эндпоинт защищён – требует валидный JWT‑токен с правом `read` (см. `config.auth.md`).

#### Ответ (200 OK)

- **Content-Type:** `application/zip`
- **Content-Disposition:** attachment; filename="amplitude_export_{start}_{end}.zip"
- **Тело:** ZIP‑архив, содержащий один файл `amplitude_export_{start}_{end}.ndjson`

Пример имени файла: `amplitude_export_20240201_20240207.zip`

#### Пример запроса (cURL)

```bash
curl -X GET "http://localhost:8000/amplitude/export?start=20240201&end=20240207&source=mobile" \
  -H "Authorization: Bearer <your_token>" \
  --output export.zip
```

#### Пример ответа в Swagger

В Swagger UI (`/docs`) параметры будут представлены как:
- `start` – пример `20240201`, описание "Start date (YYYYMMDD)"
- `end` – пример `20240207`
- `source` – выпадающий список `web`/`mobile`

---

## 🧱 Архитектура модуля

```
app/amplitude/
├── client.py          # AmplitudeClient – работа с API, итератор по событиям
├── export_utils.py    # create_ndjson_zip – упаковка NDJSON в ZIP
├── router.py          # FastAPI роутер с эндпоинтом /export
└── __init__.py
```

### 1. `client.py` – `AmplitudeClient`

**Назначение:**  
Выполняет HTTP‑запросы к Export API Amplitude, поддерживает выбор учётных данных, предоставляет асинхронный генератор строк событий (`iter_lines`).

**Методы:**

- `__init__(self, source: Literal["web","mobile"])` – инициализация с проверкой наличия credentials.
- `_get_auth_header()` – формирует заголовок `Authorization: Basic ...`
- `async def export(start: str, end: str) -> bytes` – запрос к API, возвращает сырой ZIP‑файл.
- `async def export_day(date_str: str) -> bytes` – выгрузка за один день (00:00–23:59).
- `async def iter_lines(start: datetime, end: datetime) -> AsyncGenerator[str, None]` – **главный метод**:
  - Итерируется по дням от `start` до `end` включительно.
  - Для каждого дня вызывает `export_day`.
  - Распаковывает ZIP, затем каждый GZ, декодирует в UTF-8 и `yield`‑ит каждую строку NDJSON.
  - Строки отдаются по одной, что позволяет экономить память.

**Пример использования (вне роутера):**

```python
from app.amplitude.client import AmplitudeClient
from datetime import datetime, timedelta

client = AmplitudeClient(source="web")
start = datetime(2024, 2, 1)
end = datetime(2024, 2, 7)

async for line in client.iter_lines(start, end):
    data = json.loads(line)
    print(data["event_type"])
```

### 2. `export_utils.py` – `create_ndjson_zip`

**Назначение:**  
Принимает асинхронный генератор строк (например, от `iter_lines`), записывает все строки в временный NDJSON‑файл, затем упаковывает его в ZIP и возвращает путь к этому ZIP‑файлу.

**Сигнатура:**

```python
async def create_ndjson_zip(
    lines_iterator: AsyncGenerator[str, None],
    archive_name: str,      # имя итогового ZIP (не используется напрямую)
    ndjson_filename: str,   # имя файла внутри архива
) -> str:                   # возвращает путь к созданному ZIP
```

**Детали реализации:**
- Создаёт `TemporaryDirectory()` для NDJSON.
- Записывает строки (каждая + `\n`).
- Упаковывает NDJSON в ZIP (сжатие `ZIP_DEFLATED`).
- Копирует ZIP в **persistent** временный файл (переживает закрытие контекста).
- Возвращает путь к этому файлу.
- После отправки ответа FastAPI должен удалить файл (см. `background_tasks`).

### 3. `router.py` – эндпоинт `/export`

**Логика работы:**
1. Парсит `start`, `end` (YYYYMMDD) → `datetime`.
2. Создаёт `AmplitudeClient(source=...)`.
3. Получает `iter_lines(start_dt, end_dt)`.
4. Вызывает `create_ndjson_zip` с этим генератором.
5. Добавляет фоновую задачу на удаление ZIP‑файла после отправки.
6. Возвращает `FileResponse` с ZIP.

**Обработка ошибок:**
- Неверный формат даты → `400 Bad Request`.
- Ошибки Amplitude (например, неверный токен, превышение лимитов) → пробрасываются как HTTPException (500 или 4xx от httpx).
- Отсутствие переменных окружения для выбранного `source` → `ValueError` преобразуется в `500`.

---

## 🧪 Тестирование

*(Рекомендуемый раздел, если вы добавите тесты позже)*

Пример теста для `AmplitudeClient` (используя `pytest` и `respx` для перехвата HTTP):

```python
import respx
from httpx import Response
from app.amplitude.client import AmplitudeClient

@respx.mock
async def test_export_day():
    mock_route = respx.get("https://amplitude.com/api/2/export").mock(
        return_value=Response(200, content=b"fake_zip_bytes")
    )
    client = AmplitudeClient(source="web")
    result = await client.export_day("20240201")
    assert result == b"fake_zip_bytes"
```

---

## 🚀 Интеграция с приложением

Модуль уже зарегистрирован в главном FastAPI‑приложении (предполагается, что в `main.py` подключен роутер):

```python
from app.amplitude.router import router as amplitude_router

app.include_router(amplitude_router, prefix="/amplitude", tags=["Amplitude"])
```

---

## 📦 Зависимости (уже в `pyproject.toml`)

- `httpx` – асинхронные HTTP‑запросы
- `python-multipart` – для работы с form‑данными (опционально)
- `pyyaml` – для конфигов (опционально)
- `boto3` – для S3 (используется в других модулях)
- Остальные – стандартный стек FastAPI, pydantic, uvicorn

---

## 🐞 Логирование

Модуль использует структурированный логгер из `app.config.logger`.  
Ключевые события логируются с уровнем `DEBUG` и `INFO`:
- Начало и завершение выгрузки за день.
- Количество найденных GZ‑файлов.
- Количество распакованных строк.
- Ошибки при неудачных запросах.

Пример вывода:
```
🔍 Экспорт за день 20240201
📁 Найдены файлы в GZ: ['20240201_00.gz', ...]
✅ День 20240201 распакован, событий: 1250
```

---

## 🧠 Заметки для разработчиков и ИИ‑агентов

- **NDJSON формат:** каждая строка – валидный JSON. При загрузке в DWH можно использовать построчное чтение, не загружая весь файл в память.
- **Таймаут:** В `AmplitudeClient.export` установлен `timeout=2000` секунд – достаточно для больших выгрузок.
- **Фоновая очистка:** Файл удаляется через `BackgroundTasks` FastAPI. Если отправка не удалась (ошибка до возврата `FileResponse`), временный файл может остаться – для production стоит добавить try/finally или отдельный шедулер очистки.
- **Расширение:** Если понадобится сохранять результат в S3 (вместо прямой отдачи клиенту), можно заменить `create_ndjson_zip` на запись в S3 bucket.
- **Параллельная обработка:** При очень больших диапазонах (например, месяц) можно модифицировать `iter_lines` для конкурентных запросов по дням – сейчас дни обрабатываются последовательно.

---

## 📄 Связанные документы

- [Общая архитектура DWH Helper](../requirements.md)
- [Настройка переменных окружения](../config/config.md)
- [Документация по авторизации (JWT)](../config/auth.md)

