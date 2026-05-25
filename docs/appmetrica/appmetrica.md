# Модуль интеграции с AppMetrica
> *Версия модуля: 1.0.0*
## 📌 Описание
Модуль предназначен для выгрузки событий из **AppMetrica** (Logs API) и их доставки в виде ZIP‑архива, содержащего CSV или JSON файл.  
Поддерживает:
- Асинхронный опрос (polling) статуса подготовки экспорта.
- Гибкий выбор полей (`fields`) и периода (`date_since`, `date_until`).
- Преобразование поля `event_json` в JSON‑объект (в JSON‑выгрузке).
- Авторизацию через OAuth‑токен, передаваемый в заголовке запроса к нашему API.

**Основные возможности:**
- Запуск экспорта событий за заданный интервал.
- Ожидание готовности данных с выводом прогресса (через повторные запросы).
- Получение результата в формате **CSV** (по умолчанию) или **JSON**.
- Автоматическая упаковка файла в ZIP‑архив и отдача через FastAPI.

---

## 🧩 Требования к модулю
(из исходного технического задания)

1. Модуль должен иметь свой API‑эндпоинт, реализующий все необходимые запросы к AppMetrica.
2. Авторизация в AppMetrica – заголовок `Authorization: OAuth {token}`.
3. При запросе к AppMetrica возвращается код `202 Accepted` (поставлено в очередь). Наш API должен периодически (каждые 10 секунд) повторять запрос, пока не получит `200 OK` или не истечёт таймаут.
4. Параметры запроса:
   - `application_id` – берётся из конфига, может быть переопределён в query.
   - `skip_unavailable_shards` – query, по умолчанию `false`.
   - `date_since`, `date_until` – обязательные для пользователя (формат `YYYY-MM-DD HH:MM:SS`).
   - `date_dimension` – query, по умолчанию `default`.
   - `use_utf8_bom` – query, по умолчанию `true`.
   - `fields` – список полей через запятую. Если не указан – используется фиксированный набор всех перечисленных полей.
   - `export_format` – `csv` (по умолчанию) или `json`.
5. Токен AppMetrica (`api_key`) – хранится в переменной окружения `APPMETRICA_API_KEY` - берется из заголовка `Authorization` нашего API.
6. Обработка ответа:
   - Если экспорт готов – сервис возвращает ZIP‑архив с файлом `appmetrica_resp.csv` или `appmetrica_resp.json`.
   - В процессе ожидания (polling) – возвращать промежуточный статус (см. ниже).
   - При запросе, отличном от скачивания (в перспективе), можно возвращать Pydantic‑схему, но в текущей реализации – только архив.

---

## ⚙️ Конфигурация (переменные окружения)

Все настройки задаются через переменные окружения или `.env` файл.

| Переменная                              | Описание                                                                 | Обязательная |
|-----------------------------------------|--------------------------------------------------------------------------|--------------|
| `APPMETRICA_BASE_URL`                   | Базовый URL API AppMetrica (обычно `https://api.appmetrica.yandex.ru`)   | ✅           |
| `APPMETRICA_APPLICATION_ID`             | ID приложения в AppMetrica (по умолчанию)                                | ✅           |
| `APPMETRICA_POLL_INTERVAL_SECONDS`      | Интервал опроса готовности в секундах (по умолчанию `10`)                | ❌           |
| `APPMETRICA_POLL_TIMEOUT_SECONDS`       | Максимальное время ожидания экспорта в секундах (по умолчанию `300`)     | ❌           |

Пример `.env`:
```env
APPMETRICA_BASE_URL=https://api.appmetrica.yandex.ru
APPMETRICA_APPLICATION_ID=473434
APPMETRICA_POLL_INTERVAL_SECONDS=10
APPMETRICA_POLL_TIMEOUT_SECONDS=300
APPMETRICA_API_KEY=my_super_oauth_token
```

---

## 🔌 API эндпоинт

### `GET /appmetrica/export`

Запускает экспорт событий из AppMetrica и возвращает ZIP‑архив с результатом (CSV или JSON).

#### Параметры запроса (Query)

| Параметр                 | Тип        | Описание                                                                          | По умолчанию                      |
|--------------------------|------------|-----------------------------------------------------------------------------------|-----------------------------------|
| `application_id`         | string     | ID приложения (если не указан – из конфига)                                       | `APPMETRICA_APPLICATION_ID`       |
| `skip_unavailable_shards`| boolean    | Пропускать недоступные шарды                                                      | `false`                           |
| `date_since`             | string     | **Начало периода** (формат `YYYY-MM-DD HH:MM:SS`)                                 | обязателен                        |
| `date_until`             | string     | **Конец периода** (формат `YYYY-MM-DD HH:MM:SS`)                                  | обязателен                        |
| `date_dimension`         | string     | Измерение даты (`default`, `hour`, `day`, `month`)                                | `default`                         |
| `use_utf8_bom`           | boolean    | Добавлять BOM в UTF‑8 файлы                                                       | `true`                            |
| `fields`                 | string     | Список полей через запятую (см. полный список ниже). Если не указан – все поля.   | все поля                          |
| `export_format`          | string     | `csv` или `json`                                                                  | `csv`                             |

#### Полный список полей (по умолчанию)
```
app_build_number,profile_id,os_name,os_version,device_manufacturer,device_model,device_type,device_locale,device_ipv6,app_version_name,event_name,event_json,connection_type,operator_name,country_iso_code,city,appmetrica_device_id,installation_id,session_id,event_datetime
```

#### Авторизация
Наш эндпоинт защищён JWT‑токеном (как и другие модули).  
Кроме того, **переданный в запросе Bearer‑токен** используется в качестве `api_key` для вызова AppMetrica.  
То есть клиент должен отправить:
```
Authorization: Bearer <JWT_токен_нашего_приложения>
```
Этот же токен будет подставлен в заголовок `Authorization: OAuth <токен>` при вызове AppMetrica.  
Если по каким‑то причинам токен не передан, используется значение из `APPMETRICA_API_KEY`.

> **Важно:** В текущей реализации токен для AppMetrica извлекается из заголовка `Authorization` входящего запроса (удаляя префикс `Bearer`). 

#### Ответ

**1. Пока экспорт не готов (код `202` от AppMetrica)**  
Возвращается JSON с информацией о процессе (после истечения таймаута или внутри опроса – в текущей реализации мы ждём до готовности или таймаута; при таймауте возвращается `pending`).

Пример ответа при таймауте:
```json
{
  "status": "pending",
  "detail": "Timeout while waiting for export"
}
```
> В текущей версии клиентского кода мы **не возвращаем промежуточный прогресс**, а ждём до готовности или таймаута.

**2. Экспорт готов (код `200` от AppMetrica)**  
Возвращается **ZIP‑архив** со следующими заголовками:
```
Content-Type: application/zip
Content-Disposition: attachment; filename=appmetrica_export_<date_since>_<date_until>.zip
```
Внутри архива:
- при `export_format=csv` – файл `appmetrica_resp.csv` (кодировка UTF‑8, возможен BOM)
- при `export_format=json` – файл `appmetrica_resp.json`

**Структура JSON‑файла** (пример):
```json
{
  "data": [
    {
      "app_build_number": "444",
      "profile_id": "472246",
      "os_name": "ios",
      "os_version": "18.6.2",
      "device_manufacturer": "Apple",
      "device_model": "iPhone 14 Pro",
      "device_type": "phone",
      "device_locale": "ru_RU",
      "device_ipv6": "::ffff:46.8.6.106",
      "app_version_name": "2.29.0",
      "event_name": "Banner Shown",
      "event_json": {"Title": "Что умеет приложение?", "Place": "DASHBOARD"},
      "connection_type": "cell",
      "operator_name": "",
      "country_iso_code": "RU",
      "city": "",
      "appmetrica_device_id": "13278911961594196248",
      "installation_id": "8c05f492bb564e1e98b0d15c2c893e59",
      "session_id": "10000000012",
      "event_datetime": "2026-02-09 16:20:55"
    }
  ]
}
```
Поле `event_json` уже является объектом JSON (не строкой).

**Пример cURL‑запроса** (к нашему API):
```bash
curl -X GET "http://localhost:8000/appmetrica/export?date_since=2026-02-09%2000:00:00&date_until=2026-02-09%2023:59:59&export_format=json" \
  -H "Authorization: Bearer <ваш_jwt_или_oauth_токен>" \
  --output export.zip
```

---

## 🧱 Архитектура модуля

```
app/appmetrica/
├── client.py          # AppMetricaClient – взаимодействие с Logs API, polling
├── router.py          # FastAPI роутер с эндпоинтом /export
└── __init__.py
```

### 1. `client.py` – `AppMetricaClient`

**Назначение:**  
Выполняет запрос к AppMetrica Logs API, обрабатывает асинхронную очередь (статус `202`) и возвращает готовые данные (JSON или текст CSV).

**Основные методы и атрибуты:**

- `__init__()` – загружает настройки из `settings.appmetrica`.
- `async def fetch_export(...) -> dict` – главный метод.
  - **Параметры** соответствуют параметрам API AppMetrica плюс:
    - `export_format` (`'csv'` или `'json'`) – определяет URL (`.../events.csv` или `.../events.json`).
    - `poll_timeout`, `poll_interval` – переопределяют стандартные.
    - `api_key` – OAuth токен (если не передан, используется из настроек).
  - **Логика:**
    1. Формирует URL и параметры.
    2. Отправляет GET‑запрос к AppMetrica.
    3. Если ответ `202` – начинает цикл опроса с заданным интервалом до таймаута.
    4. При `200` – возвращает `{"status": "ready", "result": data}`, где `data` – либо `dict` (JSON), либо `str` (CSV).
    5. При таймауте – возвращает `{"status": "pending", "detail": "..."}`.
    6. Другие статусы – выбрасывает исключение.

**Примечание:** Метод не выполняет упаковку в ZIP – только получение сырых данных.

### 2. `router.py` – эндпоинт `/export`

**Логика работы:**
1. Принимает параметры из query.
2. Извлекает `Authorization` заголовок, удаляет префикс `Bearer` и использует как `api_key` для `AppMetricaClient`.
3. Вызывает `client.fetch_export()`.
4. Если статус не `"ready"` (например, таймаут) – возвращает JSON с ошибкой.
5. Если данные готовы:
   - Для JSON: сериализует результат в байты (`json.dumps`).
   - Для CSV: кодирует строку в UTF‑8.
   - Создаёт ZIP‑архив в памяти (`io.BytesIO`), помещает файл с именем `appmetrica_resp.json` (или `.csv`).
   - Возвращает `StreamingResponse` с типом `application/zip`.
6. Любые исключения логируются и возвращаются как `HTTPException 500`.

**Дополнительный эндпоинт:**
- `/appmetrica/ping` – проверка работоспособности модуля (требует авторизации). Возвращает `{"module": "appmetrica", "status": "ok"}`.

---

## 🧪 Тестирование

*(Рекомендуемый раздел для будущих тестов)*

Пример асинхронного теста для `AppMetricaClient` (с использованием `respx`):

```python
import respx
from httpx import Response
from app.appmetrica.client import AppMetricaClient

@respx.mock
async def test_fetch_export_json_ready():
    mock_route = respx.get("https://api.appmetrica.yandex.ru/logs/v1/export/events.json")
    mock_route.mock(return_value=Response(200, json={"data": []}))

    client = AppMetricaClient()
    result = await client.fetch_export(
        date_since="2026-02-09 00:00:00",
        date_until="2026-02-09 23:59:59",
        export_format="json",
        api_key="test_key"
    )
    assert result["status"] == "ready"
    assert result["result"] == {"data": []}
```

---

## 🚀 Интеграция с приложением

В главном `main.py` должен быть подключён роутер:

```python
from app.appmetrica.router import router as appmetrica_router

app.include_router(appmetrica_router, prefix="/appmetrica", tags=["AppMetrica"])
```

---

## 📦 Зависимости (уже в `pyproject.toml`)

- `httpx` – асинхронные HTTP‑запросы (включая повторные попытки).
- `python-multipart` – для form‑данных (не используется, но есть в общем стеке).
- `boto3` – для S3 (другие модули).
- `psycopg2` – PostgreSQL.
- Стандартные: `fastapi`, `pydantic`, `uvicorn`.

---

## 🐞 Логирование

Логирование осуществляется через `app.config.logger`.  
Ключевые события:
- `logger.exception` при ошибках в роутере.
- В `client.py` можно добавить логирование начала опроса, прогресса (сейчас нет, рекомендуется добавить).

Пример добавляемого логирования (для будущих улучшений):
```python
logger.info(f"Export queued, starting polling (interval={poll_interval}s, timeout={poll_timeout}s)")
logger.debug(f"Polling attempt {attempt}: status {r2.status_code}")
```

---

## 🧠 Заметки для разработчиков и ИИ‑агентов

- **Формат дат:** `date_since` и `date_until` ожидаются в формате `YYYY-MM-DD HH:MM:SS` (с пробелом). В API AppMetrica они передаются как есть.
- **Поле `event_json`:** В JSON‑выгрузке оно автоматически парсится из строки в объект (метод `resp.json()`). В CSV остаётся строкой.
- **Polling:** Текущая реализация блокирует соединение на время ожидания. Для высоконагруженных сценариев можно переделать на веб‑хуки или фоновые задачи.
- **Таймауты:** Клиент AppMetrica имеет таймаут HTTP‑запроса 60 секунд (`httpx.AsyncClient(timeout=60.0)`), что достаточно для ответа при уже готовых данных. Для долгих экспортов нужно увеличить.
- **Обработка ошибок:** При коде ответа, отличном от 200 или 202, вызывается `resp.raise_for_status()`, что приведёт к `HTTPException` с соответствующим статусом (обычно 4xx или 5xx). Текст ошибки пробрасывается в ответ.
- **Совместимость с ТЗ:** Реализована возможность выбора полей (`fields`). Если параметр не указан – используются все поля из ТЗ. Параметр `skip_unavailable_shards` вынесен в query, дефолт `false` – соответствует требованию.
- **Безопасность:** Ни в коем случае не логируйте OAuth‑токены. В текущем коде они не логируются.

---

## 📄 Связанные документы

- [Общая архитектура DWH Helper](../requirements.md)
- [Настройка переменных окружения](../config/config.md)
- [Документация по авторизации (JWT)](../config/auth.md)
