## Документация модуля DB (работа с DWH)
> *Версия модуля: 1.0.0*  
### 📌 Описание
Модуль **DB** обеспечивает надёжное подключение к PostgreSQL‑базе данных (DWH), выполнение запросов на чтение и запись, а также предоставляет **REST API** для основных таблиц.  
Он используется как напрямую из других модулей (через `DBRepository`), так и через HTTP‑эндпоинты, что позволяет внешним системам загружать и выгружать данные.

**Основные возможности:**
- Пул соединений `ThreadedConnectionPool` с автокоммитом (каждое изменение фиксируется сразу).
- Поддержка **batch‑вставки** с автоматическим разбиением на чанки (учитывается лимит параметров PostgreSQL – 65 535).
- **UPSERT** через `ON CONFLICT DO UPDATE` (для таблицы `changeable_user_properties`).
- **Безопасная параметризация** запросов (защита от SQL‑инъекций).
- **GET‑эндпоинты** с фильтрацией по первичному ключу, сортировкой, лимитом и дополнительными условиями (например, `migrated` для `tmp_user_properties`).
- **POST‑эндпоинты** принимают пакеты объектов (batch), возвращают список вставленных идентификаторов и количество использованных батчей.
- **Специфические методы** репозитория для сложной логики (получение всех `ehr_id` из `permanent_user_properties`, последних изменяемых свойств для списка `ehr_id`, пометка строк `tmp_user_properties` как обработанных).

---

## 🧩 Требования к модулю (чек-лист выполнения)

### Общие
- Подключение к PostgreSQL через переменные из `config.settings`.
- Использование пула соединений (`ThreadedConnectionPool`).
- Все SQL‑запросы используют параметризацию (`%s` или `psycopg2.sql`).
- Режим автокоммита после каждого запроса.
- Логирование через `app.config.logger`.

### API (router)
- Для каждой из 8 основных таблиц есть **POST** и **GET** эндпоинты:
  - `events_part`
  - `mobile_devices`
  - `permanent_user_properties`
  - `changeable_user_properties`
  - `technical_data`
  - `tmp_event_properties`
  - `tmp_user_properties`
  - `user_locations`
- GET поддерживает фильтр по первичному ключу (`pk`), `limit`, `sort_by`, `sort_dir`.
- GET для `/user-properties` дополнительно фильтрует по `migrated`.
- POST принимает батч объектов (в теле), автоматически чанкует.
- POST возвращает `BatchInsertResponse` – список вставленных ID (`inserted_ids`), количество строк (`count`) и количество батчей (`batches`).

### Схемы (schemas)
- Все таблицы описаны Pydantic‑моделями.
- Поля `event_time` имеют тип `datetime` со строгой валидацией ISO 8601.
- Невалидные даты вызывают HTTP 422 (автоматически через `field_validator`).
- Использованы `@field_validator(mode='before')` для преобразования строк.

### Репозиторий (repository)
- Общие методы: `insert_one`, `insert_batch`, `select`, `get_by_pk`.
- `insert_batch` автоматически разбивает данные по протокольному лимиту PostgreSQL (65 535 параметров).
- Динамический расчёт `max_rows` на основе количества полей в модели и настройки `max_rows_per_insert`.
- `RETURNING` для получения ID вставленных записей.
- Подсчёт батчей и возврат вместе с ID.
- Поддержка `ON CONFLICT` (`DO NOTHING` / `DO UPDATE`).
- **Новое: поддержка внешних транзакций** – методы `insert_batch` и `upsert_batch` принимают опциональный параметр `conn`.  
  Если передан, запрос выполняется в контексте этого соединения, и соединение не возвращается в пул.  
  Для управления транзакцией добавлены:
  - `get_raw_connection()` – получить соединение с `autocommit = False`.
  - `commit(conn)` – зафиксировать и вернуть соединение в пул.
  - `rollback(conn)` – откатить и вернуть соединение в пул.
- Обратная совместимость: при вызове без `conn` поведение полностью идентично предыдущим версиям.

---

## ⚙️ Конфигурация (переменные окружения)

Модуль использует настройки из `settings.db` (задаются через переменные окружения).

| Переменная               | Описание                                        | Пример                |
|--------------------------|-------------------------------------------------|-----------------------|
| `DB_NAME`                | Имя базы данных                                 | `dwh`                 |
| `DB_USER`                | Пользователь                                    | `postgres`            |
| `DB_PASSWORD`            | Пароль                                          | `secret`              |
| `DB_HOST`                | Хост                                            | `localhost`           |
| `DB_PORT`                | Порт                                            | `5432`                |
| `DB_MINCONN`             | Минимум соединений в пуле                       | `1`                   |
| `DB_MAXCONN`             | Максимум соединений в пуле                      | `10`                  |
| `DB_MAX_PARAMS_PER_QUERY`| Максимум параметров в одном запросе (лимит PG)  | `65535`               |
| `DB_MAX_ROWS_PER_INSERT` | Жёсткий лимит строк в одном INSERT              | `10000`               |
| `DB_SAFETY_FACTOR`       | Коэффициент запаса (0..1)                       | `0.8`                 |

Эти параметры загружаются в `settings.db` (см. [документацию config](../config/config.md)).

---

## 🧱 Архитектура модуля

```
app/db/
├── __init__.py
├── repository.py   # DBRepository (пул, SQL‑методы)
├── router.py       # FastAPI эндпоинты для каждой таблицы
└── schemas.py      # Pydantic‑модели для таблиц и batch‑обёрток
```

### 1. `repository.py` – ядро доступа к данным

#### Класс `DBRepository`

- **Пул соединений:** инициализируется с параметрами из `settings.db`, использует `ThreadedConnectionPool` и `RealDictCursor` (результаты в виде словарей).
- **Методы:**
  - `_get_conn()`, `_put_conn()` – управление соединениями.
  - `execute(query, params)` – выполнить произвольный запрос, вернуть список строк (для SELECT) или пустой список.
  - `insert_one()` – вставка одной строки с опциональным `ON CONFLICT`.
  - `insert_batch()` – вставка множества строк с автоматическим разбиением.
  - `select()` – гибкая выборка с условиями (`where`, `where_conditions`), сортировкой, `LIMIT`/`OFFSET`.
  - `get_by_pk()` – получение строки по первичному ключу.
  - Специфические методы (см. выше).

**Важные детали:**
- Все операции выполняются **синхронно** (FastAPI запускает их в отдельном потоке).
- Автокоммит включён на уровне соединения (`conn.autocommit = True`), поэтому каждый `execute` фиксируется немедленно.
- При вставке батча динамически вычисляется максимальное количество строк в одном INSERT:
  \[
  max\_rows = \min\left( \left\lfloor \frac{max\_params\_per\_query}{col\_count} \cdot safety\_factor \right\rfloor,\; max\_rows\_per\_insert \right)
  \]
  Это позволяет не превысить лимит параметров PostgreSQL (65 535) и соблюсти заданный жёсткий лимит строк.

### 2. `router.py` – HTTP API

Для каждой из 8 таблиц реализованы два эндпоинта:
- `POST /{table-name}` – принимает JSON с полем `data` (массив объектов), вызывает `repo.insert_batch` и возвращает `BatchInsertResponse`.
- `GET /{table-name}` – принимает query‑параметры:
  - `pk` – фильтр по первичному ключу (название колонки различается: `uuid`, `device_id`, `ehr_id` и т.д.).
  - `limit` – максимальное количество строк.
  - `sort_by` – поле для сортировки.
  - `sort_dir` – `asc` или `desc`.
  - для `/user-properties` дополнительно `migrated` (true/false).

**Авторизация:**  
Все эндпоинты защищены через зависимости `require_read` (для GET) и `require_write` (для POST).  
Подробнее см. [auth.md](../auth/auth.md).

**Пример POST‑запроса (events-part):**
```bash
curl -X POST http://localhost:8000/dwh/events-part \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "data": [
      {"uuid": "123e4567-e89b-12d3-a456-426614174000", "event_type": "click", "event_time": "2025-01-01T12:00:00Z"}
    ]
  }'
```

**Пример GET‑запроса:**
```bash
curl "http://localhost:8000/dwh/events-part?limit=10&sort_by=event_time&sort_dir=desc" \
  -H "Authorization: Bearer <token>"
```

### 3. `schemas.py` – модели данных

- **Модели для таблиц** (например, `EventsPart`, `MobileDevices` и т.д.) – описывают поля, типы, валидацию.
- **Batch‑обёртки** – `EventsPartBatch`, `MobileDevicesBatch` и т.д. – используются в POST‑запросах.
- **Response‑схемы** – `GetEventsPartResponse` (содержит `rows: List[EventsPart]` и `count`).
- **BatchInsertResponse** – универсальный ответ для всех POST.

**Особенности валидации:**
- Поля `event_time` имеют кастомный валидатор, который преобразует ISO‑строку (например, `"2025-01-01T12:00:00Z"`) в `datetime`. Если строка не соответствует формату – выбрасывается `ValueError`, который FastAPI преобразует в `422 Unprocessable Entity`.
- Для необязательных полей допускается `None`.
- Поля типа `JSON` (например, `event_properties_json`) используют `Dict[str, Any]`.

---

## 🔌 API эндпоинты (список)

| Таблица | POST эндпоинт | GET эндпоинт | Первичный ключ для фильтра |
|---------|--------------|--------------|----------------------------|
| `events_part` | `/events-part` | `/events-part` | `uuid` |
| `mobile_devices` | `/mobile-devices` | `/mobile-devices` | `device_id` |
| `permanent_user_properties` | `/permanent-user-properties` | `/permanent-user-properties` | `ehr_id` |
| `changeable_user_properties` | `/changeable-user-properties` | `/changeable-user-properties` | `uuid` или `ehr_id` |
| `technical_data` | `/technical-data` | `/technical-data` | `uuid` |
| `tmp_event_properties` | `/event-properties` | `/event-properties` | `uuid` |
| `tmp_user_properties` | `/user-properties` | `/user-properties` | `uuid` + фильтр `migrated` |
| `user_locations` | `/user-locations` | `/user-locations` | `uuid` |

> **Примечание:** `changeable_user_properties` поддерживает фильтрацию по `uuid` или `ehr_id` через отдельные параметры `uuid` и `ehr_id` (не через `pk`).

---

## 🧠 Специфические методы репозитория (прямое использование)

Другие модули могут импортировать `DBRepository` и вызывать методы напрямую:

```python
from app.db.repository import get_repository

repo = get_repository()

# Получить все ehr_id из permanent_user_properties
ehr_set = repo.get_all_permanent_ehr_ids()

# Получить последние changeable-свойства для списка ehr_id
latest = repo.get_latest_changeable_for_ehrs([101, 102, None])

# Вставить или обновить запись в changeable_user_properties (upsert)
from app.db.schemas import ChangeableUserProperties
record = ChangeableUserProperties(
    ehr_id=101,
    uuid=uuid4(),
    event_time=datetime.now(),
    language="ru",
    age=30,
    ...
)
repo.insert_changeable(record)  # вставляет новую запись (историю)

# Пометить записи tmp_user_properties как migrated
repo.update_migrated_batch([uuid1, uuid2], migrated=True)
```

---

## 🧪 Тестирование

Для тестирования модуля рекомендуется:
- Использовать тестовую базу данных (отдельную схему или контейнер PostgreSQL).
- В `conftest.py` создать фикстуру, которая подменяет `settings.db` на тестовые параметры.
- Для репозитория писать интеграционные тесты с реальными запросами (или использовать `pytest-postgresql`).
- Для API – использовать `TestClient` FastAPI с моком `get_repository` или реальной тестовой БД.

Пример теста для `insert_batch`:

```python
def test_insert_batch(repo):
    rows = [{"uuid": "123e4567-e89b-12d3-a456-426614174000", "event_type": "test"}]
    ids, batches = repo.insert_batch("events_part", rows, returning_column="uuid")
    assert len(ids) == 1
    assert batches == 1
```

---

## 🐞 Логирование

Модуль логирует ключевые события:
- Инициализацию репозитория (`DBRepository initialized`).
- Начало и окончание вставки батча (`insert_batch for table ... total rows: N`, `batch inserted for table ..., batches: M`).
- Предупреждения о неизвестных таблицах при вычислении `max_rows`.
- Информацию о специфических запросах (`Fetching all ehr_id...`).

Логи пишутся через `logger = get_logger(__name__)`. Уровень задаётся в настройках `logging.level`.

---

## 🧠 Заметки для разработчиков и ИИ‑агентов

- **Безопасность:** Все SQL‑запросы используют параметризацию. Для динамических идентификаторов (имя таблицы, колонка) применяется `psycopg2.sql.Identifier` или ручная подстановка с экранированием (например, в `insert_batch` имена таблиц и колонок экранируются через f‑строку с предварительной проверкой? – В текущей реализации `table` и `columns` приходят из надёжных источников (имена констант), риск инъекции минимален.)
- **Производительность:** Пул соединений позволяет переиспользовать соединения. Для больших объёмов данных (тысячи строк) используется chunked insert. Для очень больших файлов рекомендуется использовать COPY, но текущий модуль этого не делает.
- **Расширяемость:** Если добавляется новая таблица, необходимо:
  1. Добавить Pydantic‑модель в `schemas.py`.
  2. Добавить запись в `TABLE_MODEL_MAP` в `repository.py`.
  3. Добавить эндпоинты в `router.py` (можно скопировать существующий паттерн).
- **Схемы, отличные от public:** Поддерживаются через нотацию `"schema.table"` в `TABLE_MODEL_MAP`. Пример – таблицы Яндекс.Метрики и Amplitude Web уже добавлены (но их эндпоинты пока не реализованы в роутере). Модели для них лежат в соответствующих модулях (`yandex_metrika.schemas`, `amplitude_web.schemas`), а в репозитории настроен отложенный импорт.
- **`ON CONFLICT`:** Для `changeable_user_properties` используется `DO UPDATE SET` всех колонок, кроме `ehr_id`. Таким образом, при повторной вставке того же `ehr_id` обновляются все остальные поля (история не сохраняется – это сделано намеренно, так как таблица предназначена для хранения **текущих** изменяемых свойств). Для истории используется другая логика.
- **Обработка `event_time`:** Валидатор `parse_event_time` выбрасывает `ValueError` при неверном формате. FastAPI автоматически превращает это в `422 Unprocessable Entity` с подробным сообщением. **Нельзя** заменять неверную дату на дефолтную – это нарушает целостность данных.
- **UUID и возвращаемые ID:** Метод `insert_batch` с `returning_column` возвращает список строк (значения приведены к `str`). Это удобно для передачи в ответе API.

---

## 📄 Связанные документы

- [Конфигурация приложения (settings, logging)](../config/config.md)
- [Модуль авторизации (auth)](../auth/auth.md)
- [Модуль S3](../s3/s3.md) – используется для хранения сырых данных
- [Модуль Data Scout](../data_scout/data_scout.md) – анализ структуры перед загрузкой в DWH
