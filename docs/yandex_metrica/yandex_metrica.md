## Документация модуля Yandex.Metrica
> *Версия модуля: 1.0.0*  
### 📌 Описание
Модуль **Yandex.Metrica** предназначен для загрузки сырых данных из API Яндекс.Метрики (логи хитов), их очистки, валидации и преобразования в набор витрин (data marts), оптимизированных для маркетинговой аналитики.  
Модуль реализует полный ETL-цикл:
- Создание и отслеживание `logrequest` (асинхронная подготовка данных).
- Скачивание частей (в формате TSV) и объединение.
- Валидация каждого хита через Pydantic-модель `MetrikaHitRow`.
- Сохранение сырых данных в таблицу `ym_raw_data`.
- Построение восьми аналитических витрин:
  - `ym_ad_data` – первый визит клиента (first-click атрибуция)
  - `ym_successful_entries` – события успешной записи на приём
  - `ym_booking_visits` – визиты на домен booking (last-click атрибуция)
  - `ym_booking_transitions` – переходы на booking с сайта или прямые
  - `ym_user_paths` – хронология визитов пользователя
  - `ym_call_data` – данные о звонках
  - `ym_page_transitions` – переходы между страницами (sankey-диаграмма)

**Основные возможности:**
- Поддержка произвольного набора полей (параметр `fields`).
- Очистка URL от нежелательных query-параметров (настройка `ETL_QUERY_PARAMS_TO_REMOVE`).
- Определение целевых страниц записи по `netloc` и `path` (конфигурация).
- Идемпотентная вставка в `ym_booking_visits` (`ON CONFLICT DO NOTHING`).
- Полная перезагрузка за день (с очисткой предыдущих данных не производится, но при повторном запуске дубликаты возможны, кроме таблицы с conflict-правилом).

---

## 🧩 Требования к модулю (из спецификации)

1. Модуль должен работать с API Яндекс.Метрики (Logs API) через OAuth-токен.
2. Выполнять оценку возможности создания отчёта → создание logrequest → ожидание статуса `processed` → скачивание всех частей → объединение в единый список хитов.
3. Валидировать каждый хит с помощью Pydantic-модели (типы, диапазоны, преобразования пустых строк в `None`).
4. Сохранять сырые данные в таблицу `ym_raw_data`.
5. Последовательно строить витрины, используя функции из `ad_efficiency.py`.
6. При повторной загрузке за тот же день не дублировать визиты в `ym_booking_visits` (использовать `ON CONFLICT DO NOTHING` по `visit_id`).
7. Предоставлять REST API эндпоинты:
   - `/ad_efficiency` – запуск полного ETL за день.
   - Вспомогательные эндпоинты для работы с logrequest: `/counters`, `/logrequests`, `/logrequest/{id}`, `/logrequest/{id}/part/{n}/download` и т.д.
8. Логировать все ключевые этапы и ошибки.
9. Конфигурация через переменные окружения (базовый URL, домен booking, целевые пути, список удаляемых query-параметров).

---

## ⚙️ Конфигурация (переменные окружения)

Модуль использует настройки из `settings.yandexmetrica` и глобальные `settings.etl`.

| Переменная | Описание | Пример |
|------------|----------|--------|
| `YANDEXMETRICA_BASE_URL` | Базовый URL API Яндекс.Метрики | `https://api-metrika.yandex.ru/management/v1` |
| `YANDEXMETRICA_DEFAULT_FIELDS` | Список полей по умолчанию через запятую | `ym:pv:watchID,ym:pv:clientID,...` |
| `YANDEXMETRICA_BOOKING_DOMAIN` | Домен для фильтрации визитов на booking | `booking.clinic.ru` |
| `YANDEXMETRICA_TARGET_NETLOC` | Список netloc (доменов) целевых страниц записи (через запятую) | `booking.clinic.ru,app.clinic.ru` |
| `YANDEXMETRICA_TARGET_PATH` | Список путей целевых страниц записи (через запятую) | `/success,/thanks` |
| `YANDEXMETRICA_TARGET_SCHEME` | (опционально) Список схем | `https` |
| `YANDEXMETRICA_TARGET_PARAMS` | (опционально) Список параметров | |
| `YANDEXMETRICA_TARGET_QUERY` | (опционально) Список query-параметров | |
| `YANDEXMETRICA_TARGET_FRAGMENT` | (опционально) | |
| `ETL_QUERY_PARAMS_TO_REMOVE` | Query-параметры, удаляемые из URL (через запятую) | `utm_source,utm_medium,fbclid` |

Эти параметры загружаются через `settings.yandexmetrica.get_target_netloc_list()` и т.д.

---

## 🔌 API эндпоинты

Все эндпоинты требуют авторизации: заголовок `Authorization: OAuth <token>` или `Bearer <token>` (см. `get_token_from_header`). Доступ – `require_read` (т.е. пользователь должен быть в `READ_ACCESS` или `WRITE_ACCESS`).

### 1. Запуск ETL за день

`POST /yandex_metrika/ad_efficiency`

**Тело запроса** (`ProcessDayRequest`):
```json
{
  "counter_id": 95463686,
  "date": "2026-03-04",
  "source": "hits",
  "fields": ["ym:pv:watchID", "ym:pv:clientID"]  // опционально
}
```

- `counter_id` – номер счётчика Яндекс.Метрики.
- `date` – дата в формате `YYYY-MM-DD`.
- `source` – тип данных (`hits` или `visits`, по умолчанию `hits`).
- `fields` – список полей (если не указан, используется `settings.yandexmetrica.default_fields`).

**Ответ** (`ProcessDayResponse`):
```json
{
  "status": "success",
  "statistics": {
    "ym_raw_data": 90948,
    "ym_ad_data": 35000,
    "ym_successful_entries": 145,
    "ym_booking_visits": 2876,
    "ym_booking_transitions": 3000,
    "ym_user_paths": 31200,
    "ym_call_data": 42,
    "ym_page_transitions": 124500
  },
  "message": "Данные успешно обработаны"
}
```

**Ошибки:** 422 (неверные параметры), 500 (внутренняя ошибка).

### 2. Вспомогательные эндпоинты (для отладки)

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/counters` | Получить список счётчиков пользователя. |
| POST | `/logrequests` | Создать новый logrequest (параметры в query). |
| GET | `/logrequests` | Список logrequest для счётчика. |
| GET | `/logrequest/{request_id}` | Информация о конкретном запросе. |
| POST | `/logrequest/{request_id}/clean` | Очистить подготовленные логи. |
| POST | `/logrequest/{request_id}/cancel` | Отменить запрос. |
| GET | `/logrequest/{request_id}/part/{part_number}/download` | Скачать одну часть (TSV). |
| POST | `/report` | Сгенерировать ZIP-архив с CSV-файлом отчёта (без сохранения в БД). |

Пример вызова `/report`:
```bash
curl -X POST "http://localhost:8000/yandex_metrika/report?counter_id=123&date1=2026-03-01&date2=2026-03-01" \
  -H "Authorization: OAuth <token>" --output report.zip
```

---

## 🧱 Архитектура модуля

```
app/yandex_metrika/
├── __init__.py
├── client.py          # MetrikaClient (обёртка над httpx)
├── services.py        # generate_report, get_metrika_hits, process_part_streaming
├── ad_efficiency.py   # Функции построения витрин + get_ad_efficiency (оркестратор)
├── schemas.py         # Pydantic-модели: MetrikaHitRow, витрины, запросы/ответы
└── router.py          # FastAPI эндпоинты
```

### 1. `client.py` – MetrikaClient

- Асинхронный HTTP-клиент на базе `httpx` с OAuth-токеном.
- Методы:
  - `create_logrequest`, `get_logrequests`, `get_logrequest_info`, `clean_logrequest`, `cancel_logrequest`, `evaluate_logrequest`
  - `download_part` (полностью в память)
  - `download_part_stream` (асинхронный генератор чанков) – используется для потоковой обработки больших частей.

### 2. `services.py` – работа с logrequest

- `_initialize_log_request()` – оценивает, создаёт запрос и ждёт статуса `processed` (пауза 15 сек, максимум не ограничен). Возвращает `request_id` и список частей.
- `process_part_streaming()` – потоково читает часть, декодирует строки и вызывает callback `process_line` для каждой непустой строки.
- `generate_report()` – создаёт ZIP с CSV (без сохранения в БД).
- `get_metrika_hits()` – возвращает список `MetrikaHitRow` (используется в `ad_efficiency`).

### 3. `ad_efficiency.py` – построение витрин

Каждая функция принимает список хитов и возвращает список объектов соответствующей Pydantic-модели.  
Логика:

| Функция | Витрина | Описание |
|---------|---------|----------|
| `get_earliest_visit` | `ym_ad_data` | Группирует по `client_id`, выбирает самый ранний хит с `is_page_view=True` (первый визит). Очищает URL от лишних параметров. |
| `get_successful_entries` | `ym_successful_entries` | Фильтрует хиты, URL которых соответствует целевым `netloc` + `path` (через `is_url_target`). Для каждого `page_view_id` оставляет самый ранний хит. |
| `get_booking_visits` | `ym_booking_visits` | Берёт первые хиты визитов, содержащих домен `booking_domain`. Добавляет флаг `had_successful_entry`, если `visit_id` присутствует в successful_entries. |
| `get_booking_transitions` | `ym_booking_transitions` | Проходит по хронологии хитов каждого клиента. При первом попадании на booking (или после хитов не на booking) фиксирует переход (`from_site` или `direct_booking`). |
| `get_user_paths` | `ym_user_paths` | Для каждого клиента собирает визиты (по `visit_id`), нумерует. Определяет, была ли запись в этом визите или позже (по `first_entry_time_by_client`). |
| `get_call_data` | `ym_call_data` | Извлекает хиты, у которых заполнено любое поле `offline_call_*` (звонок). |
| `get_page_transitions` | `ym_page_transitions` | Строит последовательность переходов между страницами внутри визита. Удаляет последовательные дубликаты URL. Для первого перехода в визите `source` заменяется на детализированную метку источника (например, `organic_yandex`, `referral_https://...`). |

**Главная функция:** `get_ad_efficiency(token, counter_id, date, source, fields)`
- Скачивает хиты через `get_metrika_hits`.
- Вставляет их в `ym_raw_data`.
- Последовательно вызывает все функции-трансформы, каждый результат вставляет в соответствующую таблицу через `repository.insert_batch`.
- Для `ym_booking_visits` использует `on_conflict="DO NOTHING" conflict_target="(visit_id)"`.
- Возвращает словарь статистики (количество записей в каждой таблице).

### 4. `schemas.py` – модели

- **`MetrikaHitRow`** – полная модель хита (около 60 полей). Включает:
  - Преобразователи: пустые строки → `None`, строки `"N/A"` → `None`, JSON-поля нормализуются.
  - Валидаторы: числовые поля не должны превышать 20 знаков.
  - Псевдонимы для полей с префиксом `ym:pv:` (например, `watchID`, `pageViewID`).
- **Модели витрин:** `MetricaAdData`, `MetrikaSuccessfulEntries`, `BookingVisit`, `BookingTransition`, `UserPath`, `CallData`, `PageTransition`.
- **Схемы запросов/ответов:** `ProcessDayRequest`, `ProcessDayResponse`, `CountersResponse`, `LogRequest` и т.д.

### 5. `router.py` – эндпоинты

- Извлекает OAuth-токен из заголовка `Authorization` (поддерживает `Bearer` и `OAuth`).
- Использует `Depends(require_read)` для авторизации (только чтение; запись не требуется, т.к. ETL использует отдельную авторизацию для БД).
- `/ad_efficiency` вызывает `ad_efficiency.get_ad_efficiency` и возвращает результат.
- Остальные эндпоинты – прокси к `MetrikaClient`.

---

## 🔄 Процесс ETL (пошагово)

1. **Получение сырых данных**  
   - `MetrikaClient` создаёт logrequest, ждёт обработки, скачивает все части.
   - Части обрабатываются потоково (чтобы не хранить гигабайты в памяти).
2. **Валидация**  
   - Каждая строка TSV преобразуется в словарь, затем валидируется через `MetrikaHitRow.model_validate()`.
   - Невалидные строки логируются и пропускаются (но процесс не прерывается).
3. **Сохранение сырых данных**  
   - Все корректные хиты вставляются в `yandex_metrika.ym_raw_data` (batch-insert).
4. **Построение витрин** (функции из `ad_efficiency.py`):
   - `ym_ad_data` – первый визит каждого клиента (first-click).
   - `ym_successful_entries` – события записи.
   - `ym_booking_visits` – визиты на booking (с флагом успешной записи).
   - `ym_booking_transitions` – переходы на booking.
   - `ym_user_paths` – нумерованные визиты с меткой записи.
   - `ym_call_data` – звонки.
   - `ym_page_transitions` – последовательность страниц (sankey).
5. **Сохранение витрин**  
   - Каждая витрина вставляется через `repository.insert_batch`.
   - Для `ym_booking_visits` – `ON CONFLICT DO NOTHING` по `visit_id`.
6. **Возврат статистики**  
   - Клиент получает количество записей в каждой таблице.

---

## 📊 Примеры аналитических запросов (бизнес-вопросы)

### First-click атрибуция
```sql
SELECT 
    utm_source,
    utm_medium,
    utm_campaign,
    COUNT(DISTINCT client_id) AS users
FROM yandex_metrika.ym_ad_data
GROUP BY utm_source, utm_medium, utm_campaign
ORDER BY users DESC;
```

### Записи по дням
```sql
SELECT DATE(date_time) AS day, COUNT(*) AS bookings
FROM yandex_metrika.ym_successful_entries
GROUP BY day ORDER BY day;
```

### Конверсия визитов на booking в записи (last-click)
```sql
SELECT 
    utm_source,
    utm_medium,
    COUNT(*) AS visits,
    SUM(CASE WHEN had_successful_entry THEN 1 ELSE 0 END) AS bookings,
    ROUND(100.0 * SUM(CASE WHEN had_successful_entry THEN 1 ELSE 0 END) / COUNT(*), 2) AS conversion_rate
FROM yandex_metrika.ym_booking_visits
GROUP BY utm_source, utm_medium
ORDER BY visits DESC;
```

### Среднее число визитов до первой записи
```sql
WITH booking_clients AS (
    SELECT client_id, MIN(visit_number) AS first_booking_visit
    FROM yandex_metrika.ym_user_paths
    WHERE had_successful_entry
    GROUP BY client_id
)
SELECT AVG(first_booking_visit) FROM booking_clients;
```

### Популярные точки входа (sankey)
```sql
SELECT source AS entry_page, COUNT(*) AS entries
FROM yandex_metrika.ym_page_transitions
WHERE source NOT LIKE '(start)'
GROUP BY source ORDER BY entries DESC LIMIT 10;
```

---

## 🧪 Тестирование

Для тестирования рекомендуется:
- Использовать тестовый счётчик Яндекс.Метрики с небольшим объёмом данных.
- Мокать `MetrikaClient` (например, через `respx`) для имитации ответов API.
- Для модульных тестов трансформаций – передавать фиктивные списки хитов.
- Интеграционные тесты могут запускаться с реальным API (но тогда требуется валидный OAuth-токен).

Пример юнит-теста для `get_earliest_visit`:
```python
def test_earliest_visit():
    hits = [
        MetrikaHitRow(client_id=1, date_time=datetime(2025,1,2,10,0), ...),
        MetrikaHitRow(client_id=1, date_time=datetime(2025,1,1,9,0), ...),
    ]
    result = get_earliest_visit(hits)
    assert len(result) == 1
    assert result[0].date_time == datetime(2025,1,1,9,0)
```

---

## 🐞 Логирование

Логирование ведётся через `logger = get_logger(__name__)`.  
Ключевые события:
- `INFO`: создание запроса, статус, количество частей, количество обработанных строк.
- `WARNING`: невозможность очистить запрос, невалидные строки (первые 5 подробно, затем счётчик).
- `ERROR`: ошибки при скачивании, валидации, вставке в БД.

---

## 🧠 Заметки для разработчиков и ИИ‑агентов

- **Обработка больших объёмов:** Функции `process_part_streaming` и `get_metrika_hits` не загружают целиком весь отчёт в память, а обрабатывают строки по мере поступления. Однако результат всё равно накапливается в списке `hits`, что при миллионах строк может быть проблемой. Для production-сценариев лучше писать строки сразу во временный файл или в БД пачками.
- **Повторная загрузка:** При повторном запуске за тот же день данные в `ym_raw_data` продублируются (т.к. нет `ON CONFLICT`), а витрины могут задвоиться, кроме `ym_booking_visits`. Рекомендуется перед запуском очищать данные за обрабатываемую дату, если нужна идемпотентность.
- **Зависимость от времени:** Функция `_initialize_log_request` ждёт статуса `processed` без ограничения по времени. Если Яндекс.Метрика долго готовит отчёт (часы), запрос будет висеть. Желательно добавить таймаут и возможность асинхронного колбэка (но текущая реализация этого не предусматривает).
- **URL-очистка:** `remove_query_params` удаляет заданные параметры из URL – важно для корректной группировки страниц.
- **Метка источника в page_transitions:** Для первого перехода визита `source` заменяется на детализированное описание (например, `organic_yandex`, `ad_google`). Это позволяет строить sankey-диаграмму от источников трафика.
- **Безопасность:** Токен передаётся в заголовке и не логируется. Все SQL-запросы выполняются через параметризацию (репозиторий). YAML-маппинг не используется (в отличие от etl), вся логика зашита в коде Python.

---

## 📄 Связанные документы

- [Модуль DB (таблицы Яндекс.Метрики)](../db/db.md) – схемы таблиц находятся в `app.db.schemas` (для витрин) и в `yandex_metrika.schemas` для сырых хитов.
- [Модуль ETL (общая логика трансформации)](../etl/etl.md) – не используется напрямую, но утилиты `remove_query_params`, `is_url_target` взяты оттуда.
- [Конфигурация приложения](../config/config.md) – настройки S3, ETL_QUERY_PARAMS_TO_REMOVE.
- [Модуль авторизации](../auth/auth.md) – требования к токену.
