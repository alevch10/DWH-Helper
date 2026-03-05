## 1. SQL-запросы и бизнес-вопросы

### Таблица `ym_ad_data` (first-click атрибуция)
*Вопрос:* Какие каналы привели пользователей на сайт впервые?
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

### Таблица `ym_successful_entries` (записи на приём)
*Вопрос:* Сколько всего записей произошло за период?
```sql
SELECT 
    DATE(date_time) AS day,
    COUNT(*) AS bookings
FROM yandex_metrika.ym_successful_entries
GROUP BY day
ORDER BY day;
```

### Таблица `ym_booking_visits` (визиты на booking, last-click)
*Вопрос:* Конверсия визитов в записи по источникам (last-click атрибуция)
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

### Таблица `ym_booking_transitions` (переходы на booking)
*Вопрос:* С каких страниц сайта чаще всего переходят на booking?
```sql
SELECT 
    prev_url AS landing_page,
    COUNT(*) AS transitions
FROM yandex_metrika.ym_booking_transitions
WHERE transition_type = 'from_site'
GROUP BY prev_url
ORDER BY transitions DESC
LIMIT 20;
```

### Таблица `ym_user_paths` (пути пользователей)
*Вопрос:* Сколько визитов в среднем делает пользователь до первой записи?
```sql
WITH booking_clients AS (
    SELECT 
        client_id,
        MIN(visit_number) AS first_booking_visit
    FROM yandex_metrika.ym_user_paths
    WHERE had_successful_entry
    GROUP BY client_id
)
SELECT 
    AVG(first_booking_visit) AS avg_visits_to_booking
FROM booking_clients;
```

### Таблица `ym_call_data` (звонки)
*Вопрос:* Сколько звонков было совершено с сайта за день?
```sql
SELECT 
    DATE(date_time) AS day,
    COUNT(*) AS calls
FROM yandex_metrika.ym_call_data
GROUP BY day
ORDER BY day;
```

### Таблица `ym_page_transitions` (санкей-диаграмма)
*Вопрос:* Какие страницы являются самыми популярными точками входа?
```sql
SELECT 
    source AS entry_page,
    COUNT(*) AS entries
FROM yandex_metrika.ym_page_transitions
WHERE source NOT LIKE '(start)'
GROUP BY source
ORDER BY entries DESC
LIMIT 10;
```
*Вопрос:* Какие переходы между страницами самые частые?
```sql
SELECT 
    source,
    target,
    COUNT(*) AS weight
FROM yandex_metrika.ym_page_transitions
WHERE source NOT LIKE '(start)' AND target NOT LIKE '(start)'
GROUP BY source, target
ORDER BY weight DESC
LIMIT 20;
```

---

## 2. Wiki-страница документации модуля Яндекс.Метрики

### 📌 Назначение модуля
Модуль предназначен для загрузки данных из API Яндекс.Метрики (логи хитов), их очистки, валидации и преобразования в набор витрин (data marts), оптимизированных для маркетинговой аналитики. Основные цели:
- Анализ эффективности рекламных каналов (first-click и last-click атрибуция).
- Отслеживание пути пользователя от первого визита до записи на приём.
- Анализ переходов на домен booking.clinic.ru.
- Построение воронок и санкей-диаграмм.
- Учёт звонков с сайта.

### ⚙️ Последовательность ETL-процесса (сиквенс)
1. **Получение сырых данных**  
   POST-запрос к API Яндекс.Метрики для создания отчёта, скачивание всех частей, объединение.
2. **Валидация и очистка**  
   Каждый хит проходит через Pydantic-модель `MetrikaHitRow` – проверяются типы, длины строк, диапазоны чисел, удаляются пустые строки.
3. **Сохранение сырых данных**  
   Таблица `ym_raw_data` – полная копия полученных хитов (используется для перестроения витрин при необходимости).
4. **Построение витрин**  
   Последовательно вызываются функции:
   - `get_earliest_visit` → `ym_ad_data` (первый визит клиента)
   - `get_successful_entries` → `ym_successful_entries` (события записи)
   - `get_booking_visits` → `ym_booking_visits` (визиты на booking)
   - `get_booking_transitions` → `ym_booking_transitions` (переходы на booking)
   - `get_user_paths` → `ym_user_paths` (хронология визитов)
   - `get_call_data` → `ym_call_data` (звонки)
   - `get_page_transitions` → `ym_page_transitions` (переходы между страницами)
5. **Возврат статистики**  
   Функция `get_ad_efficiency` возвращает объект `ProcessDayResponse` с количеством записей в каждой таблице.

### 🚀 Запуск процесса
Эндпоинт: `POST /yandex_metrika/ad_efficiency`

**Тело запроса** (JSON):
```json
{
  "counter_id": 95463686,
  "date": "2026-03-04",
  "source": "hits",
  "fields": ["ym:pv:watchID", "ym:pv:clientID", ...]  // опционально
}
```
**Заголовки:** требуется `Authorization: OAuth <token>` (токен из заголовка).

**Ответ:**
```json
{
  "status": "success",
  "statistics": {
    "ym_raw_data": 90948,
    "ym_ad_data": 35000,
    "ym_successful_entries": 145,
    ...
  },
  "message": "Данные успешно обработаны"
}
```

### 📂 Структура таблиц (кратко)
| Таблица | Назначение | Ключевые поля |
|--------|------------|---------------|
| `ym_raw_data` | Сырые хиты (архив) | `watch_id`, `client_id`, `url`, `date_time`, все поля Метрики |
| `ym_ad_data` | Первый визит клиента | `client_id`, `utm_*`, `last_traffic_source`, `date_time` |
| `ym_successful_entries` | События записи | `client_id`, `visit_id`, `date_time` |
| `ym_booking_visits` | Визиты на booking | `visit_id` (PK), `client_id`, `visit_start_time`, `utm_*`, `had_successful_entry` |
| `ym_booking_transitions` | Переходы на booking | `id` (PK), `client_id`, `hit_time`, `booking_url`, `prev_url`, `transition_type` |
| `ym_user_paths` | Последовательность визитов | `id` (PK), `client_id`, `visit_id`, `visit_number`, `had_successful_entry`, `entry_time` |
| `ym_call_data` | Данные о звонках | `visit_id`, `client_id`, `date_time`, `offline_call_*` |
| `ym_page_transitions` | Переходы между страницами (для санкей) | `id` (PK), `visit_id`, `client_id`, `transition_date`, `source`, `target`, `sequence_num` |

### 🔍 Важные особенности
- Все URL очищаются от заданных query-параметров (список в `ETL_QUERY_PARAMS_TO_REMOVE`).
- Для первого визита в `ym_page_transitions` подставляется детализированная метка источника (например, `organic_yandex`, `referral_https://...`).
- При повторной загрузке за тот же день для `ym_booking_visits` используется `ON CONFLICT DO NOTHING` (чтобы не дублировать визиты). Для остальных таблиц дубликаты не предотвращаются – при необходимости следует очищать данные перед загрузкой или добавлять уникальные индексы.

### 📈 Типовые аналитические запросы (примеры)
См. раздел **SQL-запросы и бизнес-вопросы** выше.

### 🛠️ Настройка окружения
Переменные в `.env` / GitHub Secrets:
- `YANDEXMETRICA_BASE_URL` – базовый URL API.
- `YANDEXMETRICA_DEFAULT_FIELDS` – список полей по умолчанию через запятую.
- `YANDEXMETRICA_BOOKING_DOMAIN` – домен для фильтрации (booking.avaclinic.ru).
- `YANDEXMETRICA_TARGET_NETLOC`, `YANDEXMETRICA_TARGET_PATH` – для определения страниц записи.
- `ETL_QUERY_PARAMS_TO_REMOVE` – параметры URL, которые нужно удалять.
