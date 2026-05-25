## Документация модуля Data Scout (анализ структуры данных)
> *Версия модуля: 1.0.0*  
### 📌 Описание
Модуль **Data Scout** предназначен для автоматического анализа сырых данных, хранящихся в S3 в формате ZIP‑архивов с NDJSON‑файлами. Он сканирует указанный путь в бакете, рекурсивно обходит все JSON‑объекты, собирает статистику по каждому полю (тип, диапазон значений, длина, уникальные образцы) и формирует YAML‑профиль схемы данных. Результат помогает быстро понять структуру данных, выявить вложенные объекты и JSON‑строки, спроектировать целевую схему DWH.

**Основные возможности:**
- Обход ZIP‑архивов с NDJSON (каждая строка – валидный JSON).
- Рекурсивный анализ вложенных объектов, массивов и JSON‑строк.
- Определение типа поля: `string`, `integer`, `float`, `boolean`, `array`, `object`, `null`, `json` (строковое JSON‑значение), `broken_json` (похоже на JSON, но не парсится).
- Сбор статистики: наличие `null`, минимальное/максимальное значение (для чисел), минимальная/максимальная длина (для строк и массивов), тип элементов массива (`items_type`), до 100 уникальных образцов значений.
- Формирование вложенного YAML‑дерева с группировкой полей по точкам.

---

## 🧩 Требования к модулю (из спецификации)

1. Модуль должен предоставлять **GET** эндпоинт `/scout/data_analysis?path={prefix}`.
2. Bucket S3 фиксирован в настройках (`settings.s3.bucket_name`), путь внутри бакета передаётся параметром `path`.
3. Сервис должен рекурсивно обрабатывать все ZIP‑файлы, скачивая их из S3, распаковывая и читая `.ndjson` файлы построчно.
4. Каждая строка NDJSON парсится как JSON; невалидные строки логируются и пропускаются.
5. Для каждого поля собирается статистика:
   - тип (с поддержкой `json` и `broken_json`);
   - `has_null` – был ли `null` или отсутствие поля;
   - для чисел: `min`, `max`;
   - для строк и массивов: `min_length`, `max_length`;
   - для массивов примитивов: `items_type` (если все элементы одного примитивного типа);
   - для всех полей, кроме `json`: до 100 уникальных образцов.
6. Поля типа `json` не сохраняют `unique_samples`, но рекурсивно обходят распарсенный объект, создавая виртуальные дочерние поля.
7. Поля типа `broken_json` сохраняют `unique_samples` (сами битые строки), `children` отсутствуют.
8. Ответ возвращается в формате **YAML** (Content‑Type: `application/x-yaml`).
9. Если по указанному пути нет ZIP‑файлов – ответ `404` с YAML‑сообщением об ошибке.
10. Использовать синхронные вызовы S3 (FastAPI справится в отдельном потоке). Логирование через `app.config.logger`.

---

## ⚙️ Конфигурация (переменные окружения)

Модуль использует общие настройки S3 из `settings.s3`:

| Переменная             | Описание                          | Пример                 |
|------------------------|-----------------------------------|------------------------|
| `S3_BUCKET_NAME`       | Имя бакета, где лежат данные      | `dwh-helper-data`      |
| `S3_ENDPOINT_URL`      | URL S3‑совместимого хранилища     | `https://s3.ru-1.storage` |
| `S3_ACCESS_KEY_ID`     | Access Key                        | `AKIA...`              |
| `S3_SECRET_ACCESS_KEY` | Secret Key                        | `...`                  |

Дополнительных настроек для Data Scout не требуется.

---

## 🔌 API эндпоинт

### `GET /scout/data_analysis`

Анализирует структуру данных по указанному префиксу внутри S3 бакета.

#### Query параметры

| Параметр | Тип    | Обязательный | Описание                                                                 |
|----------|--------|--------------|--------------------------------------------------------------------------|
| `path`   | string | да           | Префикс (путь) внутри бакета, например `"amplitude_exports/2024/"`. Может начинаться с `/` или без – оба варианта обрабатываются. |

#### Авторизация

Эндпоинт защищён – требует **read**‑доступа (валидный JWT, полученный через Яндекс OAuth). Используется зависимость `require_read`. Подробнее см. [документацию по авторизации](../auth/auth.md).

#### Ответы

**200 OK** – YAML с профилем схемы:
```yaml
path: amplitude_exports/2024/
schema:
  event_type:
    type: string
    has_null: false
    min_length: 5
    max_length: 32
    unique_samples:
      - purchase
      - login
  user:
    type: object
    has_null: false
    children:
      id:
        type: string
        min_length: 10
        max_length: 36
        unique_samples: [user_12345]
      metadata:
        type: json
        has_null: true
        min_length: 20
        max_length: 1024
        children:
          version:
            type: integer
            min: 1
            max: 2
            unique_samples: [1, 2]
```

**404 Not Found** – если по пути нет ZIP‑файлов:
```yaml
error: "No zip files found"
detail: "No zip files found at path: amplitude_exports/2025/"
```

**500 Internal Server Error** – при любой непредвиденной ошибке (логируется с exc_info):
```yaml
error: "Internal server error"
detail: "<сообщение исключения>"
```

#### Пример запроса (cURL)

```bash
curl -X GET "http://localhost:8000/scout/data_analysis?path=amplitude_exports/2024/" \
  -H "Authorization: Bearer <JWT_токен>" \
  --output schema.yaml
```

---

## 🧱 Архитектура модуля

```
app/data_scout/
├── __init__.py          # экспорт router
├── router.py            # эндпоинт /data_analysis, обработка ошибок, отдача YAML
├── service.py           # бизнес-логика: S3Client, обход ZIP/NDJSON, сбор статистики, построение YAML
└── schemas.py           # (опционально) – в текущей реализации не используется
```

### 1. `router.py`

- Определяет `APIRouter` с префиксом, который подключается в `main.py` как `/scout`.
- Эндпоинт `get_data_analysis(path: str)`:
  - Вызывает `analyze_data(path)` из service.
  - Сериализует результат в YAML (`yaml.dump`).
  - Возвращает `Response` с `media_type="application/x-yaml"`.
  - Перехватывает `ValueError` (нет ZIP‑файлов) → `404`.
  - Перехватывает все остальные исключения → логирует и возвращает `500`.

### 2. `service.py`

Содержит всю логику анализа данных.

#### Класс `FieldStats`

Хранит статистику для одного поля (пути). Методы:
- `__init__()` – инициализация полей.
- `to_dict()` – преобразует в словарь для YAML, исключая `None` и пустые множества.

#### Функции

| Функция | Описание |
|---------|----------|
| `get_json_type(value)` | Определяет тип JSON‑значения: `null, boolean, integer, float, string, array, object, undefined`. |
| `is_json_like(value)` | Проверяет, похожа ли строка на JSON (начинается с `{`/`[` и заканчивается `}`/`]`). |
| `try_parse_json(value)` | Пытается распарсить строку в JSON, возвращает `(success, parsed)`. |
| `update_field_stats(stats, value, skip_type_update)` | Обновляет статистику для поля: тип, `has_null`, min/max (числа), min/max_length (строки), добавляет уникальные образцы (до 100). |
| `traverse(data, current_path, stats)` | **Основной рекурсивный обход** данных (dict, list, примитив). Обрабатывает вложенные объекты, массивы, JSON‑строки (вызывает `traverse_json_content`). |
| `traverse_json_content(data, current_path, stats)` | Рекурсивно обходит содержимое распарсенной JSON‑строки, создавая виртуальные дочерние поля с тем же путём (но тип родителя остаётся `json`). |
| `build_yaml_schema(stats)` | Преобразует плоский словарь `{ "user.age": FieldStats, ... }` во вложенный YAML‑словарь с `children`. |
| `format_schema_for_output(schema)` | Рекурсивно удаляет `unique_samples` у полей с типом `json` (согласно требованию). |
| `analyze_data(path)` | **Точка входа**: получает список объектов S3, фильтрует `.zip`, для каждого скачивает, распаковывает, читает NDJSON, вызывает `traverse` для каждой строки, возвращает итоговый словарь `{"path": path, "schema": ...}`. |

#### Алгоритм работы (подробно)

1. **Сканирование S3**  
   `S3Client.list_objects(prefix=path)` – получает все объекты, чей Key начинается с `path`.
2. **Фильтрация ZIP** – оставляем только `.zip` файлы.
3. **Для каждого ZIP**:
   - Скачать содержимое в байты (`S3Client.get_object(key)`).
   - Открыть `ZipFile` из `BytesIO`.
   - Найти файлы с расширением `.ndjson`.
   - Для каждого NDJSON:
     - Прочитать всё содержимое, декодировать UTF‑8, разбить на строки.
     - Для каждой непустой строки:
       - `json.loads(line)` – если ошибка, логировать warning и пропустить.
       - Вызвать `traverse(parsed_object, (), stats)`.
4. **Обход данных (traverse)**:
   - Для `dict`: обходить каждое `(key, value)`. Если `value` – строка, похожая на JSON – попытаться распарсить. Успех → тип `json`, обойти содержимое; неудача → тип `broken_json`, собрать образцы.
   - Для `list`: обновить статистику контейнера (`min_length`, `max_length`, `items_type`), затем обойти каждый элемент (рекурсивно).
   - Для примитивов: вызвать `update_field_stats`.
5. **Построение YAML**:
   - `build_yaml_schema` собирает вложенную структуру, группируя пути по точкам.
   - `format_schema_for_output` удаляет `unique_samples` у полей с типом `json`.
6. **Возврат** – словарь с `path` и `schema`.

---

## 🧪 Тестирование

Для самопроверки используется существующая директория `/amplitude_exports` в бакете.  
Запустите приложение и выполните:

```bash
curl "http://localhost:8000/scout/data_analysis?path=amplitude_exports/" -H "Authorization: Bearer <токен>"
```

Ожидается YAML‑отчёт по всем ZIP‑файлам внутри `amplitude_exports/`.

### Рекомендации по написанию тестов

- Мокать `S3Client.list_objects` и `get_object`, возвращая тестовые ZIP с NDJSON.
- Проверить корректность определения типов, `has_null`, ограничение `unique_samples` (100).
- Проверить обработку вложенных JSON‑строк.
- Убедиться, что для поля типа `json` не выводятся `unique_samples`.

Пример теста (pytest с использованием `unittest.mock`):

```python
def test_analyze_data(mocker):
    mock_s3 = mocker.patch("app.data_scout.service.S3Client")
    mock_s3.return_value.list_objects.return_value = [{"Key": "test.zip"}]
    mock_s3.return_value.get_object.return_value = create_test_zip()  # bytes
    result = analyze_data("test/")
    assert "schema" in result
    assert result["path"] == "test/"
```

---

## 🐞 Логирование

Модуль использует логгер, полученный через `get_logger(__name__)`.  
Ключевые события (уровни `INFO`, `WARNING`, `ERROR`):

- Начало анализа: `Starting analysis for path: {path}`
- Найден ZIP: `Processing ZIP file: {key}`
- Найден NDJSON: `Processing NDJSON: {filename}`
- Невалидная JSON‑строка: `Failed to parse JSON in {file}:{line_num}: {error}`
- Битый ZIP: `Bad ZIP file {key}: {error}`
- Нет ZIP‑файлов: `No zip files found at path: {path}`
- Итоговое количество уникальных путей: `Analysis complete. Found {len(stats)} unique paths`

Ошибки времени выполнения логируются с `exc_info=True` в роутере.

---

## 🧠 Заметки для разработчиков и ИИ‑агентов

- **Производительность**: анализ может быть медленным при большом количестве больших ZIP‑файлов. Текущая реализация синхронная и последовательная. Для улучшения можно добавить асинхронную загрузку и параллельную обработку (но пока не требуется).
- **Память**: все NDJSON‑файлы читаются целиком в память (`.read()`). Для гигантских файлов (> 1 ГБ) лучше перейти на построчное чтение из ZIP (но `ZipFile.read` уже даёт строку). В текущих объёмах допустимо.
- **Виртуальные поля JSON**: когда поле имеет тип `json`, его содержимое обходится и добавляется как `children` того же поля. Это позволяет увидеть структуру JSON внутри строки, но без смешивания с полями верхнего уровня.
- **Отсутствие поля vs null**: В статистике `has_null` становится `true`, если значение поля было `null` или поле отсутствовало в объекте (т.к. при отсутствии ключа мы не вызываем `update_field_stats` для этого пути). Однако в текущей реализации `has_null` устанавливается только при явном `None`. Если поле может отсутствовать – в статистику оно не попадёт, и в YAML может не быть этого поля. Это ожидаемое поведение: мы описываем только встреченные поля.
- **Сортировка unique_samples**: в `FieldStats.to_dict()` образцы сортируются сначала по типу (`__name__`), затем по строковому представлению. Это даёт стабильный вывод.
- **Массивы объектов**: `items_type` не выводится, но `children` описывает поля элементов массива. Пример:
  ```yaml
  events:
    type: array
    has_null: false
    min_length: 1
    max_length: 100
    children:
      id: ...
      name: ...
  ```
- **YAML форматирование**: используется `yaml.dump` с `default_flow_style=False`, `allow_unicode=True`, `sort_keys=False` – сохраняет порядок полей (по мере добавления в stats).
- **Интеграция с S3**: предполагается, что `app.s3.client.S3Client` реализует методы `list_objects(prefix)` (возвращает список dict с ключом `"Key"`) и `get_object(key)` (возвращает `bytes`). Если это не так – модуль нужно адаптировать.

---

## 📄 Связанные документы

- [Конфигурация приложения (settings, S3)](../config/config.md)
- [Модуль авторизации (auth)](../auth/auth.md)
- [Интеграция с Amplitude (пример данных)](../amplitude/amplitude.md)
- [Интеграция с AppMetrica](../appmetrica/appmetrica.md)