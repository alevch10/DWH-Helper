Вот финальная спецификация, готовая к копированию и передаче AI-агенту.

## 1. Назначение
Сервис анализирует сырые данные из S3 (ZIP-архивы с NDJSON-файлами) и возвращает YAML-профиль всех полей, включая вложенные объекты и JSON-строки. Результат помогает быстро понять структуру данных и спроектировать целевую схему БД.

## 2. Новый модуль в проекте
Создать директорию `app/data_scout/` со следующими файлами:

```
app/data_scout/
├── __init__.py          # экспорт router
├── router.py            # FastAPI эндпоинт GET /data_analysis
├── service.py           # бизнес-логика: сканирование S3, анализ NDJSON
└── schemas.py           # (опционально) Pydantic-модели для внутренних структур
```

В `app/main.py` подключить роутер:
```python
from app.data_scout.router import router as data_scout_router
app.include_router(data_scout_router, prefix="/scout", tags=["Scout"])
```

## 3. API Endpoint

**GET** `/data_analysis?path={path}`

**Query параметры:**
| Параметр | Тип    | Обязательный | Описание |
|----------|--------|--------------|-----------|
| `path`   | string | да           | Префикс внутри бакета (например, `"amplitude_exports/2024/"`) |

**Bucket** фиксирован в настройках приложения (`settings.s3.bucket_name`) и не передаётся в запросе.

**Пример запроса:**
```
GET /data_analysis?path=amplitude_exports/2024/
```

## 4. Формат ответа (YAML)

При успехе (200 OK) возвращается YAML с Content-Type: `application/x-yaml` или `text/yaml`.

```yaml
path: amplitude_exports/2024/
schema:
  <field_path>:
    type: string|integer|float|boolean|array|object|null|undefined|json|broken_json
    has_null: true|false
    # Для чисел:
    min: <число>
    max: <число>
    # Для строк (обычных, json, broken_json):
    min_length: <целое>
    max_length: <целое>
    # Для массивов:
    min_length: <целое>        # минимальная длина массива (кол-во элементов)
    max_length: <целое>        # максимальная длина массива
    items_type: <тип>          # для массивов примитивов (string, integer, ... или undefined)
    # Для объектов и json-полей:
    children:                  # рекурсивное описание вложенных полей
      <child_field>: ...
    # Для всех типов, кроме json:
    unique_samples:            # до 100 уникальных значений
      - <пример1>
      - <пример2>
```

**Особенности:**
- `field_path` — путь через точки (например, `user.address.city`).
- Для полей типа `json` **не выводится** `unique_samples` (только `children`).
- Для полей типа `broken_json` выводится `unique_samples` (сами битые JSON-строки), `children` отсутствует.
- `has_null` = `true`, если хотя бы одно значение поля было `null` (явный null в данных) или отсутствовало 
- Для массивов объектов `children` описывает поля самих объектов.
- `items_type` указывается только для массивов примитивов; для массивов объектов поле `items_type` отсутствует.

## 5. Алгоритм работы

### 5.1. Основной поток (service.py)

1. Получить список всех `.zip` объектов в S3 по указанному `path` с помощью `S3Client.list_objects(prefix=path)`.
2. Для каждого ZIP-файла:
   - Скачать содержимое через `S3Client.get_object(key)` (возвращает `bytes`).
   - Распаковать архив с помощью `zipfile.ZipFile(io.BytesIO(zip_bytes))`.
   - Найти все файлы с расширением `.ndjson`.
   - Для каждого NDJSON-файла читать строки (`.decode('utf-8')`), каждую строку парсить как JSON (`json.loads`).
     - Если строка не парсится → логировать warning, пропустить.
   - Для каждого успешно распарсенного JSON-объекта выполнить рекурсивный обход (п. 5.2).
3. После обработки всех файлов сформировать YAML-дерево (п. 5.4) и вернуть его в response.

### 5.2. Рекурсивный обход и сбор статистики

Функция `traverse(data, current_path, stats)`:
- `data` — текущий фрагмент (dict, list, примитив, None).
- `current_path` — кортеж строк (например, `('user', 'address', 'city')`).
- `stats` — глобальный словарь: ключ = `'.'.join(current_path)`, значение = объект статистики.

**Правила:**

- **Если data — dict:**
  Для каждой пары `(key, value)`:
    - Новый путь = `current_path + (key,)`
    - Если `value` — dict → `traverse(value, новый_путь, stats)`
    - Если `value` — list → `traverse(value, новый_путь, stats)` (обработка массива см. ниже)
    - Если `value` — строка, которая похожа на JSON (после `strip()` начинается с `'{'`/`'['` и заканчивается `'}'`/`']'`):
        - Попытаться `json.loads(value)`
        - Если успешно:
            - Обновить статистику для текущего пути как для `json` (сохранить min/max length, тип json, **без samples**)
            - Рекурсивно обойти распарсенный объект с тем же путём (т.е. содержимое JSON станет виртуальными детьми того же поля)
        - Если не успешно:
            - Обновить статистику как для `broken_json` (тип broken_json, сохранить min/max length и samples)
    - Иначе:
        - Обновить статистику как для конечного поля (примитив, null, массив — с особым правилом для массива)

- **Если data — list:**
  - Сначала обновить статистику **самого контейнера** (тип array):
      - `min_length`/`max_length` по длине списка
      - `items_type` — если все элементы примитивного типа и одного типа → указать тип, иначе `undefined`
  - Затем для каждого элемента `item` в списке:
      - Если `item` — примитив, null, dict — вызвать `traverse(item, current_path, stats)` (путь не меняется)
      - Если `item` — list — обработать рекурсивно (массив массивов)

- **Если data — примитив (int, float, str, bool, None):**
  - Обновить статистику для конечного поля (см. 5.3).

### 5.3. Структура статистики для одного поля

Хранить в памяти для каждого пути:

```python
{
    "type": None,           # string, integer, float, boolean, array, object, null, json, broken_json, undefined
    "has_null": False,      # был ли явный null
    "min": None,            # для чисел
    "max": None,
    "min_length": None,     # для строк и массивов (длина строки или длина массива)
    "max_length": None,
    "items_type": None,     # для массивов примитивов (string/integer/...)
    "unique_samples": set() # до 100 уникальных значений (кроме json)
}
```

**Обновление при встрече значения:**
- **Тип:** 
  - Если `type is None` → установить в тип значения (None → `null`).
  - Если текущий тип != тип значения и значение не None → установить `undefined`.
- **has_null:** если значение is None → True.
- **min/max:** если значение int/float → обновить.
- **min_length/max_length:** 
  - для строк → обновить по `len(value)`
  - для массивов (при обработке контейнера) → обновить по длине списка
- **items_type:** только для массивов примитивов (определяется при обходе элементов).
- **unique_samples:** 
  - если тип не json → добавить значение (как есть, для списков — как tuple, чтобы хэшировалось).
  - если `len(unique_samples) >= 100` → не добавлять новые.

### 5.4. Формирование YAML-дерева

Функция `build_yaml(stats_dict)` преобразует плоский словарь с ключами-путями во вложенный YAML.

Пример:
```python
{
    "user.age": { "type": "integer", "min": 18, "max": 99, "unique_samples": {25, 42} },
    "user.name": { "type": "string", "min_length": 3, "max_length": 20, ... }
}
```
превращается в:
```yaml
user:
  type: object
  has_null: false
  children:
    age:
      type: integer
      min: 18
      max: 99
      unique_samples: [25, 42]
    name:
      type: string
      ...
```

- Корневой элемент — всегда объект (даже если путь пустой). Его `type = "object"`, `has_null = False`, `children` = все поля первого уровня.
- Поля, у которых `type = "object"`, также получают `children`.
- Поля `type = "array"` не имеют `children` (если массив объектов, то `children` относится к элементам, но в структуре это будет выглядеть так, как будто массив — это контейнер, а его дети — поля объектов внутри). Для простоты: при построении YAML для массива объектов мы просто создаём поле с `type: array` и вложенный `children`, который описывает структуру одного элемента.

### 5.5. Интеграция с S3 и конфигурацией

В `service.py`:
```python
from app.config.settings import settings
from app.s3.client import S3Client

s3 = S3Client()
bucket = settings.s3.bucket_name
```

Использование:
```python
objects = s3.list_objects(prefix=path)   # path приходит из запроса
for obj in objects:
    if obj["Key"].endswith(".zip"):
        zip_bytes = s3.get_object(obj["Key"])
        # обработка...
```

## 6. Обработка ошибок и логирование

- Логирование через `from app.config.logger import get_logger; logger = get_logger(__name__)`.
- Если ZIP-файл повреждён или не содержит NDJSON → логировать warning и пропустить.
- Если строка невалидный JSON → логировать warning, пропустить.
- Если путь не существует или нет ZIP-файлов → вернуть 404 с YAML-сообщением:
  ```yaml
  error: "No zip files found"
  detail: "path=amplitude_exports/2024/ does not contain any .zip files"
  ```
- Другие ошибки (исключения) → 500 Internal Server Error с YAML-телом `{error: "...", detail: "..."}`.

## 7. Тестирование

Для самопроверки использовать директорию `/amplitude_exports` в бакете (существует).

Запустить приложение, выполнить:
```bash
curl "http://localhost:8000/data_analysis?path=/amplitude_exports/"
```

Ожидается YAML-отчёт, где будут проанализированы все ZIP-архивы внутри `amplitude_exports/`.

## 8. Требования к реализации 

- **Не выводить unique_samples для полей с типом `json`**.
- **Ограничение unique_samples**: 100 значений, при достижении лимита новые не добавлять.
- **Тип `json`** выставлять для строк, которые успешно парсятся как JSON (объект или массив). Исходную строку не сохранять в samples.
- **Тип `broken_json`** – для строк, которые похожи на JSON, но не парсятся.
- **Отсутствие поля  считается `has_null:true `**.
- **Рекурсия без ограничения глубины**.
- Использовать синхронные вызовы (FastAPI справится в отдельном потоке).

## 9. Пример ожидаемого вывода

```yaml
path: amplitude_exports/2024/
schema:
  event_type:
    type: string
    has_null: false
    min_length: 5
    max_length: 32
    unique_samples:
      - "purchase"
      - "login"
  user:
    type: object
    has_null: false
    children:
      id:
        type: string
        has_null: false
        min_length: 10
        max_length: 36
        unique_samples:
          - "user_12345"
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
          platform:
            type: string
            min_length: 3
            max_length: 10
            unique_samples: ["ios", "android"]
```