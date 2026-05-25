## Документация модуля конфигурации (config)
> *Версия модуля: 1.0.0*  
# Модуль конфигурации (config)

## 📌 Описание
Модуль отвечает за централизованное управление настройками приложения и системой логирования.  
Он использует **Pydantic Settings** для загрузки переменных окружения из `.env` файла и предоставляет строго типизированный доступ к конфигурации всех подсистем (БД, внешние API, S3, авторизация и т.д.).  
Также модуль содержит настройку форматирования логов и удобную функцию получения логгера для любого компонента.

---

## 🧩 Состав модуля

```
app/config/
├── __init__.py
├── settings.py      # Pydantic‑модели и глобальный объект settings
└── logger.py        # Настройка логирования (формат, уровень, вывод в консоль)
```

### 1. `settings.py` – управление переменными окружения

#### Классы настроек

| Класс | Назначение | Связанные переменные окружения (пример) |
|-------|------------|------------------------------------------|
| `DBSettings` | Подключение к PostgreSQL (DWH) | `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_MAX_PARAMS_PER_QUERY`, `DB_MAX_ROWS_PER_INSERT`, `DB_SAFETY_FACTOR`, `DB_MINCONN`, `DB_MAXCONN` |
| `AppMetricaSettings` | Интеграция с AppMetrica | `APPMETRICA_BASE_URL`, `APPMETRICA_APPLICATION_ID`, `APPMETRICA_POLL_INTERVAL_SECONDS`, `APPMETRICA_POLL_TIMEOUT_SECONDS` |
| `YandexOAuthSettings` | OAuth для авторизации (Яндекс) | `YANDEX_CLIENT_ID`, `YANDEX_CLIENT_SECRET` |
| `YandexMetricaSettings` | Интеграция с Yandex.Metrica | `YANDEXMETRICA_BASE_URL`, `YANDEXMETRICA_DEFAULT_FIELDS`, `YANDEXMETRICA_BOOKING_DOMAIN`, `YANDEXMETRICA_TARGET_NETLOC`, `YANDEXMETRICA_TARGET_PATH`, `YANDEXMETRICA_TARGET_SCHEME`, `YANDEXMETRICA_TARGET_PARAMS`, `YANDEXMETRICA_TARGET_QUERY`, `YANDEXMETRICA_TARGET_FRAGMENT` |
| `S3Settings` | Хранилище S3 (совместимое с AWS) | `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_REGION`, `S3_ENDPOINT_URL`, `S3_BUCKET_NAME` |
| `LoggingSettings` | Уровень логирования | `LOGGING_LEVEL` (по умолч. `INFO`) |
| `AmplitudeSettings` | Amplitude (web / mobile) | `AMPLITUDE_WEB_SECRET_KEY`, `AMPLITUDE_WEB_CLIENT_ID`, `AMPLITUDE_MOBILE_SECRET_KEY`, `AMPLITUDE_MOBILE_CLIENT_ID` |
| `ETLSettings` | Параметры ETL‑процессов | `ETL_BATCH_SIZE` |
| `Settings` (главный) | Агрегирует все вышеперечисленные, плюс глобальные настройки приложения | `TITLE`, `VERSION`, `DESCRIPTION`, `DEBUG`, `AUTH_READ_ACCESS`, `AUTH_WRITE_ACCESS`, `ETL_QUERY_PARAMS_TO_REMOVE` |

#### Переменные авторизации и контроля доступа

| Переменная окружения      | Назначение                                                                 | Метод доступа в коде                     |
|---------------------------|----------------------------------------------------------------------------|------------------------------------------|
| `AUTH_READ_ACCESS`        | Список логинов (через запятую) с доступом **только на чтение** (GET)       | `settings.get_read_access_list()`        |
| `AUTH_WRITE_ACCESS`       | Список логинов с **полным доступом** (все методы)                          | `settings.get_write_access_list()`       |
| `ETL_QUERY_PARAMS_TO_REMOVE` | Список query‑параметров, которые нужно удалять при обработке запросов (для Yandex.Metrica) | `settings.get_query_params_to_remove()` |

> **Важно:** Названия переменных окружения регистронезависимы, но рекомендуется указывать в верхнем регистре, как в примерах.

#### Пример `.env` файла 

Пример .env в файле .env.example


#### Доступ к настройкам в коде

```python
from app.config.settings import settings

# Общие настройки
print(settings.title)
print(settings.debug)

# Доступ к вложенным группам
db_host = settings.db.host
amplitude_web_key = settings.amplitude.web_secret_key

# Списки доступа
read_users = settings.get_read_access_list()   # ['user1', 'user2']
```

---

### 2. `logger.py` – настройка системы логирования

#### Функции

- **`configure_logging(level: Optional[str] = None) -> None`**  
  Инициализирует корневой логгер:  
  - Устанавливает уровень логирования (по умолчанию `INFO`).  
  - Добавляет `StreamHandler` для вывода в консоль с форматированием:  
    `2025-05-26 12:34:56 - module_name - LEVEL - message`.  
  - Должна вызываться один раз при запуске приложения (например, в `main.py`).

- **`get_logger(name: str) -> logging.Logger`**  
  Возвращает экземпляр логгера с указанным именем (обычно передаётся `__name__`).  
  Используется во всех модулях для логирования.

#### Использование

```python
from app.config.logger import configure_logging, get_logger

# При старте приложения
configure_logging(level="DEBUG")   # или уровень из настроек

# В любом другом модуле
logger = get_logger(__name__)
logger.info("Запрос выполнен")
logger.error("Ошибка подключения к БД", exc_info=True)
```

#### Уровни логирования

Уровень можно задать через переменную окружения `LOGGING_LEVEL` (например, `DEBUG`, `INFO`, `WARNING`, `ERROR`).  
Если переменная не задана – по умолчанию `INFO`.

---

## 🚀 Интеграция с приложением

В `main.py` (точка входа) необходимо выполнить:

```python
from app.config.logger import configure_logging
from app.config.settings import settings

# Настройка логгера до импорта других модулей
configure_logging(level=settings.logging.level)

# Затем создание FastAPI app, подключение роутеров и т.д.
```

---

## 🧪 Тестирование конфигурации

Пример проверки загрузки настроек (можно добавить в тесты):

```python
from app.config.settings import settings

def test_settings_loaded():
    assert settings.db.host is not None
    assert settings.amplitude.web_client_id != ""
    assert isinstance(settings.get_read_access_list(), list)
```

---

## 🧠 Заметки для разработчиков и ИИ‑агентов

- **Изменение структуры настроек:** При добавлении нового внешнего сервиса создайте новый `BaseModel` (например, `NewServiceSettings`) и добавьте поле в главный класс `Settings`. Не забудьте описать переменные окружения в документации.
- **Парсинг списков:** Методы `get_read_access_list()`, `get_write_access_list()`, `get_query_params_to_remove()` разбивают строку по запятым и убирают лишние пробелы. Пустая строка возвращает пустой список.
- **Безопасность:** Секреты (пароли, ключи API) загружаются только из переменных окружения или `.env` файла. Никогда не загружайте их из кода или системы контроля версий. Файл `.env` должен быть добавлен в `.gitignore`.
- **Логирование:** Не логгируйте секреты (пароли, токены). Используйте `logger.debug()` для отладочной информации, `logger.error()` для исключений с `exc_info=True`.

---

## 📄 Связанные документы

- [Модуль авторизации (auth)](../auth/auth.md)
- [Интеграция с Amplitude](../amplitude/amplitude.md)
- [Интеграция с AppMetrica](../appmetrica/appmetrica.md)
- [Интеграция с Yandex.Metrica](../yandexmetrica/yandexmetrica.md)
...