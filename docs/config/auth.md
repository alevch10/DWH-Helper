# Модуль авторизации и аутентификации (auth)

## 📌 Описание
Модуль обеспечивает безопасный доступ к API на основе **OAuth‑токенов Яндекса** и **JWT**.  
Пользователь предоставляет свой OAuth‑токен Яндекса (полученный, например, через Яндекс.Passport), а наш сервис обменивает его на JWT, проверяет права доступа (read / write) и пропускает или отклоняет запрос.

**Основные возможности:**
- Аутентификация через Яндекс OAuth (`https://login.yandex.ru/info`).
- Верификация JWT (подпись, срок действия).
- Разграничение доступа на основе списков `READ_ACCESS` и `WRITE_ACCESS` (задаются в конфигурации).
- FastAPI‑зависимости (`require_read`, `require_write`) для защиты эндпоинтов.

---

## 🔄 Процесс авторизации (шаг за шагом)

1. **Пользователь** нажимает кнопку **Authorize** в Swagger UI (или вручную добавляет заголовок `Authorization: Bearer <OAuth_токен_Яндекса>`).
2. **Наш API** принимает этот токен и отправляет запрос к Яндексу:
   ```http
   GET https://login.yandex.ru/info?format=jwt
   Authorization: OAuth <пользовательский_токен>
   ```
3. **Яндекс** возвращает JWT‑токен, подписанный секретом клиента (настроен в `settings.yandex.client_secret`).
4. **Наш сервис** декодирует JWT, проверяет:
   - Срок действия (`exp`) – если истёк → `401 Unauthorized`.
   - Подпись – неверная подпись → `401`.
   - Поле `login` – должно присутствовать.
5. **Проверка прав:**
   - Если `login` входит в `WRITE_ACCESS` → доступ **write** (разрешены все HTTP‑методы).
   - Если `login` входит в `READ_ACCESS` → доступ **read** (только GET‑запросы).
   - Иначе → `403 Forbidden`.
6. Для каждого последующего запроса клиент передаёт тот же OAuth‑токен в заголовке `Authorization: Bearer ...`.  
   **Примечание:** Мы не храним состояние сессии, каждый запрос независимо проверяется.

---

## ⚙️ Конфигурация (переменные окружения)

| Переменная           | Описание                                                                 | Пример значения                          |
|----------------------|--------------------------------------------------------------------------|------------------------------------------|
| `YANDEX_CLIENT_ID`   | OAuth‑client ID приложения, зарегистрированного в Яндексе                | `abc123...`                              |
| `YANDEX_CLIENT_SECRET` | Секретный ключ для верификации JWT (получается вместе с client_id)     | `secret_key...`                          |
| `AUTH_READ_ACCESS`   | Список логинов через запятую, имеющих **только чтение**                 | `ivanov,petrov@petrov.ru`                 |
| `AUTH_WRITE_ACCESS`  | Список логинов через запятую, имеющих **полный доступ**                 | `admin@ya.ru, user@ya.ru`                 |

> **Важно:** JWT, возвращаемый Яндексом, подписан секретом, который мы должны получить при регистрации приложения.  
> **Никогда** не передавайте client_secret клиентам.

---

## 🧩 Структура модуля

```
app/auth/
├── __init__.py
├── deps.py          # Зависимости FastAPI: get_current_user, require_read, require_write
└── schemas.py       # Pydantic‑схема User (login, access)
```

### 1. `schemas.py` – модель пользователя

```python
from pydantic import BaseModel

class User(BaseModel):
    login: str
    access: str   # "read" или "write"
```

### 2. `deps.py` – зависимости для эндпоинтов

#### `async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(oauth2_scheme)) -> User`

- Извлекает OAuth‑токен из заголовка `Authorization: Bearer <token>`.
- Запрашивает у Яндекса JWT.
- Декодирует JWT, проверяет подпись и срок действия.
- Извлекает `login`.
- Сверяет `login` со списками доступа.
- Возвращает объект `User(login=login, access="read"|"write")`.

**Возможные ошибки:**
- `401 Unauthorized` – токен недействителен, истёк, неверная подпись или ошибка Яндекса.
- `403 Forbidden` – логин отсутствует в обоих списках доступа.

#### `def require_write(user: User = Depends(get_current_user)) -> User`

- Проверяет, что `user.access == "write"`.
- В противном случае – `403 Forbidden`.

#### `def require_read(user: User = Depends(get_current_user)) -> User`

- Просто возвращает пользователя (любой аутентифицированный пользователь с read или write доступом разрешён).

---

## 🚀 Использование в роутерах

Пример защиты эндпоинтов:

```python
from fastapi import APIRouter, Depends
from app.auth.deps import require_read, require_write

router = APIRouter()

@router.get("/data")
async def get_data(user=Depends(require_read)):
    # Доступно всем с read или write
    return {"user": user.login}

@router.post("/data")
async def post_data(user=Depends(require_write)):
    # Только write‑пользователи
    return {"status": "created"}
```

---

## 🔐 Проверка токена в Swagger UI

1. Откройте `http://localhost:8000/docs`.
2. Нажмите кнопку **Authorize** (справа вверху).
3. В поле **Value** введите: `Bearer <ваш_OAuth_токен_Яндекса>`.
4. Теперь все запросы будут автоматически подписываться этим токеном.

---

## 🧪 Пример проверки прав через cURL

```bash
# Получить данные (требуется read или write)
curl -X GET "http://localhost:8000/appmetrica/ping" \
  -H "Authorization: Bearer y0_AgAAAAB..."

# Создать запись (требуется write)
curl -X POST "http://localhost:8000/some/write/endpoint" \
  -H "Authorization: Bearer y0_AgAAAAB..."
```

---

## 🧠 Заметки для разработчиков и ИИ‑агентов

- **OAuth vs JWT:** Пользователь предоставляет **OAuth‑токен Яндекса** (долгоживущий, ~1 год). Наш сервис каждый раз обменивает его на **JWT**, чтобы проверить срок действия и логин. JWT живёт несколько часов (задаётся Яндексом), но мы проверяем `exp` при каждом запросе.
- **Списки доступа:** Задаются через переменные окружения (см. выше). Они загружаются в `settings.get_read_access_list()` и `settings.get_write_access_list()`.  
  Формат: `AUTH_READ_ACCESS=ivanov,petrov,aa.levchenko` (без пробелов или с пробелами после запятых – метод `.strip()` очистит).
- **Безопасность:**  
  - Никогда не логгируйте OAuth‑токены или JWT.  
  - Используйте HTTPS в production.  
  - Храните `YANDEX_CLIENT_SECRET` в секрете (`.env`, менеджер секретов).
- **Производительность:** Каждый запрос делает вызов к `login.yandex.ru` – это добавляет задержку. При большом трафике можно кэшировать результат верификации на короткое время (например, на 1 минуту) по `login` + `exp`. Но текущая реализация этого не делает.
- **Таймаут:** В `get_current_user` установлен таймаут HTTP‑вызова 10 секунд. При недоступности Яндекса вернётся `500` (точнее, исключение `httpx.TimeoutException`, которое преобразуется в `HTTPException 500`).

---

## 📄 Связанные документы

- [Конфигурация приложения (settings, logging)](../config/config.md)
- [AppMetrica – использование авторизации](../appmetrica/appmetrica.md)
- [Amplitude – использование авторизации](../amplitude/amplitude.md)
