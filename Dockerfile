# syntax=docker/dockerfile:1

FROM python:3.13-alpine AS builder

WORKDIR /app

# Устанавливаем зависимости для компиляции
RUN apk add --no-cache \
    build-base \
    postgresql-dev \
    python3-dev \
    curl

# Устанавливаем Poetry
RUN pip install --no-cache-dir poetry

COPY pyproject.toml poetry.lock ./

# Устанавливаем зависимости (компилируем)
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

# ------ ВТОРОЙ ЭТАП: финальный образ ------
FROM python:3.13-alpine

WORKDIR /app

# Устанавливаем ТОЛЬКО runtime-зависимости
# (библиотеки, нужные для работы, без компиляторов)
RUN apk add --no-cache \
    libpq \
    curl

# Копируем установленные пакеты из builder
COPY --from=builder /usr/local/lib/python3.13/site-packages/ /usr/local/lib/python3.13/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Копируем код приложения
COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]