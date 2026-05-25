# Описание
Данный файл содержит требования по настройке доставки приложения на исполняемую среду. 

# Требования
- Сервис должен быть подготовлен для доставки в Docker-контейнер и для управления через GitLab action. 
-- Сервис должен поддерживать обновление секретов через GitLab Action с последующей перезагрузкой сервиса. 
-- Сервис должен содержать .dockerignore воизбежание упаковки секретов и лишних данных
-- Deploy на сервер должен происходить при пуше тега. 
-- Переменная в .env "VERSION" должна определяться из номера тега
- Сервис должен содержать инструкции по запуску на локальной машине. 

# Настройки GitLab Action
## Окружения
- Создано окружение "production"

## Environment Variables
- TITLE
- DESCRIPTION 
- LOGGING_LEVEL
- DWH_MAX_WRITE_BATCH_BYTES
- DWH_MAX_ROWS_PER_INSERT
- APPMETRICA_BASE_URL
- APPMETRICA_APPLICATION_ID
- APPMETRICA_POLL_INTERVAL_SECONDS
- APPMETRICA_POLL_TIMEOUT_SECONDS
- S3_REGION
- S3_ENDPOINT_URL
- S3_BUCKET_NAME
- S3_ACCESS_KEY_ID
- AMPLITUDE_WEB_CLIENT_ID
- AMPLITUDE_MOBILE_CLIENT_ID

! Все переменные заданы без кавычек ""

## Environment Secrets
- DWH_DATABASE_URL
- APPMETRICA_API_KEY
- S3_SECRET_ACCESS_KEY
- AMPLITUDE_WEB_SECRET_KEY
- AMPLITUDE_MOBILE_SECRET_KEY
