# Описание
Модуль реализован для работы с API внешней системы аналитики данных AppMetrica. 

# Требования
- Модуль должен иметь свой файл API, в котором будет возможность использовать все имеющиеся запросы в AppMetrica. 
- Модуль должен делать API запросы в сервис AppMetrica используя токен из переменных

# Описание API
## Авторизация
- Используется header 'Authorization: OAuth {token}'

## Запрос данных о событии
### Описание запроса: 
Данный запрос заводит процесс подготовки отчета. При успешном старте процесса код 202 с body: 
```Your query is added to the queue.```
Чтобы скачать данные нужно посылать раз в 10 сек повторные запрос и тогда будем получать тот же 202, но прогресс: 
```Wait for result. Progress is 94%.```

когда процесс будет готов, сервис отдаст код 200, тип application.json и файл

### Формат запроса:
```
curl --location 'https://api.appmetrica.yandex.ru/logs/v1/export/events.json?application_id=4777504&skip_unavailable_shards=true&date_since=2026-02-09%2000%3A00%3A00&date_until=2026-02-09%2023%3A59%3A59&date_dimension=default&use_utf8_bom=true&fields=app_build_number%2Cprofile_id%2Cos_name%2Cos_version%2Cdevice_manufacturer%2Cdevice_model%2Cdevice_type%2Cdevice_locale%2Cdevice_ipv6%2Capp_version_name%2Cevent_name%2Cevent_json%2Cconnection_type%2Coperator_name%2Ccountry_iso_code%2Ccity%2Cappmetrica_device_id%2Cinstallation_id%2Csession_id%2Cinstallation_id%2Cevent_datetime' \
--header 'Authorization: OAuth {token}'
```
### Требования к запросу
- Не отправлять заголовок Cache-Control. 
- Для запроса выше должен быть свой GET запрос в приложении, чтобы я мог вызывать его с нужными мне значениями. 
Обрати внимание, в запросе есть переменные: 
-- application_id - должна быть вынесена в конфиг и подставляться как default, если не указано другое. 
-- skip_unavailable_shards - должна быть вынесена в query, но дефолтно false, если не указана
-- date_since=указывается в query, важно в описании указать формат даты
-- date_until=указывается в query, важно в описании указать формат даты
-- date_dimension=должна быть вынесена в query, но дефолтно default
-- use_utf8_bom=должна быть вынесена в query, но дефолтно true
-- fields= если возможно, сделай возможность множественного выбора с параметрами: (app_build_number%2Cprofile_id%2Cos_name%2Cos_version%2Cdevice_manufacturer%2Cdevice_model%2Cdevice_type%2Cdevice_locale%2Cdevice_ipv6%2Capp_version_name%2Cevent_name%2Cevent_json%2Cconnection_type%2Coperator_name%2Ccountry_iso_code%2Ccity%2Cappmetrica_device_id%2Cinstallation_id%2Csession_id%2Cinstallation_id%2Cevent_datetime) и автоматически выбори все эти параметра. Если невозможно, оставь этот параметр константой в этом виде.
- token - должен быть в settings.

### Формат ответа от AppMetrica
``` json
{
"data" :
[
{"app_build_number":"444","profile_id":"472246","os_name":"ios","os_version":"18.6.2","device_manufacturer":"Apple","device_model":"iPhone 14 Pro","device_type":"phone","device_locale":"ru_RU","device_ipv6":"::ffff:46.8.6.106","app_version_name":"2.29.0","event_name":"Banner Shown","event_json":"{\"Title\":\"Что умеет приложение?\",\"Place\":\"DASHBOARD\"}","connection_type":"cell","operator_name":"","country_iso_code":"RU","city":"","appmetrica_device_id":"13278911961594196248","installation_id":"8c05f492bb564e1e98b0d15c2c893e59","session_id":"10000000012","installation_id":"8c05f492bb564e1e98b0d15c2c893e59","event_datetime":"2026-02-09 16:20:55"},
{"app_build_number":"444","profile_id":"472246","os_name":"ios","os_version":"18.6.2","device_manufacturer":"Apple","device_model":"iPhone 14 Pro","device_type":"phone","device_locale":"ru_RU","device_ipv6":"::ffff:46.8.6.106","app_version_name":"2.29.0","event_name":"Create Appointment Doctors And Specialities Step","event_json":"{\"Ehr\":true,\"Doctor Type\":\"Adult\",\"Visit Type\":\"OFFLINE\"}","connection_type":"cell","operator_name":"","country_iso_code":"RU","city":"","appmetrica_device_id":"13278911961594196248","installation_id":"8c05f492bb564e1e98b0d15c2c893e59","session_id":"10000000012","installation_id":"8c05f492bb564e1e98b0d15c2c893e59","event_datetime":"2026-02-09 16:21:03"},
]
}
```
### Обработка ответа
- При запросе через API предлагать скачать файл, когда сервис вернет данные, в момент ожидания показывать, что идет процесс. 
- При запросе другой функцией приложения возвращать pydantic схему. event_json - возвращать как json. 