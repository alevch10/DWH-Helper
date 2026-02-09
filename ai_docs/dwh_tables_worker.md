# Описание
Данный модуль работает с таблицами базы данных DWH. Он должен подключаться к БД, используя переменные из config 
Модуль должен уметь считывать данные из таблиц БД и записывать новые строки в них. 
У модуля должно быть собственно API, которое позволяет делать GET и POST запрос для каждой таблицы, а также, другие модули будут делать select и insert запросы в эти таблицы. 

## Требования к API:
- API модуля представлено отдельным файлом, который импортируется в main.py как роутер. 
- Для GET запросов реализован ряд опциональных query параметров: 
-- Поиск по конкретному первичному ключу
-- Ограничение на лимит значений. По умолчанию без ограничений. 
-- Выбор колонки, по которой делать сортировку
-- Направление сортировки asc/desc.

## Требования к схемам таблиц
- Схемы таблиц должны быть вынесены в отдельный файл, чтобы их можно было удобно редактировать. Идеально представить в форме pydantic схем.  

## Таблицы и их описание: 
БД: postgres
Схема: public 

### events_part 
Описание: Основная таблица, хранит информацию о всех пользовательских событиях

#### Колонки
Название	#	Тип данных	Автоувеличение	Правило сортировки	Not Null	По умолчанию	Комментарий
uuid	1	uuid	[NULL]	[NULL]	true	[NULL]	[NULL]
event_type	2	varchar	[NULL]	default	false	[NULL]	[NULL]
event_time	3	timestamp	[NULL]	[NULL]	false	[NULL]	[NULL]
user_id	4	int4	[NULL]	[NULL]	false	[NULL]	[NULL]
platform	5	varchar	[NULL]	default	false	[NULL]	[NULL]
device_id	6	varchar(40)	[NULL]	default	false	[NULL]	[NULL]
event_id	7	int4	[NULL]	[NULL]	false	[NULL]	[NULL]
language	8	varchar	[NULL]	default	false	[NULL]	[NULL]
os_name	9	varchar	[NULL]	default	false	[NULL]	[NULL]
os_version	10	varchar	[NULL]	default	false	[NULL]	[NULL]
session_id	11	int8	[NULL]	[NULL]	false	[NULL]	[NULL]
start_version	12	varchar	[NULL]	default	false	[NULL]	[NULL]
version_name	13	varchar	[NULL]	default	false	[NULL]	[NULL]

#### Ограничения
events_part_unique - unique key (uuid и event_time)

#### Внешний ключ:
events_part_mobile_devices_fk - владелец events_part, тип foreign key, связанная сущность mobile_devices, колонка device_id

### mobile_devices
Описание: Хранит информацию об устройствах, с которых было выполнено событие. 

#### Колонки
Название	#	Тип данных	Автоувеличение	Правило сортировки	Not Null	По умолчанию	Комментарий
device_id	1	varchar(40)	[NULL]	default	true	[NULL]	[NULL]
device_brand	2	varchar	[NULL]	default	false	[NULL]	[NULL]
device_carrier	3	varchar	[NULL]	default	false	[NULL]	[NULL]
device_family	4	varchar	[NULL]	default	false	[NULL]	[NULL]
device_manufacturer	5	varchar	[NULL]	default	false	[NULL]	[NULL]
device_model	6	varchar	[NULL]	default	false	[NULL]	[NULL]
device_type	7	varchar	[NULL]	default	false	[NULL]	[NULL]

#### Ограничения:
mobile_devices_pkey - PRIMARY KEY (device_id)

### permanent_user_properties
Описание: Содержит свойства пользователя, которые назначаются при регистрации и не меняются с течением времени

#### Колонки
Название	#	Тип данных	Автоувеличение	Правило сортировки	Not Null	По умолчанию	Комментарий
ehr_id	1	int4	[NULL]	[NULL]	true	[NULL]	[NULL]
gender	2	varchar	[NULL]	default	false	[NULL]	[NULL]
cohort_day	3	int4	[NULL]	[NULL]	false	[NULL]	[NULL]
cohort_week	4	int4	[NULL]	[NULL]	false	[NULL]	[NULL]
cohort_month	5	int4	[NULL]	[NULL]	false	[NULL]	[NULL]
registered_via_app	6	bool	[NULL]	[NULL]	false	[NULL]	[NULL]
source	7	varchar	[NULL]	default	false	[NULL]	[NULL]

#### Ограничения:
permanent_user_properties_pk - PRIMARY KEY (ehr_id)

### technical_data
Описание: Содержит техническую информацию сервиса Amplitude. 

#### Колонки
Название	#	Тип данных	Автоувеличение	Правило сортировки	Not Null	По умолчанию	Комментарий
uuid	1	uuid	[NULL]	[NULL]	true	[NULL]	[NULL]
insert_id	2	varchar	[NULL]	default	false	[NULL]	[NULL]
amplitude_attribution_ids	3	json	[NULL]	[NULL]	false	[NULL]	[NULL]
amplitude_id	4	int8	[NULL]	[NULL]	false	[NULL]	[NULL]
is_attribution_event	5	varchar	[NULL]	default	false	[NULL]	[NULL]
library	6	varchar	[NULL]	default	false	[NULL]	[NULL]
group_properties_json	7	json	[NULL]	[NULL]	false	[NULL]	[NULL]
groups_json	8	json	[NULL]	[NULL]	false	[NULL]	[NULL]
plan_json	9	json	[NULL]	[NULL]	false	[NULL]	[NULL]

### tmp_event_properties
Описание: Содержит свойства события. На текущий момент в формате json. 

#### Колонки
Название	#	Тип данных	Автоувеличение	Правило сортировки	Not Null	По умолчанию	Комментарий
uuid	1	uuid	[NULL]	[NULL]	false	[NULL]	[NULL]
event_properties_json	2	jsonb	[NULL]	[NULL]	false	[NULL]	[NULL]

### tmp_user_properties
Описание: Содержит свойства пользователя, как постоянные, так и переменные. 

#### Колонки
Название	#	Тип данных	Автоувеличение	Правило сортировки	Not Null	По умолчанию	Комментарий
uuid	1	uuid	[NULL]	[NULL]	false	[NULL]	[NULL]
user_properties_json	2	jsonb	[NULL]	[NULL]	false	[NULL]	[NULL]
language	3	varchar	[NULL]	default	false	[NULL]	[NULL]
session_id	4	int8	[NULL]	[NULL]	false	[NULL]	[NULL]
start_version	5	varchar	[NULL]	default	false	[NULL]	[NULL]
migrated	22	bool	[NULL]	[NULL]	false	[NULL]	[NULL]
event_time	24	timestamp	[NULL]	[NULL]	false	[NULL]	[NULL]

### user_locations
Описание: Содержит информацию о геопозиции пользователя в момент совершения каждого из событий

#### Колонки
Название	#	Тип данных	Автоувеличение	Правило сортировки	Not Null	По умолчанию	Комментарий
uuid	1	uuid	[NULL]	[NULL]	true	[NULL]	[NULL]
location_lat	2	float8	[NULL]	[NULL]	false	[NULL]	[NULL]
location_lng	3	float8	[NULL]	[NULL]	false	[NULL]	[NULL]
city	4	varchar	[NULL]	default	false	[NULL]	[NULL]
country	5	varchar	[NULL]	default	false	[NULL]	[NULL]
ip_address	6	inet	[NULL]	[NULL]	false	[NULL]	[NULL]
region	7	varchar	[NULL]	default	false	[NULL]	[NULL]
