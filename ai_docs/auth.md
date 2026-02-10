# Описание
Модуль реализует авторизацию и аутентификацию на сервисе. 

# Переменные 
- READ_ACCESS
- WRITE_ACCESS
## Формат заполнения
WRITE_ACCESS=['aa.levchenko', 'ff.fedorov']

# Процесс авторизации
1) Пользователь нажимает кнопку авторизации в Swagger 
2) Вводит туда {token}
3) Введенный токен используется для двух задач: 
- в качестве переменной "APPMETRICA_API_KEY", которую необходимо удалить из .env
- в качестве {token} для запроса на авторизацию:
```
curl --location 'https://login.yandex.ru/info?format=jwt' \
--header 'Authorization: OAuth {token}'
```
4) В ответ на запрос поступает jwt-токен:
``` jwt
eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE4MDIxODkwNjEsImlhdCI6MTc3MDcwNTYyMSwiaXNzIjoibG9naW4ueWFuZGV4LnJ1IiwianRpIjoiMGQ2MDJiNGUtYjdhYy00MTVjLWEzMDUtMDhjZjI2ZmJhOTFkIiwibG9naW4iOiJhYS5sZXZjaGVua29Ac2V2ZXJtZWQuY29tIiwicHN1aWQiOiIxLkFBOXFGdy5fUkFBbzFwZlZLVDExTlR0Wk9TSGxnLll0QUx4aDhxN2huNUV1V3Izb3JQbXciLCJ1aWQiOjExMzAwMDAwNjk0MTcyMDB9.hSAWTHJtvadJ9xqtNVl4KuGdZfHbgAmudERWw8LWgtQ
```
который при расшифровке выдает следующую структуру: 
``` json
{
   "iat": 1620915565,
   "jti": "384b7cd-0c42a10aa38c",
   "exp": 1652451414,
   "iss": "login.yandex.ru",
   "uid": 1004426,
   "login": "ivan",
   "psuid": "1.AAceCw.tbHg2w.qPWSRC5v2t2IaksPJgnge"
}

```
5) После получения токена необходимо выполнить 2 проверки: 
- exp < текущего времени в timestamp, иначе 401 
- login прописан в одну из переменных: READ_ACCESS / WRITE_ACCESS, иначе 403
6) Данный токен необходимо использовать для каждого запроса, при этом: 
- Если логин только в переменной READ_ACCESS, то ему доступны только GET-эндпоинты, а POST, PUT, DELETE возвращают 403.
- Для логина из WRITE_ACCESS доступны все запросы. 
7) Перед выполнением каждого запроса идет проверка токена. 
