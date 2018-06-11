# mtuci-masters

Код проекта для системы профилирования пользователей. Написан на языке Python 3. Рекомендуется использовать виртуальную среду:

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## nginx log format

Описан формат логов nginx, куда приходят данные из фронтенда при помощи библиотеки Matomo.

```
log_format  kv escape=json  
    '{"ip":"$remote_addr","time":"$time_iso8601",'
    '"request":"$request","body":"$request_body",'
    '"referrer":"$http_referer","user_agent":"$http_user_agent",'
    '"country":"$geoip_city_country_code","city":"$geoip_city"}';
```

## parse_nginx_logs.py
Происходит парсинг логов nginx. Оптимально ставить по крону `* * * * *`.

## run_api.py
Рекомендации относительно просмотров.
