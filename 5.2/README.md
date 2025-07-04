
PostgreSQL → Kafka → ClickHouse

Компоненты:

Producer (producer_pg_to_kafka.py):

1. Читает данные из таблицы user_logins в PostgreSQL

2. Отправляет в топик Kafka user_events

Consumer (consumer_to_clickhouse.py):

1. Слушает топик user_events

2. Записывает данные в таблицу user_logins в ClickHouse

Требования

Docker и Docker Compose

Python 3.8+

Библиотеки: json, kafka-python, psycopg2, clickhouse-connect,time

Этапы:
1. Запуск docker-compose.yml:
docker-compose up -d
2. Запуск producer:
python producer_pg_to_kafka.py
3. Запуск consumer:
python consumer_to_clickhouse.py

Остановить CTRL+C


