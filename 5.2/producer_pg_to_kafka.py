# producer_pg_to_kafka.py
import psycopg2
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

conn = psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port=5432
)
cursor = conn.cursor()

cursor.execute("SELECT id,username, event_type, extract(epoch FROM event_time) FROM user_logins where sent_to_kafka IS NULL or sent_to_kafka = FALSE")
rows = cursor.fetchall()

for row in rows:
    data = {
        "id": row[0],
        "user": row[1],
        "event": row[2],
        "timestamp": float(row[3])  # преобразуем Decimal → float
    }
    producer.send("user_events", value=data)
    producer.flush()
    cursor.execute("""update user_logins set sent_to_kafka = TRUE where id=%s""",(row[0],))
    conn.commit()
    print("Sent:", data)
    time.sleep(0.5)

cursor.close
conn.close()
    
    
