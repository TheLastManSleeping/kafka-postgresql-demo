import json
import os
import time
from kafka import KafkaConsumer
import psycopg2

print("Consumer starting...")

# --- Подключение к PostgreSQL с ретраями ---
while True:
    try:
        conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST'),
            dbname=os.environ.get('POSTGRES_DB'),
            user=os.environ.get('POSTGRES_USER'),
            password=os.environ.get('POSTGRES_PASSWORD')
        )
        print("Successfully connected to PostgreSQL")
        break
    except psycopg2.OperationalError as e:
        print(f"Could not connect to PostgreSQL: {e}. Retrying in 5 seconds...")
        time.sleep(5)

cur = conn.cursor()

# --- НОВЫЙ БЛОК: Конфигурация Kafka Consumer ---
consumer_config = {
    'bootstrap_servers': 'kafka:9092',
    'group_id': 'postgres-consumer-group',
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
    
    # Читаем настройки из переменных окружения с разумными значениями по умолчанию
    'auto_offset_reset': os.environ.get('AUTO_OFFSET_RESET', 'earliest'),
    'max_poll_records': int(os.environ.get('MAX_POLL_RECORDS', 500)),
    'fetch_min_bytes': int(os.environ.get('FETCH_MIN_BYTES', 1))
}
print(f"Конфигурация консьюмера: {consumer_config}")

# Создаем консьюмер с новыми настройками
consumer = KafkaConsumer(
    'user_clicks', 'page_views', 'orders',
    **consumer_config
)
# --- КОНЕЦ НОВОГО БЛОКА ---

print("Kafka Consumer is ready to receive messages.")

# --- Основной цикл обработки сообщений ---
for message in consumer:
    topic = message.topic
    data = message.value
    print(f"Received message from topic '{topic}': {data}")

    try:
        if topic == 'user_clicks':
            cur.execute(
                "INSERT INTO clicks (event_timestamp, user_id, ip_address, url, element_id) VALUES (%s, %s, %s, %s, %s)",
                (data['timestamp'], data['user_id'], data['ip_address'], data['url'], data['element_id'])
            )
        elif topic == 'page_views':
            cur.execute(
                "INSERT INTO views (event_timestamp, user_id, ip_address, url, duration_seconds) VALUES (%s, %s, %s, %s, %s)",
                (data['timestamp'], data['user_id'], data['ip_address'], data['url'], data['duration_seconds'])
            )
        elif topic == 'orders':
            cur.execute(
                "INSERT INTO orders (event_timestamp, order_id, user_id, ip_address, amount) VALUES (%s, %s, %s, %s, %s)",
                (data['timestamp'], data['order_id'], data['user_id'], data['ip_address'], data['amount'])
            )
        
        conn.commit()
        print(f"Successfully inserted record into '{topic}' table.")

    except Exception as e:
        print(f"Error inserting record: {e}")
        conn.rollback()

cur.close()
conn.close()