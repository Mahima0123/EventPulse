from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv
from datetime import datetime
import psycopg2

load_dotenv()

# DB connection
conn = psycopg2.connect(
    host="localhost",
    port=os.getenv("POSTGRES_PORT"),
    database= os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

cur = conn.cursor()

# Kafka consumer
consumer = KafkaConsumer(
    "user_events",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consumer Started.. Listening to user_events...")

# process events
for message in consumer:
    event = message.value
    
    try:
        cur.execute("""
            INSERT INTO raw_user_events (
                event_id,
                user_id,
                event_type,
                content_id,
                country,
                subscription_type,
                event_timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
        """, (
            event["event_id"],
            event["user_id"],
            event["event_type"],
            event["content_id"],
            event["country"],
            event["subscription_type"],
            datetime.fromisoformat(event["event_timestamp"])
        ))
        conn.commit()
        print(f"Inserted: {event['event_type']} | {event['user_id']}")

    except Exception as e:
        print("Error inserting event:", e)
        conn.rollback()

for message in consumer:
    event = message.value
    try:
        cur.execute("""
            INSERT INTO raw_user_events (
                event_id,
                user_id,
                event_type,
                content_id,
                country,
                subscription_type,
                event_timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
        """, (
            event["event_id"],
            event["user_id"],
            event["event_type"],
            event["content_id"],
            event["country"],
            event["subscription_type"],
            datetime.fromisoformat(event["event_timestamp"])
        ))
        conn.commit()
        print(f"Inserted: {event['event_type']} | {event['user_id']}")
    except Exception as e:
        print("Error inserting event:", e)