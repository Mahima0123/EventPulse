from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
import uuid
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

EVENT_TYPES = [
    "login",
    "search",
    "video_play",
    "video_pause",
    "video_complete",
    "like_content",
    "subscription_purchase"
]

COUNTRIES = [
    "US",
    "UK",
    "CA",
    "IN",
    "DE",
    "AU"
]

SUBSCRIPTIONS = [
    "free",
    "premium"
]

CONTENT_IDS = [f"MOV_{i}" for i in range(1, 101)]

USERS = [f"U{i}" for i in range(1, 501)]


def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USERS),
        "event_type": random.choice(EVENT_TYPES),
        "content_id": random.choice(CONTENT_IDS),
        "country": random.choice(COUNTRIES),
        "subscription_type": random.choice(SUBSCRIPTIONS),
        "event_timestamp": datetime.utcnow().isoformat()
    }


print("Starting producer...")

while True:
    event = generate_event()

    producer.send(
        "user_events",
        value=event
    )

    print(f"Produced: {event}")

    time.sleep(random.uniform(0.5, 2))