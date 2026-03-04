# producer.py
import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  # internal Docker network
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    request_timeout_ms=20000
)

topic = 'transactions'
valid_sources = ['mobile', 'web', 'pos']

# Track duplicate events
sent_duplicates = set()

def generate_event():
    """
    Generate event:
    - valid
    - invalid (negative, huge, bad_timestamp, unknown_source, duplicate)
    - late (>3 menit)
    """
    event_type = random.choices(['valid', 'invalid', 'late'], weights=[70, 15, 15])[0]

    event = {
        "user_id": f"U{random.randint(10000,99999)}",
        "amount": random.randint(1,1000000),
        "timestamp": datetime.utcnow().isoformat() + 'Z',
        "source": random.choice(valid_sources)
    }

    # INVALID event
    if event_type == 'invalid':
        invalid_type = random.choice(['negative','huge','bad_timestamp','unknown_source','duplicate'])
        if invalid_type == 'negative': event['amount'] = -random.randint(1,1000)
        elif invalid_type == 'huge': event['amount'] = random.randint(10000001,20000000)
        elif invalid_type == 'bad_timestamp': event['timestamp'] = '2025-13-40T25:61:00Z'
        elif invalid_type == 'unknown_source': event['source'] = 'unknown'
        elif invalid_type == 'duplicate':
            if sent_duplicates:
                event = dict(random.choice(list(sent_duplicates)))
            else:
                event['user_id'] = 'U12345'
                event['timestamp'] = datetime.utcnow().isoformat() + 'Z'

    # LATE event
    if event_type == 'late':
        event['timestamp'] = (datetime.utcnow() - timedelta(minutes=random.randint(4,10))).isoformat() + 'Z'

    sent_duplicates.add(tuple(event.items()))
    return event

# Send event loop
while True:
    e = generate_event()
    producer.send(topic, e)
    print(f"Sent: {e}")
    time.sleep(random.uniform(1,2))