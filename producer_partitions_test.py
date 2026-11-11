
import json
import sys
from kafka import KafkaProducer

topic = sys.argv[1] if len(sys.argv) > 1 else "tp05-one-partition"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
)

print(f"Sending events to: {topic}")

try:
    for event_id in range(1, 13):
        event = {
            "event_id": event_id,
            "sensor_id": f"sensor-{event_id:02d}",
            "temperature_c": 20 + event_id,
        }

        metadata = producer.send(topic, value=event).get(timeout=10)

        print(
            f"Event {event_id} | "
            f"Partition={metadata.partition} | "
            f"Offset={metadata.offset}"
        )

finally:
    producer.close()

print("Finished sending 12 events.")


