
import json
from kafka import KafkaConsumer

TOPIC = "campus-buildings"
TARGET_BUILDING = "Building-A"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tp05-building-filter",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

received = 0
matched = 0

print(f"Listening to topic: {TOPIC}")
print(f"Filtering events from: {TARGET_BUILDING}")

try:
    for message in consumer:
        event = message.value
        received += 1

        if event.get("campus_building") == TARGET_BUILDING:
            matched += 1

            print(
                f"Event={event['event_id']} | "
                f"Building={event['campus_building']} | "
                f"Sensor={event['sensor_id']} | "
                f"T={event['temperature_c']}°C"
            )

            if event["temperature_c"] >= 35:
                print("ALERT: High temperature detected!")

        print(f"Received: {received} | Matched: {matched}")

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")
    print(f"Total received: {received}")
    print(f"Building-A events: {matched}")

finally:
    consumer.close()
