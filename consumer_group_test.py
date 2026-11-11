
import json
import sys
from kafka import KafkaConsumer

TOPIC = "campus-sensors"

group_id = sys.argv[1] if len(sys.argv) > 1 else "tp05-test-group"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=group_id,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Listening to topic: {TOPIC}")
print(f"Consumer Group: {group_id}")

try:
    for message in consumer:
        event = message.value

        print(
            f"Group={group_id} | "
            f"Partition={message.partition} | "
            f"Offset={message.offset} | "
            f"Sensor={event['sensor_id']} | "
            f"T={event['temperature_c']}°C"
        )

except KeyboardInterrupt:
    print("Consumer stopped by user.")

finally:
    consumer.close()




