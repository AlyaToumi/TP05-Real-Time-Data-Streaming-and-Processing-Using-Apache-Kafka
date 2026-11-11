
import json
from kafka import KafkaConsumer

TOPIC = "campus-sensors"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tp05-campus-average",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

total_temperature = 0
event_count = 0

print("Listening to topic:", TOPIC)
print("Calculating running average temperature...")

try:
    for message in consumer:
        event = message.value

        sensor_id = event["sensor_id"]
        temperature = event["temperature_c"]
        humidity = event["humidity_pct"]
        timestamp = event["timestamp"]

        event_count += 1
        total_temperature += temperature
        running_average = total_temperature / event_count

        print(
            f"{timestamp} | {sensor_id} | "
            f"T={temperature}°C | RH={humidity}%"
        )

        print(
            f"Running Average: {running_average:.2f}°C "
            f"| Events processed: {event_count}"
        )

        if temperature >= 35:
            print("ALERT: High temperature detected!")

finally:
    consumer.close()


