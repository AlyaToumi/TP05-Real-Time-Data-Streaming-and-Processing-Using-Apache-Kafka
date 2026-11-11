
import csv
import json
from kafka import KafkaConsumer

TOPIC = "campus-buildings"
CSV_FILE = "campus_events.csv"
MAX_EVENTS = 30

FIELDS = [
    "event_id",
    "sensor_id",
    "temperature_c",
    "humidity_pct",
    "timestamp",
]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="tp05-csv-export",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)

count = 0

print(f"Reading events from: {TOPIC}")
print(f"Saving events to: {CSV_FILE}")

try:
    with open(
        CSV_FILE,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=FIELDS
        )

        writer.writeheader()

        for message in consumer:
            event = message.value

            row = {
                field: event.get(field, "")
                for field in FIELDS
            }

            writer.writerow(row)
            count += 1

            print(
                f"Saved Event {count}: "
                f"Sensor={row['sensor_id']} | "
                f"T={row['temperature_c']}°C"
            )

            if count >= MAX_EVENTS:
                break

    print(f"\nFinished! Saved {count} events.")
    print(f"CSV file created: {CSV_FILE}")

except KeyboardInterrupt:
    print("\nExport interrupted by user.")

finally:
    consumer.close()
   