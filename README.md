# TP05 — Real-Time Data Streaming and Processing Using Apache Kafka

**Module:** Big Data Analytics  
**Level:** Second-Year Master — Artificial Intelligence & Data Science  
**Institution:** University of El Oued  
**Academic Year:** 2026/2027

## 1. Project Overview

This practical work implements a simulated smart-campus monitoring system using Apache Kafka, Python, and Docker Compose. Python producers generate synthetic temperature and humidity readings, publish them as JSON events to Kafka topics, and Python consumers process these events in near real time.

The work covers message production and consumption, high-temperature detection, running averages, consumer groups, topic partitions, building-based filtering, and CSV export.

> All sensor readings are synthetic and intended for educational use.

## 2. Technologies

- Apache Kafka 3.9.1, running in a single-broker KRaft configuration
- Docker Desktop and Docker Compose
- Python 3.10+ (tested with Python 3.11)
- `kafka-python==2.0.2`
- JSON for Kafka event payloads; CSV for exported records
- Visual Studio Code and PowerShell on Windows

## 3. Project Files

```text
tp05-kafka/
├── compose.yaml
├── requirements.txt
├── producer.py
├── consumer.py
├── producer_100.py
├── consumer_average.py
├── consumer_group_test.py
├── producer_partitions_test.py
├── producer_buildings.py
├── consumer_buildings.py
├── consumer_csv.py
├── campus_events.csv
├── README.md
└── Screenshots/
```

| File | Purpose |
|---|---|
| `compose.yaml` | Starts the single Kafka broker with Docker Compose. |
| `requirements.txt` | Python dependency (`kafka-python==2.0.2`). |
| `producer.py` | Original producer: publishes 30 simulated sensor events. |
| `consumer.py` | Original consumer: reads sensor events and alerts at temperatures >= 35°C. |
| `producer_100.py` | Experiment 1: publishes 100 events. |
| `consumer_average.py` | Experiment 2: calculates a running temperature average. |
| `consumer_group_test.py` | Experiment 3: compares different and shared consumer groups. |
| `producer_partitions_test.py` | Experiment 4: publishes 12 test events to a selected topic. |
| `producer_buildings.py` | Experiment 5: publishes 30 events with `campus_building`. |
| `consumer_buildings.py` | Experiment 5: filters for `Building-A` and detects high temperatures. |
| `consumer_csv.py` | Experiment 6: exports 30 events to CSV. |
| `campus_events.csv` | Exported events with the five required columns. |
| `Screenshots/` | Terminal screenshots documenting the experiments. |

The original `producer.py` and `consumer.py` were preserved; extension experiments use separate scripts.

## 4. Architecture

```text
Simulated Sensors (Python Producers)
                 |
                 v
       Apache Kafka Broker
          localhost:9092
                 |
       Kafka Topics / Partitions
                 |
                 v
       Python Kafka Consumers
          |       |       |
       Alerts  Filtering  CSV Export
```

Kafka decouples event producers from consumers. Topics organize events, partitions allow partition-level parallelism, and consumer groups coordinate partition assignments.

### Topics Used

| Topic | Partitions | Purpose |
|---|---:|---|
| `campus-sensors` | 3 | Original sensor streaming and consumer-group experiments. |
| `tp05-one-partition` | 1 | Single-partition comparison. |
| `tp05-three-partitions` | 3 | Three-partition comparison. |
| `campus-buildings` | Broker default (3 in this configuration) | Building filtering and CSV export. |

The setup has **one broker** and a **replication factor of 1**, so it does not provide broker-level fault tolerance.

## 5. Setup and Installation

### 5.1 Prerequisites

Install Docker Desktop, Python 3.10 or later, and Git (for GitHub submission). Run the following commands from the `tp05-kafka` directory.

### 5.2 Start Kafka

```powershell
docker compose up -d
docker compose ps
docker compose logs --tail=50 kafka
```

The Kafka bootstrap address for the Python scripts is `localhost:9092`. This is a **Kafka protocol endpoint, not a web page**; opening it in a browser will not show a website.

### 5.3 Create the Python Environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If the virtual environment is already installed, activate it instead of recreating it.

### 5.4 Inspect or Create the Original Topic

```powershell
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-sensors --partitions 3 --replication-factor 1
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic campus-sensors
```

### 5.5 Run the Original Producer and Consumer

Open two terminals. Start the consumer in the first:

```powershell
python consumer.py
```

Start the producer in the second:

```powershell
python producer.py
```

The producer publishes 30 JSON events. The consumer prints each reading and an alert when `temperature_c >= 35`. Stop the consumer with `Ctrl+C` after observing the output.

## 6. Further Investigation Experiments

### Experiment 1 — 100 Events

```powershell
python producer_100.py
```

The extended producer successfully sent **100 events**. Combined with the initial 30-event run, the `campus-sensors` topic contained **130 events** at the time of the running-average experiment. Later reruns may add more messages.

### Experiment 2 — Running Average Temperature

```powershell
python consumer_average.py
```

The consumer maintains a cumulative sum and event count:

`running_average = total_temperature / event_count`

**Observed result:** 130 events processed; final average **29.88°C**; **40** events triggered the high-temperature threshold.

These figures describe the recorded experiment, not a guaranteed result of future runs with randomly generated data.

### Experiment 3 — Consumer Groups

Different consumer groups independently read the same events:

```powershell
python consumer_group_test.py group-A
python consumer_group_test.py group-B
```

Run the commands in separate terminals. In the recorded experiment, both groups read the same **130** historical events.

For consumers sharing a group, start the following command in **two separate terminals**:

```powershell
python consumer_group_test.py group-shared
```

Inspect their assignments:

```powershell
docker exec -it tp05-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group group-shared --members --verbose
```

**Observed result:** one consumer was assigned Partition 2, while the other received Partitions 0 and 1. Members of a single consumer group share partitions; separate groups maintain independent progress.

### Experiment 4 — One Partition vs. Three Partitions

Kafka does not support directly reducing the partition count of an existing topic. Two separate experimental topics were created to preserve the original topic:

```powershell
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic tp05-one-partition --partitions 1 --replication-factor 1
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic tp05-three-partitions --partitions 3 --replication-factor 1
```

Publish 12 events to each topic:

```powershell
python producer_partitions_test.py tp05-one-partition
python producer_partitions_test.py tp05-three-partitions
```

**Recorded results:**

| Topic | Partition 0 | Partition 1 | Partition 2 | Total |
|---|---:|---:|---:|---:|
| One partition | 12 | — | — | 12 |
| Three partitions | 2 | 7 | 3 | 12 |

Offsets are independent per partition. More partitions can enable more consumers in the same group to process data in parallel, but they do not create additional brokers or guarantee higher throughput in this single-broker setup. The distribution is not guaranteed to be equal.

### Experiment 5 — Filtering by Campus Building

```powershell
python consumer_buildings.py
```

In a second terminal:

```powershell
python producer_buildings.py
```

The producer adds a `campus_building` field and publishes 30 events across `Building-A`, `Building-B`, and `Building-C`. The consumer receives the events but prints only records belonging to `Building-A`.

**Recorded results:** 30 events received, 10 matched `Building-A`, and 3 matched events had temperatures >= 35°C (event IDs 7, 13, and 22). Filtering is performed in Python after Kafka delivers the events.

### Experiment 6 — CSV Export (Optional)

```powershell
python consumer_csv.py
```

The consumer reads previously stored events from `campus-buildings` and writes `campus_events.csv` with these columns:

```text
event_id,sensor_id,temperature_c,humidity_pct,timestamp
```

**Recorded result:** 30 data rows exported successfully. Verify in PowerShell:

```powershell
(Import-Csv .\campus_events.csv | Measure-Object).Count
```

Expected count for the recorded export: `30`.

The CSV script writes a new file on each run and stops after 30 events. If the topic contains fewer than 30 available events, it waits for additional events.

## 7. Results Summary

| Investigation | Result |
|---|---|
| Basic streaming | Producer and consumer exchanged JSON sensor events. |
| 100-event producer | 100 events published. |
| Running average | 130 events; 29.88°C average; 40 high-temperature alerts. |
| Consumer groups | Independent groups read the same data; shared group split 3 partitions as 1 + 2. |
| Partition comparison | 12/12 in Partition 0 for the 1-partition topic; 2/7/3 across the 3-partition topic. |
| Building filter | 30 received; 10 matched `Building-A`; 3 high-temperature alerts. |
| CSV export | 30 records, 5 columns. |

## 8. Evidence and Screenshots

The instructor requires evidence of the running container, topic description, producer output, and consumer output. Store the corresponding screenshots in `Screenshots/`:

- `docker-compose-status.png`
- `topic-description.png`
- `producer-output.png`
- `consumer-output.png`

Optional screenshots may document the 100-event run, running average, consumer groups, partition comparison, building filter, and CSV export. Include only files actually captured and verified.

## 9. Troubleshooting and Notes

- **Browser error at `localhost:9092`:** expected because the Kafka listener is not an HTTP web server. Verify connectivity with Kafka tools or the Python clients.
- **Consumer shows only `Listening to topic`:** it may have already consumed all available messages and be waiting for new ones. Run the producer again or use a fresh group ID when appropriate.
- **Messages appear out of timestamp order:** Kafka preserves order within a partition, not a global chronological order across partitions.
- **`Ctrl+C` stops the consumer:** expected for a continuously running stream consumer.
- **Docker Debug suggestion:** informational output from Docker, not an error.
- **Repeated runs:** new producer executions append events to existing topics; results can differ because readings are random.

## 10. Limitations

This is an educational, single-broker deployment with replication factor 1. It has no broker-level redundancy, does not demonstrate multi-broker failover, and uses synthetic rather than physical sensor measurements. The experiments demonstrate Kafka functionality, not a comprehensive performance benchmark.

## 11. Cleanup

Stop the Kafka container without deleting stored Kafka data:

```powershell
docker compose down
```

**Warning:** `docker compose down -v` also removes the named volume containing Kafka data. Use it only when the stored topics and messages are no longer needed.

## 12. References

- [Apache Kafka documentation](https://kafka.apache.org/documentation/)
- [Apache Kafka Docker image](https://hub.docker.com/r/apache/kafka)
- [kafka-python documentation](https://kafka-python.readthedocs.io/en/master/)
- [Docker Compose documentation](https://docs.docker.com/compose/)
