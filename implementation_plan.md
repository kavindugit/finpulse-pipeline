# Section 1: Synthetic Data Generator + Kafka Producer

## Overview

Section 1 is the heartbeat of the entire FinPulse pipeline. Its job is to continuously emit realistic, fraud-annotated financial transaction events into a Kafka topic (`transactions.raw`) so that every downstream component (Spark streaming, raw lake, dashboards) has a live, meaningful data source.

A skeleton `producer.py` and the PaySim seed dataset already exist. This plan upgrades what's there into a production-quality, demo-ready component.

---

## Current State Assessment

| File | Status | Notes |
|---|---|---|
| [producer.py](file:///d:/finpulse-pipeline/generator/producer.py) | ✅ Exists — needs hardening | Core classes exist; has several gaps noted below |
| [requirements.txt](file:///d:/finpulse-pipeline/generator/requirements.txt) | ⚠️ Incomplete | Missing `pandas`, `numpy`; `kafka-python` is deprecated |
| `generator/paysim_seed/paysim dataset.csv` | ✅ Exists | 493 MB — full dataset present |
| [docker-compose.yml](file:///d:/finpulse-pipeline/docker-compose.yml) | ⚠️ Incomplete | Kafka service exists but no `generator` service; missing `kafka-ui` for demo |
| [tests/test_fraud_rules.py](file:///d:/finpulse-pipeline/tests/test_fraud_rules.py) | ❌ Empty | Needs unit tests for all anomaly injection logic |
| `generator/config.py` | ❌ Missing | No central config — constants scattered in producer.py |

### Gaps in the existing `producer.py`

1. **PaySim CSV is ignored** — `BankState` generates synthetic accounts from scratch using Faker; the real PaySim dataset on disk is never loaded or used as a statistical seed
2. **`step_counter` never increments for normal transactions** — the `if self.step_counter % 100 == 0` block increments correctly only every 100 steps, but each transaction should always increment the counter
3. **No `transaction_id`** — events have no unique ID field, making downstream deduplication and reconciliation impossible
4. **No Dockerfile** — the generator can't be containerized and run alongside Kafka via `docker-compose up`
5. **Hardcoded broker address** — `localhost:9092` breaks inside Docker (should be `kafka:29092`)
6. **No `kafka-ui`** — the docker-compose has no visual tool to verify events are flowing into Kafka during a demo
7. **`odd_hour` anomaly has a bug** — it replaces the timestamp but uses the generator's `current_time`, meaning the overall simulation clock is not advanced
8. **No `transaction_id` or `ingestion_timestamp`** — downstream Spark / dbt models need a stable primary key

---

## Proposed Changes

### 1. `generator/config.py` — [NEW]

Central configuration file to eliminate all magic constants.

```
KAFKA_BROKER_INTERNAL = "kafka:29092"   # used inside Docker
KAFKA_BROKER_EXTERNAL = "localhost:9092" # used when running locally
TOPIC_NAME = "transactions.raw"
NUM_ACCOUNTS = 1000
FRAUD_PROB = 0.05
PAYSIM_CSV_PATH = "/data/paysim dataset.csv"  # mounted in Docker
```

---

### 2. `generator/requirements.txt` — [MODIFY]

Replace `kafka-python` (unmaintained) with `confluent-kafka` (actively maintained, higher performance). Add `pandas` and `numpy` for PaySim sampling.

```
confluent-kafka==2.4.0
Faker==25.8.0
pandas==2.2.2
numpy==1.26.4
```

> [!IMPORTANT]
> `kafka-python` 2.0.2 has a [known bug with newer Kafka brokers](https://github.com/dpkp/kafka-python/issues/2412) that causes intermittent `NoBrokersAvailable` errors. Switching to `confluent-kafka` is strongly recommended before the demo.

---

### 3. `generator/paysim_loader.py` — [NEW]

A dedicated module that reads the PaySim CSV and exposes statistical distributions (amount per tx type, balance ranges) to seed the `BankState`. This replaces pure Faker-random initialization.

**Key responsibilities:**
- Load `paysim dataset.csv` into a pandas DataFrame on startup (or a 10% sample for speed)
- Compute per-type amount distributions (`mean`, `std`) for use in `TransactionGenerator`
- Expose a `sample_initial_balance()` function that returns a balance drawn from real PaySim customer balance data

---

### 4. `generator/producer.py` — [MODIFY]

**Changes:**
- Import from `config.py` instead of inline constants
- Use `confluent-kafka` `Producer` instead of `kafka-python`
- Add `transaction_id` (UUID4) to every event
- Add `ingestion_timestamp` (UTC ISO string, wall-clock time, separate from simulated `timestamp`)
- Fix `step_counter` so it increments on every transaction
- Accept `--broker` CLI flag so the same script runs locally (localhost) and inside Docker (kafka:29092) without code changes
- `BankState.__init__` optionally accepts a `paysim_loader` to seed balances realistically
- Fix `odd_hour` bug: after forcing the timestamp, do NOT advance the shared `current_time` clock
- Add a 4th anomaly type: **`geo_impossible`** — emits two transactions from the same account within 60 seconds but with a `location` field set to two different countries (e.g., `LK` then `US`)

**New event schema (full):**
```json
{
  "transaction_id": "uuid4",
  "step": 42,
  "timestamp": "2026-07-16T03:12:44",
  "ingestion_timestamp": "2026-07-16T17:55:01Z",
  "type": "TRANSFER",
  "amount": 87543.00,
  "nameOrig": "C1234567890",
  "oldbalanceOrg": 9500.00,
  "newbalanceOrig": -78043.00,
  "nameDest": "C9876543210",
  "oldbalanceDest": 2000.00,
  "newbalanceDest": 89543.00,
  "location": "LK",
  "isFraud": 1,
  "isFlaggedFraud": 0,
  "anomaly_type": "large_amount"
}
```

---

### 5. `generator/Dockerfile` — [NEW]

Containerizes the producer so it can be started with a single `docker-compose up`.

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "producer.py", "--broker", "kafka:29092"]
```

---

### 6. `docker-compose.yml` — [MODIFY]

**Changes:**
- Add `generator` service that builds from `./generator` and depends on the `kafka` healthcheck
- Add `kafka-ui` (Provectus) service on port `8090` for live visual verification of events flowing into `transactions.raw`
- Add a proper Kafka healthcheck so dependent services wait correctly
- Mount `paysim_seed/` as a volume into the generator container at `/data/`

```yaml
generator:
  build: ./generator
  depends_on:
    kafka:
      condition: service_healthy
  volumes:
    - ./generator/paysim_seed:/data
  environment:
    KAFKA_BROKER: kafka:29092

kafka-ui:
  image: provectuslabs/kafka-ui:latest
  ports:
    - "8090:8080"
  environment:
    KAFKA_CLUSTERS_0_NAME: finpulse
    KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
  depends_on:
    - kafka
```

---

### 7. `tests/test_fraud_rules.py` — [MODIFY]

Populate the currently empty test file with unit tests that verify anomaly injection logic **without** needing a live Kafka broker (pure Python, no Docker required).

**Test cases to implement:**

| Test | What it verifies |
|---|---|
| `test_large_amount_exceeds_threshold` | `_inject_large_amount` produces `amount >= 50000` |
| `test_large_amount_is_fraud` | `isFraud == 1`, `anomaly_type == 'large_amount'` |
| `test_odd_hour_timestamp` | Timestamp hour is between 2–4 |
| `test_odd_hour_does_not_advance_clock` | Generator's `current_time` is unchanged after odd_hour injection |
| `test_rapid_fire_sequence_length` | A single `_setup_rapid_fire` call produces 4–8 consecutive transactions |
| `test_rapid_fire_same_origin` | All rapid-fire transactions share the same `nameOrig` |
| `test_rapid_fire_short_interval` | Time delta between rapid-fire events is ≤ 2 seconds |
| `test_normal_transaction_not_fraud` | A normal transaction has `isFraud == 0` |
| `test_transaction_has_unique_id` | Every transaction contains a `transaction_id` field |
| `test_step_counter_increments` | `step_counter` increments by 1 on every call |

---

## Infrastructure Diagram

```
┌────────────────────────────────────────────────────────────┐
│                    docker-compose up                        │
│                                                            │
│  ┌──────────────┐    ┌────────────────────────────────┐   │
│  │  zookeeper   │◄───│           kafka                │   │
│  │  :2181       │    │   :9092 (host) :29092 (int)   │   │
│  └──────────────┘    └────────────┬───────────────────┘   │
│                                   │ transactions.raw topic │
│  ┌──────────────────────────┐     │                       │
│  │    generator             │─────┘                       │
│  │  producer.py             │      ┌──────────────────┐   │
│  │  paysim_loader.py        │      │   kafka-ui        │   │
│  │  /data/paysim dataset.csv│      │   :8090           │   │
│  └──────────────────────────┘      └──────────────────┘   │
└────────────────────────────────────────────────────────────┘
```

---

## Open Questions

> [!IMPORTANT]
> **PaySim loading strategy**: The CSV is 493 MB. Loading the full file on startup will add ~5–10 seconds of startup time inside Docker. Should we:
> - **(A)** Load a 10% random sample on startup (fast, slightly less representative)
> - **(B)** Pre-process the CSV once into a smaller Parquet file and commit that to the repo (recommended for demo speed)
> - **(C)** Load the full file (accurate, slower startup)

> [!NOTE]
> **`confluent-kafka` vs `kafka-python`**: The switch to `confluent-kafka` requires `librdkafka` to be installed, which means the Dockerfile needs `apt-get install -y librdkafka-dev`. This is handled automatically in the Dockerfile above but worth knowing if running locally on Windows (requires manual install or WSL).

---

## Verification Plan

### Automated Tests
```bash
# Run unit tests (no Kafka required)
cd d:\finpulse-pipeline
pip install pytest faker pandas numpy
pytest tests/test_fraud_rules.py -v
```
Expected: all 10 tests pass.

### Manual / Integration Verification
1. `docker-compose up --build generator kafka zookeeper kafka-ui`
2. Open **http://localhost:8090** → navigate to `finpulse` cluster → `transactions.raw` topic
3. Confirm messages are arriving; inspect a sample — verify:
   - `transaction_id` is present and unique
   - `isFraud == 1` records appear with `anomaly_type` set
   - `timestamp` and `ingestion_timestamp` are both present
4. Inject a rapid-fire burst by setting `FRAUD_PROB=1.0` temporarily; confirm 4–8 back-to-back events from the same `nameOrig`

---

## File Change Summary

| File | Action |
|---|---|
| [generator/producer.py](file:///d:/finpulse-pipeline/generator/producer.py) | MODIFY |
| [generator/requirements.txt](file:///d:/finpulse-pipeline/generator/requirements.txt) | MODIFY |
| `generator/config.py` | NEW |
| `generator/paysim_loader.py` | NEW |
| `generator/Dockerfile` | NEW |
| [docker-compose.yml](file:///d:/finpulse-pipeline/docker-compose.yml) | MODIFY |
| [tests/test_fraud_rules.py](file:///d:/finpulse-pipeline/tests/test_fraud_rules.py) | MODIFY |
