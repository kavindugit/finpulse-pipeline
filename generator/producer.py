"""
producer.py
-----------
FinPulse synthetic transaction producer.

Streams a continuous feed of realistic financial transactions into the
Kafka topic `transactions.raw`. Each event is a JSON object that mimics
the PaySim mobile-money schema, enriched with:

  - transaction_id      — UUID4; stable primary key for downstream dedup
  - ingestion_timestamp — wall-clock UTC ISO string (separate from the
                          simulated event timestamp)
  - location            — ISO-3166-1 alpha-2 country code
  - anomaly_type        — null | large_amount | odd_hour | rapid_fire |
                          geo_impossible

Architecture note:
  We write to Kafka (not directly to Postgres) so that:
    1. Multiple consumers (Spark, future services) can read independently
    2. Events are replayable if detection logic changes
    3. The producer is fully decoupled from the processing layer

Usage:
    # Local (Kafka at localhost:9092):
    python producer.py

    # Inside Docker (Kafka at kafka:29092):
    python producer.py --broker kafka:29092

    # Override fraud probability for testing:
    FRAUD_PROB=1.0 python producer.py
"""

import argparse
import json
import logging
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np

# ---------------------------------------------------------------------------
# Kafka library compatibility shim
# ---------------------------------------------------------------------------
# confluent-kafka (C extension) is used in Docker / Linux — fast & production-grade.
# kafka-python-ng (pure Python) is used on Windows for local dev — no C compiler needed.
# The rest of producer.py is identical either way; only _build_producer() differs.

try:
    from confluent_kafka import Producer as _ConfluentProducer, KafkaException
    _KAFKA_BACKEND = "confluent"
except ImportError:
    from kafka import KafkaProducer as _KafkaPythonProducer  # type: ignore[assignment]
    _KAFKA_BACKEND = "kafka-python-ng"

import config
import paysim_loader
from paysim_loader import PaySimSeed

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Country codes for geo_impossible anomaly
# ---------------------------------------------------------------------------
COUNTRIES = ["LK", "US", "GB", "IN", "SG", "AU", "DE", "NG", "AE", "JP"]


# ---------------------------------------------------------------------------
# BankState
# ---------------------------------------------------------------------------

class BankState:
    """
    In-memory ledger of synthetic customer accounts.

    Account IDs and opening balances are seeded from the PaySim
    statistical distributions when a PaySimSeed is provided; otherwise
    they fall back to uniform random values.
    """

    def __init__(
        self,
        num_accounts: int,
        seed: Optional[PaySimSeed] = None,
        rng: Optional[np.random.Generator] = None,
    ):
        self._rng = rng or np.random.default_rng()
        self.accounts: dict[str, float] = {}

        for _ in range(num_accounts):
            acc_id = f"C{self._rng.integers(1_000_000_000, 9_999_999_999)}"
            balance = (
                seed.sample_initial_balance(self._rng)
                if seed
                else float(self._rng.uniform(100.0, 100_000.0))
            )
            self.accounts[acc_id] = round(balance, 2)

        self._account_list = list(self.accounts.keys())

    def get_random_account(self) -> str:
        return self._rng.choice(self._account_list)

    def get_balance(self, acc_id: str) -> float:
        return self.accounts.get(acc_id, 0.0)

    def update_balance(self, acc_id: str, delta: float) -> float:
        self.accounts[acc_id] = round(self.accounts.get(acc_id, 0.0) + delta, 2)
        return self.accounts[acc_id]


# ---------------------------------------------------------------------------
# TransactionGenerator
# ---------------------------------------------------------------------------

class TransactionGenerator:
    """
    Produces realistic normal transactions using PaySim-derived distributions.
    """

    def __init__(
        self,
        state: BankState,
        seed: Optional[PaySimSeed] = None,
        rng: Optional[np.random.Generator] = None,
    ):
        self.state         = state
        self.seed          = seed
        self._rng          = rng or np.random.default_rng()
        self.step_counter  = 1
        self.current_time  = datetime.now()

    def generate_normal_transaction(self) -> dict:
        # Pick transaction type
        if self.seed:
            tx_type = self.seed.sample_tx_type(self._rng)
        else:
            tx_type = self._rng.choice(
                ["CASH-IN", "CASH-OUT", "DEBIT", "PAYMENT", "TRANSFER"],
                p=[0.22, 0.35, 0.01, 0.34, 0.08],
            )

        name_orig = self.state.get_random_account()
        name_dest = self.state.get_random_account()
        while name_dest == name_orig:
            name_dest = self.state.get_random_account()

        old_bal_orig = self.state.get_balance(name_orig)
        old_bal_dest = self.state.get_balance(name_dest)

        # Sample amount from PaySim distribution (or fallback uniform)
        if self.seed:
            amount = self.seed.sample_amount(tx_type, self._rng)
        else:
            max_amt = max(old_bal_orig * 0.9, 10.0)
            amount = round(float(self._rng.uniform(1.0, max_amt)), 2)

        amount = round(amount, 2)

        # Apply balance logic
        if tx_type == "CASH-IN":
            new_bal_orig = old_bal_orig + amount
            new_bal_dest = old_bal_dest
        else:
            new_bal_orig = old_bal_orig - amount
            new_bal_dest = old_bal_dest + amount if tx_type == "TRANSFER" else old_bal_dest

        # Persist balance changes
        self.state.update_balance(name_orig, new_bal_orig - old_bal_orig)
        if tx_type == "TRANSFER":
            self.state.update_balance(name_dest, new_bal_dest - old_bal_dest)

        tx = self._build_tx(
            step           = self.step_counter,
            timestamp      = self.current_time,
            tx_type        = tx_type,
            amount         = amount,
            name_orig      = name_orig,
            old_bal_orig   = old_bal_orig,
            new_bal_orig   = new_bal_orig,
            name_dest      = name_dest,
            old_bal_dest   = old_bal_dest,
            new_bal_dest   = new_bal_dest,
            location       = self._rng.choice(COUNTRIES),
            is_fraud       = 0,
            anomaly_type   = None,
        )

        # Always advance step counter and simulated clock
        self.step_counter += 1
        self.current_time += timedelta(seconds=int(self._rng.integers(1, 31)))
        return tx

    @staticmethod
    def _build_tx(
        *,
        step, timestamp, tx_type, amount,
        name_orig, old_bal_orig, new_bal_orig,
        name_dest, old_bal_dest, new_bal_dest,
        location, is_fraud, anomaly_type,
    ) -> dict:
        return {
            "transaction_id":     str(uuid.uuid4()),
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "step":               step,
            "timestamp":          timestamp.isoformat(),
            "type":               tx_type,
            "amount":             amount,
            "nameOrig":           name_orig,
            "oldbalanceOrg":      old_bal_orig,
            "newbalanceOrig":     new_bal_orig,
            "nameDest":           name_dest,
            "oldbalanceDest":     old_bal_dest,
            "newbalanceDest":     new_bal_dest,
            "location":           location,
            "isFraud":            is_fraud,
            "isFlaggedFraud":     0,
            "anomaly_type":       anomaly_type,
        }


# ---------------------------------------------------------------------------
# AnomalyInjector
# ---------------------------------------------------------------------------

class AnomalyInjector:
    """
    Wraps TransactionGenerator and probabilistically injects fraud anomalies.

    Four anomaly types:
      large_amount   — single TRANSFER that far exceeds the sender's balance
      odd_hour       — forces the event timestamp into the 02:00–04:00 window
                       WITHOUT advancing the shared simulation clock (bug-fix)
      rapid_fire     — 4–8 small transfers from the same account in quick
                       succession (< 2 seconds apart)
      geo_impossible — two transactions from the same account within 60 s
                       but with two different country codes (physically
                       impossible travel)
    """

    ANOMALY_TYPES = ["large_amount", "odd_hour", "rapid_fire", "geo_impossible"]

    def __init__(
        self,
        generator: TransactionGenerator,
        fraud_prob: float = config.FRAUD_PROB,
        rng: Optional[np.random.Generator] = None,
    ):
        self.generator      = generator
        self.fraud_prob     = fraud_prob
        self._rng           = rng or np.random.default_rng()
        self._rapid_state   = None    # type: Optional[dict]
        self._geo_state     = None    # type: Optional[dict]

    def get_next_transaction(self) -> dict:
        # Drain in-progress multi-event sequences first
        if self._rapid_state and self._rapid_state["count"] > 0:
            return self._emit_rapid_fire()
        if self._geo_state:
            return self._emit_geo_impossible_second()

        # Roll for anomaly
        if self._rng.random() < self.fraud_prob:
            anomaly = self._rng.choice(self.ANOMALY_TYPES)
            if anomaly == "large_amount":
                return self._inject_large_amount()
            elif anomaly == "odd_hour":
                return self._inject_odd_hour()
            elif anomaly == "rapid_fire":
                self._setup_rapid_fire()
                return self._emit_rapid_fire()
            elif anomaly == "geo_impossible":
                return self._emit_geo_impossible_first()

        return self.generator.generate_normal_transaction()

    # ------------------------------------------------------------------
    # large_amount
    # ------------------------------------------------------------------

    def _inject_large_amount(self) -> dict:
        tx = self.generator.generate_normal_transaction()
        # Override amount to be 5–10× the sender's original balance
        inflated = round(tx["oldbalanceOrg"] * float(self._rng.uniform(5, 10)), 2)
        amount   = max(inflated, 50_000.0)

        tx["type"]           = "TRANSFER"
        tx["amount"]         = amount
        tx["newbalanceOrig"] = round(tx["oldbalanceOrg"] - amount, 2)
        tx["newbalanceDest"] = round(tx["oldbalanceDest"] + amount, 2)
        tx["isFraud"]        = 1
        tx["anomaly_type"]   = "large_amount"

        # Sync state
        self.generator.state.update_balance(
            tx["nameOrig"], tx["newbalanceOrig"] - tx["oldbalanceOrg"]
        )
        self.generator.state.update_balance(
            tx["nameDest"], tx["newbalanceDest"] - tx["oldbalanceDest"]
        )
        return tx

    # ------------------------------------------------------------------
    # odd_hour  (BUG-FIX: do NOT advance the shared simulation clock)
    # ------------------------------------------------------------------

    def _inject_odd_hour(self) -> dict:
        tx = self.generator.generate_normal_transaction()
        # Replace only the timestamp field — the simulation clock
        # (generator.current_time) already advanced inside generate_normal_transaction
        odd_time     = self.generator.current_time.replace(
            hour   = int(self._rng.integers(2, 5)),
            minute = int(self._rng.integers(0, 60)),
            second = int(self._rng.integers(0, 60)),
        )
        tx["timestamp"]    = odd_time.isoformat()
        tx["isFraud"]      = 1
        tx["anomaly_type"] = "odd_hour"
        return tx

    # ------------------------------------------------------------------
    # rapid_fire
    # ------------------------------------------------------------------

    def _setup_rapid_fire(self):
        self._rapid_state = {
            "count":    int(self._rng.integers(4, 9)),   # 4–8 events
            "nameOrig": self.generator.state.get_random_account(),
            "time":     self.generator.current_time,
        }

    def _emit_rapid_fire(self) -> dict:
        state    = self._rapid_state
        nameOrig = state["nameOrig"]
        nameDest = self.generator.state.get_random_account()
        while nameDest == nameOrig:
            nameDest = self.generator.state.get_random_account()

        old_bal_orig = self.generator.state.get_balance(nameOrig)
        old_bal_dest = self.generator.state.get_balance(nameDest)
        amount       = round(float(self._rng.uniform(10.0, 100.0)), 2)
        new_bal_orig = old_bal_orig - amount
        new_bal_dest = old_bal_dest + amount

        self.generator.state.update_balance(nameOrig, new_bal_orig - old_bal_orig)
        self.generator.state.update_balance(nameDest, new_bal_dest - old_bal_dest)

        # Advance the rapid-fire clock by 1–2 seconds (tight burst)
        state["time"] += timedelta(seconds=int(self._rng.integers(1, 3)))

        tx = TransactionGenerator._build_tx(
            step         = self.generator.step_counter,
            timestamp    = state["time"],
            tx_type      = "TRANSFER",
            amount       = amount,
            name_orig    = nameOrig,
            old_bal_orig = old_bal_orig,
            new_bal_orig = new_bal_orig,
            name_dest    = nameDest,
            old_bal_dest = old_bal_dest,
            new_bal_dest = new_bal_dest,
            location     = self.generator._rng.choice(COUNTRIES),
            is_fraud     = 1,
            anomaly_type = "rapid_fire",
        )

        self.generator.step_counter += 1
        state["count"] -= 1
        if state["count"] == 0:
            self.generator.current_time = state["time"]
            self._rapid_state = None

        return tx

    # ------------------------------------------------------------------
    # geo_impossible  (two-event sequence)
    # ------------------------------------------------------------------

    def _emit_geo_impossible_first(self) -> dict:
        tx = self.generator.generate_normal_transaction()
        country_a = self._rng.choice(COUNTRIES)
        country_b = self._rng.choice([c for c in COUNTRIES if c != country_a])

        tx["location"]     = country_a
        tx["isFraud"]      = 1
        tx["anomaly_type"] = "geo_impossible"

        # Store context for the follow-up event
        self._geo_state = {
            "nameOrig":  tx["nameOrig"],
            "timestamp": self.generator.current_time,  # ~same time
            "country_b": country_b,
        }
        return tx

    def _emit_geo_impossible_second(self) -> dict:
        state    = self._geo_state
        nameOrig = state["nameOrig"]
        nameDest = self.generator.state.get_random_account()
        while nameDest == nameOrig:
            nameDest = self.generator.state.get_random_account()

        old_bal_orig = self.generator.state.get_balance(nameOrig)
        old_bal_dest = self.generator.state.get_balance(nameDest)
        amount       = round(float(self._rng.uniform(100.0, 5_000.0)), 2)
        new_bal_orig = old_bal_orig - amount
        new_bal_dest = old_bal_dest + amount

        self.generator.state.update_balance(nameOrig, new_bal_orig - old_bal_orig)
        self.generator.state.update_balance(nameDest, new_bal_dest - old_bal_dest)

        # Timestamp is within 60 seconds of the first event
        near_time = state["timestamp"] + timedelta(seconds=int(self._rng.integers(5, 61)))

        tx = TransactionGenerator._build_tx(
            step         = self.generator.step_counter,
            timestamp    = near_time,
            tx_type      = "TRANSFER",
            amount       = amount,
            name_orig    = nameOrig,
            old_bal_orig = old_bal_orig,
            new_bal_orig = new_bal_orig,
            name_dest    = nameDest,
            old_bal_dest = old_bal_dest,
            new_bal_dest = new_bal_dest,
            location     = state["country_b"],
            is_fraud     = 1,
            anomaly_type = "geo_impossible",
        )

        self.generator.step_counter += 1
        self._geo_state = None
        return tx


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def _delivery_report(err, msg):
    """Called by confluent-kafka after each message is acknowledged."""
    if err:
        logger.error("Delivery failed for record %s: %s", msg.key(), err)


class _KafkaPythonAdapter:
    """
    Thin adapter so the main loop can call .produce() / .poll() / .flush()
    regardless of whether confluent-kafka or kafka-python-ng is loaded.
    """
    def __init__(self, broker: str):
        self._prod = _KafkaPythonProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: v,
            key_serializer=lambda k: k,
        )

    def produce(self, topic, *, key, value, callback=None):
        self._prod.send(topic, key=key, value=value)

    def poll(self, _timeout):
        pass  # kafka-python flushes automatically per-message

    def flush(self, timeout=10):
        self._prod.flush(timeout=timeout)
        self._prod.close()


def _build_producer(broker: str):
    logger.info("Kafka backend: %s", _KAFKA_BACKEND)
    if _KAFKA_BACKEND == "confluent":
        return _ConfluentProducer({
            "bootstrap.servers": broker,
            "socket.timeout.ms": 10_000,
            "message.timeout.ms": 30_000,
        })
    else:
        return _KafkaPythonAdapter(broker)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="FinPulse transaction producer")
    parser.add_argument(
        "--broker",
        default=config.KAFKA_BROKER,
        help=f"Kafka bootstrap server (default: {config.KAFKA_BROKER})",
    )
    args = parser.parse_args()

    logger.info("Loading PaySim seed distributions…")
    seed = paysim_loader.load()

    logger.info("Initialising BankState with %d accounts…", config.NUM_ACCOUNTS)
    rng       = np.random.default_rng()
    state     = BankState(config.NUM_ACCOUNTS, seed=seed, rng=rng)
    generator = TransactionGenerator(state, seed=seed, rng=rng)
    injector  = AnomalyInjector(generator, fraud_prob=config.FRAUD_PROB, rng=rng)

    logger.info("Connecting to Kafka broker at %s…", args.broker)
    try:
        producer = _build_producer(args.broker)
    except KafkaException as exc:
        logger.error("Failed to create Kafka producer: %s", exc)
        sys.exit(1)

    logger.info("Streaming to topic '%s'. Press Ctrl+C to stop.", config.TOPIC_NAME)
    try:
        while True:
            tx      = injector.get_next_transaction()
            payload = json.dumps(tx).encode("utf-8")

            producer.produce(
                config.TOPIC_NAME,
                key=tx["transaction_id"].encode(),
                value=payload,
                callback=_delivery_report,
            )
            producer.poll(0)  # non-blocking flush of delivery callbacks

            if tx["isFraud"]:
                logger.warning(
                    "FRAUD  [%-16s] %-10s %12.2f  %s → %s",
                    tx["anomaly_type"], tx["type"], tx["amount"],
                    tx["nameOrig"], tx["nameDest"],
                )
            else:
                logger.info(
                    "NORMAL              %-10s %12.2f  %s → %s",
                    tx["type"], tx["amount"], tx["nameOrig"], tx["nameDest"],
                )

            time.sleep(random.uniform(config.SLEEP_MIN, config.SLEEP_MAX))

    except KeyboardInterrupt:
        logger.info("Interrupted — flushing remaining messages…")
    finally:
        producer.flush(timeout=10)
        logger.info("Producer closed cleanly.")


if __name__ == "__main__":
    main()
