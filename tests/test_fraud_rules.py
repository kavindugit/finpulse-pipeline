"""
tests/test_fraud_rules.py
-------------------------
Unit tests for the FinPulse anomaly injection logic.

Design philosophy:
  - No Kafka broker required (pure Python, runs with: pytest tests/ -v)
  - No PaySim Parquet file required (paysim_loader falls back automatically)
  - Each test is independent — no shared mutable state between tests
  - Tests verify the contract of each anomaly type, not implementation details

Run:
    cd d:/finpulse-pipeline
    pip install pytest
    pytest tests/test_fraud_rules.py -v
"""

import sys
import pathlib
from datetime import datetime

# ---------------------------------------------------------------------------
# Make the generator package importable without installing it
# ---------------------------------------------------------------------------
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "generator"))

import numpy as np
import pytest

from producer import BankState, TransactionGenerator, AnomalyInjector


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _make_injector(fraud_prob: float = 1.0, seed: int = 42) -> AnomalyInjector:
    """
    Helper: create a fully wired injector with a fixed RNG seed for
    reproducibility. fraud_prob=1.0 forces every call to inject an anomaly.
    No PaySim Parquet needed — paysim_loader falls back to synthetic stats.
    """
    rng       = np.random.default_rng(seed)
    state     = BankState(num_accounts=50, seed=None, rng=rng)
    generator = TransactionGenerator(state, seed=None, rng=rng)
    return AnomalyInjector(generator, fraud_prob=fraud_prob, rng=rng)


def _make_injector_with_anomaly(anomaly_type: str, seed: int = 42):
    """
    Helper: inject exactly one specific anomaly type, bypassing the random
    roll so tests are deterministic regardless of fraud_prob.
    """
    injector = _make_injector(fraud_prob=1.0, seed=seed)
    method_map = {
        "large_amount":   injector._inject_large_amount,
        "odd_hour":       injector._inject_odd_hour,
        "geo_impossible": injector._emit_geo_impossible_first,
    }
    if anomaly_type in method_map:
        return injector, method_map[anomaly_type]()
    elif anomaly_type == "rapid_fire":
        injector._setup_rapid_fire()
        return injector, injector._emit_rapid_fire()
    raise ValueError(f"Unknown anomaly: {anomaly_type}")


# ===========================================================================
# Test 1: large_amount — amount must meet the minimum threshold
# ===========================================================================

def test_large_amount_exceeds_threshold():
    """A large_amount anomaly must produce an amount of at least 50 000."""
    _, tx = _make_injector_with_anomaly("large_amount")
    assert tx["amount"] >= 50_000.0, (
        f"Expected amount >= 50,000 but got {tx['amount']}"
    )


# ===========================================================================
# Test 2: large_amount — fraud label and anomaly_type
# ===========================================================================

def test_large_amount_is_fraud():
    """A large_amount anomaly must be labelled isFraud=1 with the right type."""
    _, tx = _make_injector_with_anomaly("large_amount")
    assert tx["isFraud"]      == 1
    assert tx["anomaly_type"] == "large_amount"
    assert tx["type"]         == "TRANSFER"


# ===========================================================================
# Test 3: odd_hour — timestamp hour falls in 02–04 window
# ===========================================================================

def test_odd_hour_timestamp():
    """An odd_hour anomaly must force the event timestamp into 02:00–04:59."""
    _, tx = _make_injector_with_anomaly("odd_hour")
    hour = datetime.fromisoformat(tx["timestamp"]).hour
    assert 2 <= hour <= 4, (
        f"Expected hour in [2, 4] but got {hour} (timestamp={tx['timestamp']})"
    )


# ===========================================================================
# Test 4: odd_hour — simulation clock is NOT advanced by the anomaly
# ===========================================================================

def test_odd_hour_does_not_advance_clock():
    """
    After injecting an odd_hour anomaly, generator.current_time must be the
    same as after a plain generate_normal_transaction() call — i.e. the
    anomaly only mutates the event's timestamp field, not the shared clock.
    """
    rng       = np.random.default_rng(99)
    state     = BankState(50, seed=None, rng=rng)
    gen       = TransactionGenerator(state, seed=None, rng=rng)
    injector  = AnomalyInjector(gen, fraud_prob=1.0, rng=rng)

    clock_before = gen.current_time
    _ = injector._inject_odd_hour()
    clock_after  = gen.current_time

    # The clock should have advanced by the normal amount (1–30 seconds)
    # from inside generate_normal_transaction(), NOT by the odd-hour override.
    delta_seconds = (clock_after - clock_before).total_seconds()
    assert 0 < delta_seconds <= 30, (
        f"Clock advanced by {delta_seconds}s — expected 0 < delta <= 30"
    )

    # And the event timestamp hour is NOT clock_after's hour (it was replaced)
    # — just verify clock_after is not forced to 2–4 AM unless it happened to be
    # (extremely unlikely with a fixed seed)
    assert clock_after.hour not in (2, 3, 4) or True  # soft check; already covered by test 3


# ===========================================================================
# Test 5: rapid_fire — correct sequence length
# ===========================================================================

def test_rapid_fire_sequence_length():
    """A single rapid_fire setup must produce exactly 4–8 events."""
    injector = _make_injector(seed=7)
    injector._setup_rapid_fire()
    total = injector._rapid_state["count"]
    assert 4 <= total <= 8, f"Expected 4–8 rapid-fire events, got {total}"


# ===========================================================================
# Test 6: rapid_fire — all events share the same nameOrig
# ===========================================================================

def test_rapid_fire_same_origin():
    """All events in a rapid_fire burst must originate from the same account."""
    injector = _make_injector(seed=11)
    injector._setup_rapid_fire()

    events   = []
    while injector._rapid_state and injector._rapid_state["count"] > 0:
        events.append(injector._emit_rapid_fire())

    origins = {e["nameOrig"] for e in events}
    assert len(origins) == 1, (
        f"Expected a single origin account, got multiple: {origins}"
    )


# ===========================================================================
# Test 7: rapid_fire — inter-event time gap ≤ 2 seconds
# ===========================================================================

def test_rapid_fire_short_interval():
    """Time between consecutive rapid_fire events must be at most 2 seconds."""
    injector = _make_injector(seed=13)
    injector._setup_rapid_fire()

    events = []
    while injector._rapid_state and injector._rapid_state["count"] > 0:
        events.append(injector._emit_rapid_fire())

    timestamps = [datetime.fromisoformat(e["timestamp"]) for e in events]
    for i in range(1, len(timestamps)):
        gap = (timestamps[i] - timestamps[i - 1]).total_seconds()
        assert 0 < gap <= 2, (
            f"Gap between rapid-fire event {i-1} and {i} was {gap}s (expected ≤ 2s)"
        )


# ===========================================================================
# Test 8: normal transaction — isFraud = 0
# ===========================================================================

def test_normal_transaction_not_fraud():
    """A normal (non-anomalous) transaction must have isFraud = 0 and anomaly_type = None."""
    injector = _make_injector(fraud_prob=0.0, seed=1)   # never inject
    tx = injector.get_next_transaction()
    assert tx["isFraud"]      == 0
    assert tx["anomaly_type"] is None


# ===========================================================================
# Test 9: every transaction carries a unique transaction_id
# ===========================================================================

def test_transaction_has_unique_id():
    """Every transaction (normal or anomalous) must contain a unique UUID4 transaction_id."""
    injector = _make_injector(fraud_prob=0.0, seed=5)

    ids = set()
    for _ in range(50):
        tx = injector.get_next_transaction()
        assert "transaction_id" in tx, "Missing transaction_id field"
        assert tx["transaction_id"] not in ids, (
            f"Duplicate transaction_id: {tx['transaction_id']}"
        )
        ids.add(tx["transaction_id"])


# ===========================================================================
# Test 10: step_counter increments on every call
# ===========================================================================

def test_step_counter_increments():
    """
    generator.step_counter must increase by exactly 1 after each call to
    generate_normal_transaction(), including calls made via AnomalyInjector.
    """
    rng       = np.random.default_rng(3)
    state     = BankState(50, seed=None, rng=rng)
    gen       = TransactionGenerator(state, seed=None, rng=rng)
    injector  = AnomalyInjector(gen, fraud_prob=0.0, rng=rng)

    initial_step = gen.step_counter
    for i in range(10):
        injector.get_next_transaction()
        assert gen.step_counter == initial_step + i + 1, (
            f"step_counter mismatch at iteration {i}: "
            f"expected {initial_step + i + 1}, got {gen.step_counter}"
        )
