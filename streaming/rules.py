"""
rules.py
--------
Pure-Python fraud detection rules for FinPulse.

These functions operate on plain dicts (no Spark dependency) so they can be:
  1. Unit-tested without a SparkSession (fast CI)
  2. Imported by fraud_detector.py and applied inside a foreachBatch callback

Each rule function takes a row dict and optional window-aggregation context,
and returns a (is_fraud: bool, rule_name: str | None) tuple.
"""

from __future__ import annotations

from typing import Optional

# ---------------------------------------------------------------------------
# Thresholds — keep in sync with generator/config.py anomaly parameters
# ---------------------------------------------------------------------------

LARGE_AMOUNT_THRESHOLD: float = 500_000.0   # absolute amount trigger
RAPID_FIRE_COUNT_THRESHOLD: int = 3          # tx count in 5-min window
ODD_HOURS: frozenset[int] = frozenset({0, 1, 2, 3, 4})  # midnight–4 am UTC


# ---------------------------------------------------------------------------
# Individual rule functions
# ---------------------------------------------------------------------------

def rule_large_amount(amount: float) -> tuple[bool, Optional[str]]:
    """Flag a single transaction whose amount exceeds the threshold."""
    if amount >= LARGE_AMOUNT_THRESHOLD:
        return True, "large_amount"
    return False, None


def rule_rapid_fire(tx_count_5m: int) -> tuple[bool, Optional[str]]:
    """Flag when an account sends >= threshold transactions in a 5-min window."""
    if tx_count_5m >= RAPID_FIRE_COUNT_THRESHOLD:
        return True, "rapid_fire"
    return False, None


def rule_odd_hour(hour_utc: int) -> tuple[bool, Optional[str]]:
    """Flag transactions that occur in the early-morning odd hours (0–4 UTC)."""
    if hour_utc in ODD_HOURS:
        return True, "odd_hour"
    return False, None


def rule_geo_impossible(geo_impossible: int) -> tuple[bool, Optional[str]]:
    """Flag transactions tagged as geographically impossible by the producer."""
    if geo_impossible == 1:
        return True, "geo_impossible"
    return False, None


# ---------------------------------------------------------------------------
# Composite rule engine
# ---------------------------------------------------------------------------

def apply_rules(
    *,
    amount: float,
    tx_count_5m: int,
    hour_utc: int,
    geo_impossible: int,
) -> tuple[bool, Optional[str]]:
    """
    Run all rules and return the first match.

    Priority order (most severe first):
      1. geo_impossible
      2. large_amount
      3. rapid_fire
      4. odd_hour

    Returns (True, rule_name) on first match, else (False, None).
    """
    for check_fn, kwargs in [
        (rule_geo_impossible, {"geo_impossible": geo_impossible}),
        (rule_large_amount,   {"amount": amount}),
        (rule_rapid_fire,     {"tx_count_5m": tx_count_5m}),
        (rule_odd_hour,       {"hour_utc": hour_utc}),
    ]:
        flagged, name = check_fn(**kwargs)
        if flagged:
            return True, name

    return False, None
