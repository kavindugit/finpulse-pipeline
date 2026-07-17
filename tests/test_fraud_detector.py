"""
test_fraud_detector.py
-----------------------
Unit tests for the FinPulse Spark Section 2 fraud rule engine.

Tests are intentionally written against streaming/rules.py (pure Python),
NOT against the Spark job itself, so they run in < 1 second with no
SparkSession, no Kafka, no Postgres required.

Run with:
    pytest tests/test_fraud_detector.py -v
"""

import sys
import os

# Allow importing from streaming/ without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "streaming"))

from rules import (
    rule_large_amount,
    rule_rapid_fire,
    rule_odd_hour,
    rule_geo_impossible,
    apply_rules,
    LARGE_AMOUNT_THRESHOLD,
    RAPID_FIRE_COUNT_THRESHOLD,
)


# ===========================================================================
# rule_large_amount
# ===========================================================================

class TestLargeAmountRule:
    def test_triggers_at_threshold(self):
        """Amount exactly at threshold should be flagged."""
        flagged, name = rule_large_amount(LARGE_AMOUNT_THRESHOLD)
        assert flagged is True
        assert name == "large_amount"

    def test_triggers_above_threshold(self):
        """Amount well above threshold should be flagged."""
        flagged, name = rule_large_amount(1_000_000.0)
        assert flagged is True
        assert name == "large_amount"

    def test_does_not_trigger_below_threshold(self):
        """Amount just below threshold should NOT be flagged."""
        flagged, name = rule_large_amount(LARGE_AMOUNT_THRESHOLD - 0.01)
        assert flagged is False
        assert name is None


# ===========================================================================
# rule_rapid_fire
# ===========================================================================

class TestRapidFireRule:
    def test_triggers_at_threshold(self):
        flagged, name = rule_rapid_fire(RAPID_FIRE_COUNT_THRESHOLD)
        assert flagged is True
        assert name == "rapid_fire"

    def test_triggers_above_threshold(self):
        flagged, name = rule_rapid_fire(10)
        assert flagged is True
        assert name == "rapid_fire"

    def test_does_not_trigger_below_threshold(self):
        flagged, name = rule_rapid_fire(RAPID_FIRE_COUNT_THRESHOLD - 1)
        assert flagged is False
        assert name is None


# ===========================================================================
# rule_odd_hour
# ===========================================================================

class TestOddHourRule:
    def test_triggers_at_midnight(self):
        flagged, name = rule_odd_hour(0)
        assert flagged is True
        assert name == "odd_hour"

    def test_triggers_at_3am(self):
        flagged, name = rule_odd_hour(3)
        assert flagged is True
        assert name == "odd_hour"

    def test_triggers_at_4am(self):
        """4 AM is the last odd hour."""
        flagged, name = rule_odd_hour(4)
        assert flagged is True
        assert name == "odd_hour"

    def test_does_not_trigger_at_5am(self):
        flagged, name = rule_odd_hour(5)
        assert flagged is False
        assert name is None

    def test_does_not_trigger_at_noon(self):
        flagged, name = rule_odd_hour(12)
        assert flagged is False
        assert name is None


# ===========================================================================
# rule_geo_impossible
# ===========================================================================

class TestGeoImpossibleRule:
    def test_triggers_when_flagged(self):
        flagged, name = rule_geo_impossible(1)
        assert flagged is True
        assert name == "geo_impossible"

    def test_does_not_trigger_when_clean(self):
        flagged, name = rule_geo_impossible(0)
        assert flagged is False
        assert name is None


# ===========================================================================
# apply_rules (composite engine)
# ===========================================================================

class TestApplyRules:
    """Tests that verify the priority ordering of the composite engine."""

    def _call(self, **overrides):
        defaults = dict(
            amount=1_000.0,
            tx_count_5m=1,
            hour_utc=12,
            geo_impossible=0,
        )
        defaults.update(overrides)
        return apply_rules(**defaults)

    def test_normal_transaction_not_flagged(self):
        flagged, name = self._call()
        assert flagged is False
        assert name is None

    def test_geo_impossible_takes_priority_over_large_amount(self):
        """When both geo_impossible and large_amount fire, geo_impossible wins."""
        flagged, name = self._call(
            geo_impossible=1,
            amount=1_000_000.0,
        )
        assert flagged is True
        assert name == "geo_impossible"

    def test_large_amount_takes_priority_over_rapid_fire(self):
        flagged, name = self._call(
            amount=1_000_000.0,
            tx_count_5m=10,
        )
        assert flagged is True
        assert name == "large_amount"

    def test_rapid_fire_takes_priority_over_odd_hour(self):
        flagged, name = self._call(
            tx_count_5m=5,
            hour_utc=2,
        )
        assert flagged is True
        assert name == "rapid_fire"

    def test_odd_hour_alone_fires(self):
        flagged, name = self._call(hour_utc=3)
        assert flagged is True
        assert name == "odd_hour"
