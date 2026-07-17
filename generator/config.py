"""
config.py
---------
Central configuration for the FinPulse data generator.
All constants live here — nothing is hardcoded in producer.py.

Environment variable overrides are supported so Docker / CI can inject
values without rebuilding the image.
"""

import os

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
# When running inside Docker the broker is reachable on the internal
# listener (kafka:29092). When running locally it's localhost:9092.
# Pass --broker <addr> on the CLI or set this env var.
KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME: str   = os.getenv("TOPIC_NAME", "transactions.raw")

# ---------------------------------------------------------------------------
# Simulation parameters
# ---------------------------------------------------------------------------
NUM_ACCOUNTS: int           = int(os.getenv("NUM_ACCOUNTS", "1000"))
FRAUD_PROB: float           = float(os.getenv("FRAUD_PROB", "0.05"))

# Min sleep between messages in seconds (controls throughput)
SLEEP_MIN: float            = float(os.getenv("SLEEP_MIN", "0.1"))
SLEEP_MAX: float            = float(os.getenv("SLEEP_MAX", "1.5"))

# ---------------------------------------------------------------------------
# PaySim seed file
# ---------------------------------------------------------------------------
# Parquet file produced by scripts/convert_paysim.py
# Mounted into Docker at /data/paysim_seed.parquet
PAYSIM_PARQUET_PATH: str = os.getenv(
    "PAYSIM_PARQUET_PATH",
    "generator/paysim_seed/paysim_seed.parquet",   # local default
)

# Transaction type weights derived from the real PaySim distribution
# (CASH-OUT ~35%, PAYMENT ~34%, CASH-IN ~22%, TRANSFER ~8%, DEBIT ~1%)
# These will be overridden by the actual Parquet data when loaded.
FALLBACK_TX_WEIGHTS: dict = {
    "CASH-IN":   0.22,
    "CASH-OUT":  0.35,
    "PAYMENT":   0.34,
    "TRANSFER":  0.08,
    "DEBIT":     0.01,
}
