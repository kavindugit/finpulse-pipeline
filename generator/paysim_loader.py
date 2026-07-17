"""
paysim_loader.py
----------------
Loads the pre-processed PaySim Parquet seed file and exposes statistical
distributions so BankState and TransactionGenerator produce data that
mirrors real mobile-money transaction patterns.

Industry pattern:
    Convert raw source once (scripts/convert_paysim.py) → commit compact
    Parquet → load at runtime in <1 second. Never ship the 493 MB CSV
    into a Docker image or read it on every startup.
"""

import logging
import pathlib
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from config import FALLBACK_TX_WEIGHTS, PAYSIM_PARQUET_PATH

logger = logging.getLogger(__name__)


@dataclass
class TxTypeStats:
    """Statistical summary for a single transaction type."""
    name: str
    weight: float          # proportion of all transactions
    amount_mean: float
    amount_std: float
    amount_min: float
    amount_max: float

    def sample_amount(self, rng: np.random.Generator) -> float:
        """Draw a transaction amount from a log-normal approximation."""
        # Log-normal is a good fit for financial amounts (long right tail)
        log_mean = np.log(max(self.amount_mean, 1.0))
        log_std  = max(np.log1p(self.amount_std / max(self.amount_mean, 1.0)), 0.1)
        amount   = rng.lognormal(log_mean, log_std)
        # Clamp to observed range
        return float(np.clip(amount, self.amount_min, self.amount_max))


@dataclass
class PaySimSeed:
    """
    Holds all statistical distributions extracted from the PaySim dataset.
    Consumers call helper methods; they never touch the raw DataFrame.
    """
    tx_stats: Dict[str, TxTypeStats]
    balance_p5: float    # 5th percentile of customer opening balances
    balance_p95: float   # 95th percentile
    balance_mean: float
    balance_std: float
    tx_types: List[str] = field(init=False)
    tx_weights: List[float] = field(init=False)

    def __post_init__(self):
        self.tx_types   = list(self.tx_stats.keys())
        self.tx_weights = [s.weight for s in self.tx_stats.values()]

    def sample_initial_balance(self, rng: np.random.Generator) -> float:
        """
        Return a starting balance drawn from the real PaySim balance
        distribution (log-normal fit on oldbalanceOrg).
        """
        log_mean = np.log(max(self.balance_mean, 1.0))
        log_std  = max(np.log1p(self.balance_std / max(self.balance_mean, 1.0)), 0.3)
        balance  = rng.lognormal(log_mean, log_std)
        return float(np.clip(balance, self.balance_p5, self.balance_p95))

    def sample_tx_type(self, rng: np.random.Generator) -> str:
        """Randomly pick a transaction type according to PaySim proportions."""
        return rng.choice(self.tx_types, p=self.tx_weights)

    def sample_amount(self, tx_type: str, rng: np.random.Generator) -> float:
        """Draw a transaction amount for the given type."""
        return self.tx_stats[tx_type].sample_amount(rng)


def load(parquet_path: str = PAYSIM_PARQUET_PATH) -> PaySimSeed:
    """
    Load the Parquet seed file and return a PaySimSeed object.

    Falls back to hardcoded defaults (FALLBACK_TX_WEIGHTS) if the file is
    not found, so the producer can still run for unit tests and local dev
    without requiring the seed data.
    """
    path = pathlib.Path(parquet_path)

    if not path.exists():
        logger.warning(
            "PaySim Parquet seed not found at '%s'. "
            "Using fallback distributions. Run scripts/convert_paysim.py first.",
            parquet_path,
        )
        return _build_fallback_seed()

    logger.info("Loading PaySim seed from '%s'…", path)
    df = pd.read_parquet(path, engine="pyarrow")
    logger.info("Loaded %d rows in seed file.", len(df))
    return _build_seed_from_df(df)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _build_seed_from_df(df: pd.DataFrame) -> PaySimSeed:
    tx_stats: Dict[str, TxTypeStats] = {}
    total = len(df)

    for tx_type, group in df.groupby("type", observed=True):
        amounts = group["amount"].dropna()
        tx_stats[str(tx_type)] = TxTypeStats(
            name        = str(tx_type),
            weight      = len(group) / total,
            amount_mean = float(amounts.mean()),
            amount_std  = float(amounts.std()),
            amount_min  = float(amounts.quantile(0.01)),
            amount_max  = float(amounts.quantile(0.99)),
        )

    # Balance stats from account opening balances (non-zero rows only)
    balances = df.loc[df["oldbalanceOrg"] > 0, "oldbalanceOrg"]
    seed = PaySimSeed(
        tx_stats      = tx_stats,
        balance_p5    = float(balances.quantile(0.05)),
        balance_p95   = float(balances.quantile(0.95)),
        balance_mean  = float(balances.mean()),
        balance_std   = float(balances.std()),
    )

    logger.info(
        "PaySim seed ready. Types: %s",
        {k: f"{v.weight:.1%}" for k, v in seed.tx_stats.items()},
    )
    return seed


def _build_fallback_seed() -> PaySimSeed:
    """Synthetic fallback when the Parquet file is absent."""
    tx_stats: Dict[str, TxTypeStats] = {}
    for name, weight in FALLBACK_TX_WEIGHTS.items():
        tx_stats[name] = TxTypeStats(
            name        = name,
            weight      = weight,
            amount_mean = 50_000.0,
            amount_std  = 80_000.0,
            amount_min  = 1.0,
            amount_max  = 500_000.0,
        )
    return PaySimSeed(
        tx_stats      = tx_stats,
        balance_p5    = 100.0,
        balance_p95   = 50_000.0,
        balance_mean  = 10_000.0,
        balance_std   = 20_000.0,
    )
