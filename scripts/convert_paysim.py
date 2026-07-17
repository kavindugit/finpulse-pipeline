"""
convert_paysim.py
-----------------
One-time utility to convert the raw PaySim CSV (493 MB) into a compact
Parquet seed file (~15 MB) that the generator loads at runtime.

Industry rationale:
  - Parquet is columnar + Snappy-compressed → ~30x smaller than CSV
  - pandas reads it in <1 second vs 10–15 seconds for the raw CSV
  - The seed file captures the full statistical distribution (amounts,
    balances, tx-type proportions) without shipping gigabytes into Docker

Usage (run once from the repo root):
    pip install pandas pyarrow
    python scripts/convert_paysim.py

Output:
    generator/paysim_seed/paysim_seed.parquet
"""

import sys
import pathlib
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
CSV_PATH  = REPO_ROOT / "generator" / "paysim_seed" / "paysim dataset.csv"
OUT_PATH  = REPO_ROOT / "generator" / "paysim_seed" / "paysim_seed.parquet"

# ---------------------------------------------------------------------------
# Columns we actually need downstream (drop the rest to keep the file tiny)
# ---------------------------------------------------------------------------
KEEP_COLS = [
    "type",        # transaction type: CASH-IN, CASH-OUT, TRANSFER, PAYMENT, DEBIT
    "amount",      # transaction amount (LCU)
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
]

def main():
    if not CSV_PATH.exists():
        print(f"[ERROR] CSV not found at: {CSV_PATH}")
        sys.exit(1)

    print(f"[1/4] Reading CSV: {CSV_PATH}")
    print("      (this may take 10–15 seconds for a 493 MB file…)")

    df = pd.read_csv(
        CSV_PATH,
        usecols=KEEP_COLS,
        dtype={
            "type": "category",     # saves memory
            "isFraud": "int8",
            "isFlaggedFraud": "int8",
        },
    )

    n_rows = len(df)
    print(f"[2/4] Loaded {n_rows:,} rows. Type distribution:")
    for tx_type, count in df["type"].value_counts().items():
        print(f"      {tx_type:<12} {count:>8,}  ({count/n_rows*100:.1f}%)")

    print(f"[3/4] Writing Parquet -> {OUT_PATH}")
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, engine="pyarrow", compression="snappy", index=False)

    size_mb = OUT_PATH.stat().st_size / (1024 * 1024)
    print(f"[4/4] Done. Output size: {size_mb:.1f} MB  (was ~493 MB CSV)")
    print()
    print("Next step: commit generator/paysim_seed/paysim_seed.parquet to the repo.")
    print("The raw CSV should remain in .gitignore (too large for Git).")

if __name__ == "__main__":
    main()
