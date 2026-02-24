#!/usr/bin/env python3
"""
One-time setup script: imports Parquet files into a DuckDB database and
creates indexes for fast (targetId, diseaseId) point lookups.

Usage:
    uv run timeseries-build-db <parquet_dir> [--db PATH] [--skip-index]

Expected scale: ~160 files, ~760M rows.
  - Import:       ~5–10 min
  - Index build:  ~15–30 min
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import duckdb

DEFAULT_DB = Path(__file__).parent / "timeseries.db"


def build(parquet_dir: Path, db_path: Path, *, skip_index: bool = False) -> None:
    print(f"Source  : {parquet_dir}")
    print(f"Database: {db_path}")

    if db_path.exists():
        answer = input(f"\n{db_path} already exists. Overwrite? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            return
        db_path.unlink()

    con = duckdb.connect(str(db_path))

    # ── 1. Import all Parquet files ───────────────────────────────────────────
    t0 = time.time()
    print("\n[1/3] Importing Parquet files…")
    con.execute(f"""
        CREATE TABLE timeseries AS
        SELECT * FROM read_parquet('{parquet_dir}/*.parquet')
    """)
    n = con.execute("SELECT COUNT(*) FROM timeseries").fetchone()[0]
    print(f"      {n:,} rows imported in {time.time() - t0:.1f}s")

    # ── 2. Autocomplete lookup tables ─────────────────────────────────────────
    t0 = time.time()
    print("\n[2/3] Building autocomplete lookup tables…")
    con.execute("""
        CREATE TABLE targets AS
        SELECT DISTINCT targetId, approvedSymbol
        FROM timeseries
        WHERE approvedSymbol IS NOT NULL
        ORDER BY approvedSymbol
    """)
    con.execute("""
        CREATE TABLE diseases AS
        SELECT DISTINCT diseaseId, name
        FROM timeseries
        WHERE name IS NOT NULL
        ORDER BY name
    """)
    nt = con.execute("SELECT COUNT(*) FROM targets").fetchone()[0]
    nd = con.execute("SELECT COUNT(*) FROM diseases").fetchone()[0]
    print(f"      {nt:,} targets · {nd:,} diseases ({time.time() - t0:.1f}s)")

    # ── 3. Indexes ────────────────────────────────────────────────────────────
    if skip_index:
        print("\n[3/3] Skipping index creation (--skip-index).")
    else:
        print("\n[3/3] Creating indexes…")
        for label, ddl in [
            (
                "timeseries (targetId, diseaseId)",
                "CREATE INDEX idx_pair ON timeseries (targetId, diseaseId)",
            ),
            (
                "targets (approvedSymbol)",
                "CREATE INDEX idx_target ON targets (approvedSymbol)",
            ),
            (
                "diseases (name)",
                "CREATE INDEX idx_disease ON diseases (name)",
            ),
        ]:
            t0 = time.time()
            print(f"      {label}… ", end="", flush=True)
            con.execute(ddl)
            print(f"{time.time() - t0:.1f}s")

    con.close()

    size_mb = db_path.stat().st_size / 1_048_576
    print(f"\nDone — {db_path} ({size_mb:,.0f} MB)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a DuckDB database from Parquet files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "parquet_dir",
        type=Path,
        help="Directory containing *.parquet files",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        metavar="PATH",
        help="Output database path",
    )
    parser.add_argument(
        "--skip-index",
        action="store_true",
        help="Skip index creation (faster build, but slower queries)",
    )
    args = parser.parse_args()

    if not args.parquet_dir.is_dir():
        parser.error(f"Not a directory: {args.parquet_dir}")

    build(args.parquet_dir, args.db, skip_index=args.skip_index)


if __name__ == "__main__":
    main()
