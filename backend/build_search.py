#!/usr/bin/env python3
"""
Adds (or replaces) search tables in an existing DuckDB database using the
dedicated search Parquet files.

Run this after build_db.py, or any time the search data changes.

Usage:
    uv run timeseries-build-search [--db PATH] [--target-dir DIR] [--disease-dir DIR]
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import duckdb

BACKEND_DIR     = Path(__file__).parent
PROJECT_DIR     = BACKEND_DIR.parent
DEFAULT_DB      = BACKEND_DIR / "timeseries.db"
DEFAULT_TARGET  = PROJECT_DIR / "search_target"
DEFAULT_DISEASE = PROJECT_DIR / "search_disease"


def build_search(
    db_path: Path,
    target_dir: Path,
    disease_dir: Path,
) -> None:
    print(f"Database      : {db_path}")
    print(f"search_target : {target_dir}")
    print(f"search_disease: {disease_dir}")

    con = duckdb.connect(str(db_path))

    for table, parquet_dir in [
        ("search_targets",  target_dir),
        ("search_diseases", disease_dir),
    ]:
        t0 = time.time()
        print(f"\nImporting {table}…")

        # Read only the columns we need to keep the table lean.
        # `prefixes` is used for matching; `multiplier` for ranking.
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"""
            CREATE TABLE {table} AS
            SELECT id, name, description, prefixes, multiplier
            FROM read_parquet('{parquet_dir}/*.parquet')
        """)

        n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {n:,} rows imported in {time.time() - t0:.1f}s")

    con.close()
    print("\nDone.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Add search tables to the timeseries DuckDB database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db",          type=Path, default=DEFAULT_DB,      metavar="PATH")
    parser.add_argument("--target-dir",  type=Path, default=DEFAULT_TARGET,  metavar="DIR")
    parser.add_argument("--disease-dir", type=Path, default=DEFAULT_DISEASE, metavar="DIR")
    args = parser.parse_args()

    for p in (args.db, args.target_dir, args.disease_dir):
        if not p.exists():
            parser.error(f"Path not found: {p}")

    build_search(args.db, args.target_dir, args.disease_dir)


if __name__ == "__main__":
    main()
