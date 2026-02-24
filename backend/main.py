from __future__ import annotations

import math
import os
import threading
from functools import lru_cache
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ── Paths ──────────────────────────────────────────────────────────────────────
BACKEND_DIR = Path(__file__).parent
PROJECT_DIR = BACKEND_DIR.parent
DB_PATH = Path(os.getenv("TIMESERIES_DB", str(BACKEND_DIR / "timeseries.db")))

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Timeseries Prototype", docs_url="/api/docs", redoc_url=None)

# ── Thread-local DuckDB connections ───────────────────────────────────────────
# FastAPI runs sync endpoints in a thread pool; each thread gets its own
# read-only connection (DuckDB supports concurrent readers on the same file).
_local = threading.local()


def get_db() -> duckdb.DuckDBPyConnection:
    if not hasattr(_local, "con"):
        if not DB_PATH.exists():
            raise RuntimeError(
                f"Database not found at {DB_PATH}. "
                "Run `uv run timeseries-build-db <parquet_dir>` first."
            )
        _local.con = duckdb.connect(str(DB_PATH), read_only=True)
    return _local.con


# ── Helpers ────────────────────────────────────────────────────────────────────
def _clean(v: Any) -> Any:
    """Convert float NaN → None so FastAPI serialises it as JSON null."""
    return None if isinstance(v, float) and math.isnan(v) else v


def cursor_to_records(cur) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [{col: _clean(val) for col, val in zip(cols, row)} for row in cur.fetchall()]


# ── Endpoints ──────────────────────────────────────────────────────────────────
@app.get("/api/search/targets")
def search_targets(q: str = Query(..., min_length=1)) -> list[dict]:
    cur = get_db().execute(
        """
        SELECT id AS targetId, name AS approvedSymbol, description
        FROM search_targets
        WHERE len(list_filter(prefixes, x -> x ILIKE ?)) > 0
        ORDER BY multiplier DESC
        LIMIT 20
        """,
        [f"{q}%"],
    )
    return cursor_to_records(cur)


@app.get("/api/search/diseases")
def search_diseases(q: str = Query(..., min_length=1)) -> list[dict]:
    cur = get_db().execute(
        """
        SELECT id AS diseaseId, name, description
        FROM search_diseases
        WHERE len(list_filter(prefixes, x -> x ILIKE ?)) > 0
        ORDER BY multiplier DESC
        LIMIT 20
        """,
        [f"%{q}%"],
    )
    return cursor_to_records(cur)


@lru_cache(maxsize=512)
def _cached_timeseries(target_id: str, disease_id: str) -> list[dict]:
    """Queries DB once per unique (targetId, diseaseId) pair; result is cached."""
    cur = get_db().execute(
        "SELECT * FROM timeseries "
        "WHERE targetId = ? AND diseaseId = ? "
        "ORDER BY year",
        [target_id, disease_id],
    )
    return cursor_to_records(cur)


@app.get("/api/timeseries")
def get_timeseries(
    targetId: str = Query(...),
    diseaseId: str = Query(...),
):
    data = _cached_timeseries(targetId, diseaseId)
    if not data:
        raise HTTPException(
            status_code=404,
            detail="No data found for this target–disease pair.",
        )
    return JSONResponse(content=data)


# ── Frontend ───────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return FileResponse(PROJECT_DIR / "index.html")


# Mount after all API routes so /api/* is never shadowed.
app.mount("/", StaticFiles(directory=str(PROJECT_DIR)), name="static")


# ── Entry point ────────────────────────────────────────────────────────────────
def main() -> None:
    import uvicorn

    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    main()
