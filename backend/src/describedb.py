#!/usr/bin/env python3
import sqlite3
import sys
import re
from pathlib import Path
from typing import List, Tuple, Dict, Any

# -------- Config --------
DEFAULT_DB = "/Users/jordangoodman/programming/txappraisal/backend/data/database/rrc_permits.db"
SAMPLE_VALUES = 3
MAX_TABLES = None   # set to an int to limit

# -------- Helpers --------
def is_numeric_type(decl: str) -> bool:
    decl = (decl or "").upper()
    return any(k in decl for k in ("INT", "REAL", "FLOA", "DOUB", "NUM", "DEC"))

def is_date_like(name: str, decl: str) -> bool:
    decl = (decl or "").upper()
    name_u = (name or "").upper()
    # heuristic: name contains date/time keywords OR text/char type that might hold dates
    return ("DATE" in name_u or "TIME" in name_u) or ("DATE" in decl or "TIME" in decl)

def fmt(v: Any, width: int = 30) -> str:
    s = "NULL" if v is None else str(v)
    s = s.replace("\n", "\\n")
    return s if len(s) <= width else s[:width-1] + "…"

def print_rule():
    print("-" * 110)

def fetch_one(conn: sqlite3.Connection, sql: str) -> Any:
    cur = conn.execute(sql)
    row = cur.fetchone()
    return row[0] if row else None

# -------- Core --------
def list_tables(conn: sqlite3.Connection) -> List[str]:
    q = """
    SELECT name FROM sqlite_master
    WHERE type='table' AND name NOT LIKE 'sqlite_%'
    ORDER BY name;
    """
    return [r[0] for r in conn.execute(q).fetchall()]

def table_row_count(conn: sqlite3.Connection, table: str) -> int:
    return fetch_one(conn, f'SELECT COUNT(*) FROM "{table}"')

def table_columns(conn: sqlite3.Connection, table: str) -> List[Dict[str, Any]]:
    cols = []
    for cid, name, decltype, notnull, dflt, pk in conn.execute(f'PRAGMA table_info("{table}")'):
        cols.append({
            "cid": cid,
            "name": name,
            "decltype": decltype,
            "notnull": bool(notnull),
            "default": dflt,
            "pk": bool(pk),
        })
    return cols

def column_stats(conn: sqlite3.Connection, table: str, col: Dict[str, Any]) -> Dict[str, Any]:
    name = col["name"]
    decl = col["decltype"] or ""
    # non-null, distinct
    q_counts = f'''
      SELECT
        SUM(CASE WHEN "{name}" IS NOT NULL THEN 1 ELSE 0 END) AS n_nonnull,
        COUNT(DISTINCT "{name}") AS n_unique
      FROM "{table}";
    '''
    n_nonnull, n_unique = conn.execute(q_counts).fetchone()

    # numeric min/max
    num_min = num_max = None
    if is_numeric_type(decl):
        q_num = f'SELECT MIN(CAST("{name}" AS REAL)), MAX(CAST("{name}" AS REAL)) FROM "{table}" WHERE "{name}" IS NOT NULL;'
        num_min, num_max = conn.execute(q_num).fetchone()

    # text length range
    len_min = len_max = None
    if not is_numeric_type(decl):
        q_len = f'SELECT MIN(LENGTH("{name}")), MAX(LENGTH("{name}")) FROM "{table}" WHERE "{name}" IS NOT NULL;'
        len_min, len_max = conn.execute(q_len).fetchone()

    # date-ish min/max (heuristic)
    date_min = date_max = None
    if is_date_like(name, decl):
        # try common ISO-ish ordering; if not ISO, this is just a hint
        q_date = f'SELECT MIN("{name}"), MAX("{name}") FROM "{table}" WHERE "{name}" IS NOT NULL;'
        date_min, date_max = conn.execute(q_date).fetchone()

    # sample values
    q_sample = f'SELECT "{name}" FROM "{table}" WHERE "{name}" IS NOT NULL ORDER BY RANDOM() LIMIT {SAMPLE_VALUES};'
    samples = [r[0] for r in conn.execute(q_sample).fetchall()]

    return {
        "name": name,
        "decltype": decl,
        "notnull": col["notnull"],
        "pk": col["pk"],
        "n_nonnull": n_nonnull or 0,
        "n_unique": n_unique or 0,
        "num_min": num_min,
        "num_max": num_max,
        "len_min": len_min,
        "len_max": len_max,
        "date_min": date_min,
        "date_max": date_max,
        "samples": samples,
    }

def glimpse_table(conn: sqlite3.Connection, table: str):
    row_count = table_row_count(conn, table)
    cols = table_columns(conn, table)

    print_rule()
    print(f'Table: {table}   rows={row_count}   cols={len(cols)}')
    print_rule()
    header = [
        "col",
        "type",
        "pk",
        "notnull",
        "nonnull",
        "unique",
        "num[min..max]",
        "len[min..max]",
        "date[min..max]",
        "samples",
    ]
    print("{:<32} {:<14} {:<2} {:<7} {:>8} {:>8} {:<21} {:<15} {:<25} {}".format(*header))
    print("-" * 110)

    for col in cols:
        st = column_stats(conn, table, col)

        num_rng = ""
        if st["num_min"] is not None or st["num_max"] is not None:
            num_rng = f'{st["num_min"]}..{st["num_max"]}'

        len_rng = ""
        if st["len_min"] is not None or st["len_max"] is not None:
            len_rng = f'{st["len_min"]}..{st["len_max"]}'

        date_rng = ""
        if st["date_min"] is not None or st["date_max"] is not None:
            date_rng = f'{fmt(st["date_min"], 12)}..{fmt(st["date_max"], 12)}'

        samples = ", ".join(fmt(v, 20) for v in st["samples"])

        print("{:<32} {:<14} {:<2} {:<7} {:>8} {:>8} {:<21} {:<15} {:<25} {}".format(
            st["name"][:32],
            (st["decltype"] or "").upper()[:14],
            "✓" if st["pk"] else "",
            "✓" if st["notnull"] else "",
            st["n_nonnull"],
            st["n_unique"],
            num_rng[:21],
            len_rng[:15],
            date_rng[:25],
            samples
        ))
    print()

def glimpse_db(db_path: str):
    p = Path(db_path)
    if not p.exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA case_sensitive_like=OFF;")
        tables = list_tables(conn)
        if MAX_TABLES:
            tables = tables[:MAX_TABLES]

        if not tables:
            print("No user tables found.")
            return

        for t in tables:
            glimpse_table(conn, t)
    finally:
        conn.close()

# -------- Entrypoint --------
if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB
    glimpse_db(db)
