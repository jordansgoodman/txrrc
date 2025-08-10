
import os
import re
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Dict

import pandas as pd

# ----------------------------
# Paths / Config
# ----------------------------
data_directory = "/Users/jordangoodman/programming/txappraisal/backend/data"
out_path = Path(data_directory) / "all_dat_combined.txt"
db_path = Path(data_directory) / "rrc_permits.db"
ENCODING = "latin1"

# ----------------------------
# Concatenate .dat files
# ----------------------------
# Match:
#   foo.dat
#   foo.dat.MM-DD-YYYY
pat = re.compile(r"\.dat(?:\.(\d{2}-\d{2}-\d{4}))?$", re.IGNORECASE)

def parse_date_from_name(name: str):
    m = pat.search(name)
    if not m:
        return None
    ds = m.group(1)
    if not ds:
        return None
    try:
        return datetime.strptime(ds, "%m-%d-%Y")
    except ValueError:
        return None

# Collect matching files (non-recursive)
candidates = []
for p in Path(data_directory).iterdir():
    if not p.is_file():
        continue
    if pat.search(p.name):
        candidates.append(p)

if not candidates:
    raise FileNotFoundError(f"No .dat or .dat.MM-DD-YYYY files found in {data_directory}")

# Sort by embedded date if present, else by name
candidates.sort(key=lambda p: (parse_date_from_name(p.name) or datetime.min, p.name.lower()))

BUF = 1024 * 1024  # 1 MB buffer

with open(out_path, "wb") as dst:
    for p in candidates:
        print(f"Appending {p.name}")
        last_byte = None
        with open(p, "rb") as src:
            while True:
                chunk = src.read(BUF)
                if not chunk:
                    break
                dst.write(chunk)
                last_byte = chunk[-1:]
        # Ensure line break as file delimiter
        if last_byte not in (None, b"\n"):
            dst.write(b"\n")

print(f"Wrote {len(candidates)} files into {out_path}")

# ----------------------------
# Parse combined fixed-width file into SQLite
# ----------------------------
# Known layouts (extend as needed)
COLSPECS: Dict[str, List[Tuple[int, int]]] = {
    # record_type 01
    "01": [
        (0, 2), (2, 14), (14, 46), (46, 56), (56, 64),
        (64, 70), (70, 110), (110, 116), (116, 117), (117, 119), (119, 127)
    ],
    # record_type 02
    "02": [
        (0, 2), (2, 14), (14, 54), (54, 60), (60, 63),
        (63, 72), (72, 82), (82, 92), (92, 132), (132, 140), (140, 180)
    ],
}

NAMES: Dict[str, List[str]] = {
    "01": [
        "record_type", "permit_number", "operator_name", "api_number", "issue_date",
        "field_number", "field_name", "well_number", "status_code", "purpose_code", "filing_date"
    ],
    "02": [
        "record_type", "permit_number", "lease_name", "well_number", "county_code",
        "abstract_number", "block", "section", "survey_name", "depth_info", "surface_location"
    ],
}

def slice_by_colspec(line: str, colspecs: List[Tuple[int, int]]) -> List[str]:
    return [line[a:b] for a, b in colspecs]

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    return df

# Buckets for known types and unknown lines
buffers: Dict[str, List[List[str]]] = {rt: [] for rt in COLSPECS.keys()}
unknown: List[tuple[str, str]] = []

# Stream the combined file once
with open(out_path, "r", encoding=ENCODING, errors="ignore") as f:
    for raw in f:
        if not raw.strip():
            continue
        line = raw.rstrip("\n")
        rt = line[:2]
        if rt in COLSPECS:
            row = slice_by_colspec(line, COLSPECS[rt])
            buffers[rt].append(row)
        else:
            unknown.append((rt, line))

# Load to SQLite (full refresh per table)
conn = sqlite3.connect(db_path)
try:
    # Speed tweaks for bulk import
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")

    # Known types
    for rt, rows in buffers.items():
        tbl = f"records_{rt}"
        if rows:
            df = pd.DataFrame(rows, columns=NAMES[rt])
            df = normalize_df(df)
            df.to_sql(tbl, conn, if_exists="replace", index=False)
            print(f"Loaded {len(df):>8} rows into {tbl}")
        else:
            # ensure table exists empty for consistency
            df = pd.DataFrame(columns=NAMES[rt])
            df.to_sql(tbl, conn, if_exists="replace", index=False)
            print(f"Loaded {len(df):>8} rows into {tbl}")

    # Unknown catch-all
    if unknown:
        dfu = pd.DataFrame(unknown, columns=["record_type", "raw_line"])
        dfu.to_sql("records_unknown", conn, if_exists="replace", index=False)
        print(f"Loaded {len(dfu):>8} rows into records_unknown")
    else:
        dfu = pd.DataFrame(columns=["record_type", "raw_line"])
        dfu.to_sql("records_unknown", conn, if_exists="replace", index=False)
        print(f"Loaded {len(dfu):>8} rows into records_unknown")

    # Optional merged permits table (01 left-join 02 on permit_number)
    df01 = pd.DataFrame(buffers["01"], columns=NAMES["01"]) if buffers["01"] else pd.DataFrame(columns=NAMES["01"])
    df02 = pd.DataFrame(buffers["02"], columns=NAMES["02"]) if buffers["02"] else pd.DataFrame(columns=NAMES["02"])
    if not df01.empty:
        df01 = normalize_df(df01)
        df02 = normalize_df(df02)
        df_final = df01.merge(df02, on="permit_number", how="left", suffixes=("_01", "_02"))
        df_final.to_sql("permits", conn, if_exists="replace", index=False)
        print(f"Loaded {len(df_final):>8} rows into permits")

    conn.commit()
finally:
    conn.close()

print(f"SQLite database created at: {db_path}")
