import os
import re
import sqlite3
import zipfile
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Dict
import pandas as pd
import shutil

# ----------------------------
# Paths / Config
# ----------------------------

# Base project root - dynamically resolves to your home directory

root = Path.home() / "Documents" / "programming" / "txrrc" / "backend"
downloads_dir = root / "data" / "download"       # input: zips and .dat live here
txt_dir       = root / "data" / "txt"            # output: combined txt
db_dir        = root / "data" / "database"       # output: sqlite db
log_dir       = root / "data" / "log"            # logs folder
log_path = log_dir / "process.log"
out_path = txt_dir / "all_dat_combined.txt"
db_path  = db_dir / "rrc_permits.db"

ENCODING = "latin1"

# ----------------------------
# Ensure folders exist
# ----------------------------
downloads_dir.mkdir(parents=True, exist_ok=True)
txt_dir.mkdir(parents=True, exist_ok=True)
db_dir.mkdir(parents=True, exist_ok=True)
log_dir.mkdir(parents=True, exist_ok=True)

# Ensure folders exist
downloads_dir.mkdir(parents=True, exist_ok=True)
txt_dir.mkdir(parents=True, exist_ok=True)
db_dir.mkdir(parents=True, exist_ok=True)
log_dir.mkdir(parents=True, exist_ok=True)

# clear folder
def clear_folder(folder_path):
    if not os.path.exists(folder_path):
        print(f"Folder '{folder_path}' does not exist.")
        return
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.remove(file_path)  
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  
        except Exception as e:
            print(f"Failed to delete {file_path}: {e}")

clear_folder(txt_dir)

# ----------------------------
# Logging helper
# ----------------------------
def log(msg: str):
    print(msg)
    with open(log_path, "a") as logf:
        logf.write(f"{datetime.now().isoformat()} - {msg}\n")

# Reset log each run
log_path.write_text("")
log("=== Run started ===")
log(f"Downloads dir: {downloads_dir}")
log(f"TXT out:       {out_path}")
log(f"DB out:        {db_path}")
log(f"Log file:      {log_path}")

# ----------------------------
# Step 1: Unpack any ZIPs in downloads folder
# ----------------------------
for zip_file in downloads_dir.glob("*.zip"):
    try:
        log(f"Unpacking ZIP: {zip_file.name}")
        with zipfile.ZipFile(zip_file, "r") as zf:
            zf.extractall(downloads_dir)
            log(f"Extracted {len(zf.namelist())} items from {zip_file.name}")
    except Exception as e:
        log(f"Failed to extract {zip_file.name}: {e}")

# ----------------------------
# Step 2: Concatenate .dat files from downloads into data/txt/all_dat_combined.txt
# ----------------------------
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

candidates = [p for p in downloads_dir.iterdir() if p.is_file() and pat.search(p.name)]
if not candidates:
    raise FileNotFoundError(f"No .dat or .dat.MM-DD-YYYY files found in {downloads_dir}")

candidates.sort(key=lambda p: (parse_date_from_name(p.name) or datetime.min, p.name.lower()))

BUF = 1024 * 1024  # 1 MB buffer

with open(out_path, "wb") as dst:
    for p in candidates:
        log(f"Appending {p.name}")
        last_byte = None
        with open(p, "rb") as src:
            while True:
                chunk = src.read(BUF)
                if not chunk:
                    break
                dst.write(chunk)
                last_byte = chunk[-1:]
        if last_byte not in (None, b"\n"):
            dst.write(b"\n")

log(f"Wrote {len(candidates)} files into {out_path}")

# ----------------------------
# Step 3: Parse combined file into SQLite at data/database/rrc_permits.db
# ----------------------------
COLSPECS: Dict[str, List[Tuple[int, int]]] = {
    "01": [
        (0, 2), (2, 14), (14, 46), (46, 56), (56, 64),
        (64, 70), (70, 110), (110, 116), (116, 117), (117, 119), (119, 127)
    ],
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

buffers: Dict[str, List[List[str]]] = {rt: [] for rt in COLSPECS.keys()}
unknown: List[tuple[str, str]] = []

with open(out_path, "r", encoding=ENCODING, errors="ignore") as f:
    for raw in f:
        if not raw.strip():
            continue
        line = raw.rstrip("\n")
        rt = line[:2]
        if rt in COLSPECS:
            buffers[rt].append(slice_by_colspec(line, COLSPECS[rt]))
        else:
            unknown.append((rt, line))

conn = sqlite3.connect(db_path)
try:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")

    for rt, rows in buffers.items():
        tbl = f"records_{rt}"
        if rows:
            df = normalize_df(pd.DataFrame(rows, columns=NAMES[rt]))
            df.to_sql(tbl, conn, if_exists="replace", index=False)
            log(f"Loaded {len(df):>8} rows into {tbl}")
        else:
            df = pd.DataFrame(columns=NAMES[rt])
            df.to_sql(tbl, conn, if_exists="replace", index=False)
            log(f"Loaded {len(df):>8} rows into {tbl}")

    dfu = pd.DataFrame(unknown, columns=["record_type", "raw_line"]) if unknown else pd.DataFrame(columns=["record_type", "raw_line"])
    dfu.to_sql("records_unknown", conn, if_exists="replace", index=False)
    log(f"Loaded {len(dfu):>8} rows into records_unknown")

    df01 = pd.DataFrame(buffers["01"], columns=NAMES["01"]) if buffers["01"] else pd.DataFrame(columns=NAMES["01"])
    df02 = pd.DataFrame(buffers["02"], columns=NAMES["02"]) if buffers["02"] else pd.DataFrame(columns=NAMES["02"])
    if not df01.empty:
        df_final = normalize_df(df01).merge(normalize_df(df02), on="permit_number", how="left", suffixes=("_01", "_02"))
        df_final.to_sql("permits", conn, if_exists="replace", index=False)
        log(f"Loaded {len(df_final):>8} rows into permits")

    conn.commit()
finally:
    conn.close()

log(f"SQLite database created at: {db_path}")
log("Process completed successfully.")
