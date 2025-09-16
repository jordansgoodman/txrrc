from pathlib import Path
import pandas as pd
import sqlite3
from io import StringIO

root = Path.home() / "Documents" / "programming" / "txrrc" / "backend"
downloads_dir = root / "data" / "download"       # input: zips and .dat live here
txt_dir       = root / "data" / "txt"            # output: combined txt
db_dir        = root / "data" / "database"       # output: sqlite db
log_dir       = root / "data" / "log"            # logs folder
log_path = log_dir / "process.log"
out_path = txt_dir / "all_dat_combined.txt"
db_path  = db_dir / "rrc_permits.db"

ENCODING = "latin1"

db_dir.mkdir(parents=True, exist_ok=True)

colspecs_01 = [
    (0, 2),     # record_type
    (2, 9),     # status_number
    (9, 11),    # status_sequence_number
    (11, 14),   # county_code
    (14, 46),   # lease_name
    (46, 48),   # district
    (48, 54),   # operator_number
    (58, 66),   # date_app_received
    (66, 98),   # operator_name
    (100, 101), # status_of_app_flag
    (101, 112), # problem_flags
    (112, 119), # permit_number
    (119, 127), # issue_date
    (127, 135), # withdrawn_date
]

names_01 = [
    "record_type",
    "status_number",
    "status_sequence_number",
    "county_code",
    "lease_name",
    "district",
    "operator_number",
    "date_app_received",
    "operator_name",
    "status_of_app_flag",
    "problem_flags",
    "permit_number",
    "issue_date",
    "withdrawn_date",
]

with sqlite3.connect(db_path) as conn:
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS records01;")
    conn.commit()

for file in downloads_dir.glob("*.dat*"):
    print(f"Processing {file.name}...")

    records_01 = []

    with open(file, "r", encoding=ENCODING) as f:
        for line in f:
            if line.startswith("01"): 
                records_01.append(line.rstrip("\n"))

    if records_01:
        df_01 = pd.read_fwf(
            StringIO("\n".join(records_01)),
            colspecs=colspecs_01,
            names=names_01,
            dtype="str"
        )

        with sqlite3.connect(db_path) as conn:
            df_01.to_sql("records01", conn, if_exists="append", index=False)

print(f"All 01 records appended into {db_path}")
