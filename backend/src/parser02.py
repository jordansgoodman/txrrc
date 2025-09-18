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

# record02 layout from RRC spec

colspecs_02 = [
    (0, 2),     # record_type (02)
    (2, 14),    # permit_number
    (14, 46),   # well_name (32 chars)
    (46, 50),   # well_number (3 chars)
    (50, 52),   # sidetrack_flag (2 chars)
    (54, 56),   # county_code (2 chars)
    (56, 63),   # api_number (8 chars)
    (63, 67),   # field_number (8 chars) # validated


    (70, 102),  # field_name (32 chars)
    (102, 134), # operator_name (32 chars)
    (134, 142), # spud_date (CCYYMMDD)
    (142, 150), # completion_date (CCYYMMDD)
    (150, 158), # plug_back_date (CCYYMMDD)
    (158, 166), # test_date (CCYYMMDD)
    (166, 174), # shut_in_date (CCYYMMDD)
    (174, 210), # surface_location (36 chars)
    (210, 246), # bottom_hole_location (36 chars)
    (246, 254), # total_depth (8 chars)
    (254, 262), # vertical_depth (8 chars)
    (262, 270), # horizontal_length (8 chars)
    (270, 278), # acreage (8 chars)
    (278, 286), # spacing (8 chars)
    (286, 326), # remarks (40 chars)
]

names_02 = [
    "record_type",
    "permit_number",
    "well_name",
    "well_number",
    "sidetrack_flag",
    "county_code",
    "api_number",
    "field_number",
    "field_name",
    "operator_name",
    "spud_date",
    "completion_date",
    "plug_back_date",
    "test_date",
    "shut_in_date",
    "surface_location",
    "bottom_hole_location",
    "total_depth",
    "vertical_depth",
    "horizontal_length",
    "acreage",
    "spacing",
    "remarks",
]

with sqlite3.connect(db_path) as conn:
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS records02;")
    conn.commit()

for file in downloads_dir.glob("*.dat*"):
    print(f"Processing {file.name}...")

    records_02 = []

    with open(file, "r", encoding=ENCODING) as f:
        for line in f:
            if line.startswith("02"): 
                records_02.append(line.rstrip("\n"))

    if records_02:
        df_02 = pd.read_fwf(
            StringIO("\n".join(records_02)),
            colspecs=colspecs_02,
            names=names_02,
            dtype="str"
        )

        df_02["well_number"] = df_02["well_number"].str.replace(" ", "", regex=False).str.strip()

        df_02["sidetrack_flag"] = (
            df_02["sidetrack_flag"]
            .str.strip()
            .replace("", "00")           # blank → "00" (no sidetrack)
            .apply(lambda x: x.zfill(2) if x.isdigit() else x)
) 
        with sqlite3.connect(db_path) as conn:
            df_02.to_sql("records02", conn, if_exists="append", index=False)

print(f"All 02 records appended into {db_path}")


