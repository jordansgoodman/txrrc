import re
from pathlib import Path
import pandas as pd
import sqlite3
from io import StringIO

root = Path.home() / "Documents" / "programming" / "txrrc" / "backend"
downloads_dir = root / "data" / "download"
db_dir        = root / "data" / "database"
db_path       = db_dir / "rrc_permits.db"

ENCODING = "latin1"
db_dir.mkdir(parents=True, exist_ok=True)

# Colspecs
colspecs_01 = [
    (0, 2),(2, 9),(9, 11),(11, 14),(14, 46),(46, 48),
    (48, 54),(58, 66),(66, 98),(100, 101),(101, 112),
    (112, 119),(119, 127),(127, 135)
]
names_01 = [
    "record_type","status_number","status_sequence_number","county_code",
    "lease_name","district","operator_number","date_app_received",
    "operator_name","status_of_app_flag","problem_flags","permit_number",
    "issue_date","withdrawn_date"
]


colspecs_02 = [
    (0, 2),     # record_type (02)
    (2, 14),    # permit_number
    (14, 46),   # well_name (32 chars)
    (46, 50),   # well_number (3 chars)
    (50, 52),   # sidetrack_flag (2 chars)
    (54, 56),   # county_code (2 chars)
    (56, 63),   # api_number (8 chars)
    (63, 67),   # field_number (8 chars) 
    (345, 352),  # field_name (32 chars)
    (98, 105), # total_depth (8 chars)
    (105, 110), # vertical_depth (8 chars)
    (114, 121), # horizontal_length (9 chars) 
    (121, 129), # spud_date (CCYYMMDD)
    (129, 137), # completion_date (CCYYMMDD) 
    (137, 145), # plug_back_date (CCYYMMDD)
    (145, 153), # test_date (CCYYMMDD)
    (153, 161), # shut_in_date (CCYYMMDD) 
    (161, 195), # surface_location (36 chars) 
    (400, 433), # bottom_hole_location (36 chars) 
    (323, 330), # acreage (8 chars)
    (330, 339), # spacing (8 chars)
    (433, 500), # remarks (40 chars)
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
    "total_depth",
    "vertical_depth",
    "horizontal_length",
    "spud_date",
    "completion_date",
    "plug_back_date",
    "test_date",
    "shut_in_date",
    "surface_location",
    "bottom_hole_location",
    "acreage",
    "spacing",
    "remarks",
]


# -------------------
# DB Setup
# -------------------
with sqlite3.connect(db_path) as conn:
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS records01;")
    cur.execute("DROP TABLE IF EXISTS records02;")
    cur.execute("DROP TABLE IF EXISTS records14;")
    cur.execute("DROP TABLE IF EXISTS records15;")
    conn.commit()

# -------------------
# Stream parse and write
# -------------------
current_permit = None

def parse_fixed(line, colspecs, names):
    return pd.read_fwf(StringIO(line), colspecs=colspecs, names=names, dtype=str)

with sqlite3.connect(db_path) as conn:
    for file in downloads_dir.glob("*.dat*"):
        print(f"Processing {file.name}...")
        with open(file, "r", encoding=ENCODING) as f:
            for line in f:
                rec_type = line[:2]

                if rec_type == "01":
                    df = parse_fixed(line, colspecs_01, names_01)
                    current_permit = df["permit_number"].iloc[0]
                    df.to_sql("records01", conn, if_exists="append", index=False)

                elif rec_type == "02":
                    df = parse_fixed(line, colspecs_02, names_02)

                    # Cleanup
                    df["well_number"] = df["well_number"].str.replace(" ", "", regex=False).str.strip()
                    df["sidetrack_flag"] = (
                        df["sidetrack_flag"].str.strip().replace("", "00").apply(
                            lambda x: x.zfill(2) if x.isdigit() else x
                        )
                    )
                    df["total_depth"] = df["total_depth"].str.lstrip("0")
                    df["bottom_hole_location"] = df["bottom_hole_location"].str.strip()

                    # Add foreign key
                    df["permit_number"] = current_permit

                    df.to_sql("records02", conn, if_exists="append", index=False)


                elif rec_type == "14":
                    parts = line.strip().split()
                    if len(parts) >= 3:
                        df = pd.DataFrame([{
                            "permit_number": current_permit,
                            "longitude": float(parts[1]),
                            "latitude": float(parts[2])
                        }])
                        df.to_sql("records14", conn, if_exists="append", index=False)

                elif rec_type == "15":
                    parts = line.strip().split()
                    if len(parts) >= 3:
                        df = pd.DataFrame([{
                            "permit_number": current_permit,
                            "longitude": float(parts[1]),
                            "latitude": float(parts[2])
                        }])
                        df.to_sql("records15", conn, if_exists="append", index=False)

print(f"Records 01, 02, 14, 15 streamed into {db_path}")
