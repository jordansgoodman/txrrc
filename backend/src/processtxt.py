import pandas as pd
import sqlite3 

file_path = "/Users/jordangoodman/programming/txappraisal/backend/data/daf802.txt"
db_path = "/Users/jordangoodman/programming/txappraisal/backend/data/rrc_permits.db"

colspecs_01 = [
    (0, 2), (2, 14), (14, 46), (46, 56), (56, 64),
    (64, 70), (70, 110), (110, 116), (116, 117), (117, 119), (119, 127)
]
names_01 = [
    "record_type", "permit_number", "operator_name", "api_number", "issue_date",
    "field_number", "field_name", "well_number", "status_code", "purpose_code", "filing_date"
]
df01 = pd.read_fwf(file_path, colspecs=colspecs_01, names=names_01, dtype=str, encoding="latin1")

df01 = df01[df01["record_type"] == "01"]

colspecs_02 = [
    (0, 2), (2, 14), (14, 54), (54, 60), (60, 63),
    (63, 72), (72, 82), (82, 92), (92, 132), (132, 140), (140, 180)
]   
names_02 = [
    "record_type", "permit_number", "lease_name", "well_number", "county_code",
    "abstract_number", "block", "section", "survey_name", "depth_info", "surface_location"
]
df02 = pd.read_fwf(file_path, colspecs=colspecs_02, names=names_02, dtype=str, encoding="latin1")

df02 = df02[df02["record_type"] == "02"]

df_final = df01.merge(df02, on="permit_number", how="left", suffixes=("_01", "_02"))


conn = sqlite3.connect(db_path)
df_final.to_sql("permits", conn, if_exists="replace", index=False)
conn.commit()
conn.close()
print(f"SQLite database created at: {db_path}")