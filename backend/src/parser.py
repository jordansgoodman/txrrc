from pathlib import Path
import pandas as pd
from io import StringIO

file_path = Path("/home/admin/Documents/programming/txrrc/backend/data/download/daf420.dat.01-31-2018")

records = { "01": [], "02": [], "03": [], "04": [], "05": [] }


# helper functions

def glimpse(df, max_width=100):
    print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}\n")
    for col in df.columns:
        col_data = df[col].astype(str).head().tolist()
        values = ", ".join(col_data)
        if len(values) > max_width:
            values = values[:max_width] + "..."
        print(f"{col} ({df[col].dtype}): {values}")

# creating records

with open(file_path, "r", encoding="latin1") as f:
    for line in f:
        record_type = line[:2]
        if record_type in records:
            records[record_type].append(line.rstrip("\n"))

for rt, lines in records.items():
    print(f"Record Type {rt}: {len(lines)} lines")

# processing 01 records

colspecs_01 = [
    (0, 2),     # record_type (01) # validated
    (2, 9),     # status_number (drilling permit status number) # validated
    (9, 11),    # status_sequence_number # validated
    (11, 14),   # county_code # validated
    (14, 46),   # lease_name (32 chars) # validated
    (46, 48),   # district # validated
    (48, 54),   # operator_number (6 chars)# validated
    (58, 66),   # date_app_received (CCYYMMDD) # validated
    (66, 98),    # operator_name # validated # validated
    (100, 101),  # status_of_app_flag (1 character only!) # validated
    (113, 119),   # problem_flags (11 chars) # need to fix
    (106, 113),  # permit_number (7 chars)
    (113, 121),  # issue_date (8 chars)
    (121, 129),  # withdrawn_date (8 chars)
    (129, 135),  # well_number (6 chars)
    (135, 212),  # reserved / filler
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
    "well_number",
    "reserved"
]

df_01 = pd.read_fwf(StringIO("\n".join(records["01"])),
                    colspecs=colspecs_01, names=names_01,dtype='str')

glimpse(df_01)