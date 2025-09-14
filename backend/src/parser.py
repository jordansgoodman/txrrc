from pathlib import Path
import pandas as pd

file_path = Path("/home/admin/Documents/programming/txrrc/backend/data/download/daf420.dat.01-31-2018")

records = { "01": [], "02": [], "03": [], "04": [], "05": [] }

with open(file_path, "r", encoding="latin1") as f:
    for line in f:
        record_type = line[:2]
        if record_type in records:
            records[record_type].append(line.rstrip("\n"))

for rt, lines in records.items():
    print(f"Record Type {rt}: {len(lines)} lines")


colspecs_01 = [
    (0, 2), (2, 14), (14, 46), (46, 54), (54, 62),
    (62, 102), (102, 108), (108, 148), (148, 154)
]

names_01 = [
    "record_type", "permit_number", "well_name", "api_number",
    "issue_date", "operator_name", "field_number", "field_name", "well_number"
]

df_01 = pd.read_fwf(pd.io.common.StringIO("\n".join(records["01"])),
                    colspecs=colspecs_01, names=names_01)


def glimpse(df, max_width=100):
    print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}\n")
    for col in df.columns:
        col_data = df[col].astype(str).head().tolist()
        values = ", ".join(col_data)
        if len(values) > max_width:
            values = values[:max_width] + "..."
        print(f"{col} ({df[col].dtype}): {values}")

glimpse(df_01)
