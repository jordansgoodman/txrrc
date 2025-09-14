from pathlib import Path
import pandas as pd
from io import StringIO

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

df_01 = pd.read_fwf(StringIO("\n".join(records["01"])),
                    colspecs=colspecs_01, names=names_01)

colspecs_02 = [
    (0, 2), (2, 14), (14, 54), (54, 60), (60, 63),
    (63, 72), (72, 82), (82, 92), (92, 132), (132, 140),
    (140, 160), (160, 170), (170, 180)
]

names_02 = [
    "record_type",
    "permit_number",
    "lease_name",
    "well_number",
    "county_code",
    "abstract_number",
    "block",
    "section",
    "survey_name",
    "depth_info",
    "surface_location",
    "latitude",
    "longitude"
]


colspecs_03 = [(0,2),(2,14),(14,50)]
names_03 = ["record_type","permit_number","status_flag"]

colspecs_04 = [(0,2),(2,14),(14,46),(46,72)]
names_04 = ["record_type","permit_number","well_name","misc_data"]


colspecs_05 = [(0,2),(2,14),(14,40),(40,92),(92,262)]
names_05 = ["record_type","permit_number","township","survey_name","other_survey_data"]


def parse_records(lines, colspecs, names):
    if not lines:
        return pd.DataFrame(columns=names)
    return pd.read_fwf(StringIO("\n".join(lines)),
                       colspecs=colspecs, names=names, dtype=str)

df_01 = parse_records(records["01"], colspecs_01, names_01)
df_02 = parse_records(records["02"], colspecs_02, names_02)
df_03 = parse_records(records["03"], colspecs_03, names_03)
df_04 = parse_records(records["04"], colspecs_04, names_04)
df_05 = parse_records(records["05"], colspecs_05, names_05)

# print("01 Header:", df_01.head())
#print("02 Location:", df_02.head())
#print("03 Status:", df_03.head())
#print("04 Wellbore:", df_04.head())
# print("05 Survey:", df_05.head())
