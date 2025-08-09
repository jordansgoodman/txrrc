import os
import re
from pathlib import Path
from datetime import datetime

data_directory = "/Users/jordangoodman/programming/txappraisal/backend/data"
out_path = Path(data_directory) / "all_dat_combined.txt"

# Match files like:
#   foo.dat
#   foo.dat.03-31-2021
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
        if last_byte not in (None, b"\n"):
            dst.write(b"\n")

print(f"Wrote {len(candidates)} files into {out_path}")
