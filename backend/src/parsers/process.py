import sqlite3
import pandas as pd
from io import StringIO
from src.config import DB_PATH, DOWNLOADS_DIR, ENCODING

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
    (0, 2),(2, 14),(14, 46),(46, 50),(50, 52),(54, 56),(56, 63),(63, 67),
    (345, 352),(98, 105),(105, 110),(114, 121),(121, 129),(129, 137),
    (137, 145),(145, 153),(153, 161),(161, 195),(400, 433),(323, 330),
    (330, 339),(433, 500)
]
names_02 = [
    "record_type","permit_number","well_name","well_number","sidetrack_flag",
    "county_code","api_number","field_number","field_name","total_depth",
    "vertical_depth","horizontal_length","spud_date","completion_date",
    "plug_back_date","test_date","shut_in_date","surface_location",
    "bottom_hole_location","acreage","spacing","remarks"
]

def parse_fixed(line, colspecs, names):
    return pd.read_fwf(StringIO(line), colspecs=colspecs, names=names, dtype=str)

def build_database():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        for tbl in ("records01_tmp", "records02_tmp", "records14_tmp", "records15_tmp", "permit_data_tmp"):
            cur.execute(f"DROP TABLE IF EXISTS {tbl};")
        conn.commit()

        cur.execute("""
            CREATE TABLE records01_tmp (
                record_type TEXT, status_number TEXT, status_sequence_number TEXT,
                county_code TEXT, lease_name TEXT, district TEXT, operator_number TEXT,
                date_app_received TEXT, operator_name TEXT, status_of_app_flag TEXT,
                problem_flags TEXT, permit_number TEXT, issue_date TEXT, withdrawn_date TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE records02_tmp (
                record_type TEXT, permit_number TEXT, well_name TEXT, well_number TEXT,
                sidetrack_flag TEXT, county_code TEXT, api_number TEXT, field_number TEXT,
                field_name TEXT, total_depth TEXT, vertical_depth TEXT,
                horizontal_length TEXT, spud_date TEXT, completion_date TEXT,
                plug_back_date TEXT, test_date TEXT, shut_in_date TEXT,
                surface_location TEXT, bottom_hole_location TEXT,
                acreage TEXT, spacing TEXT, remarks TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE records14_tmp (
                permit_number TEXT, longitude REAL, latitude REAL
            );
        """)
        cur.execute("""
            CREATE TABLE records15_tmp (
                permit_number TEXT, longitude REAL, latitude REAL
            );
        """)
        conn.commit()

        current_permit = None

        for file in DOWNLOADS_DIR.glob("*.dat*"):
            print(f"Processing {file.name}...")
            with open(file, "r", encoding=ENCODING) as f:
                for line in f:
                    rec_type = line[:2]

                    if rec_type == "01":
                        df = parse_fixed(line, colspecs_01, names_01)
                        current_permit = df["permit_number"].iloc[0]
                        df.to_sql("records01_tmp", conn, if_exists="append", index=False)

                    elif rec_type == "02":
                        df = parse_fixed(line, colspecs_02, names_02)
                        df["well_number"] = df["well_number"].str.replace(" ", "", regex=False).str.strip()
                        df["sidetrack_flag"] = (
                            df["sidetrack_flag"].str.strip().replace("", "00").apply(lambda x: x.zfill(2) if x.isdigit() else x)
                        )
                        df["total_depth"] = df["total_depth"].str.lstrip("0")
                        df["bottom_hole_location"] = df["bottom_hole_location"].str.strip()
                        df["permit_number"] = current_permit
                        df.to_sql("records02_tmp", conn, if_exists="append", index=False)

                    elif rec_type == "14":
                        parts = line.strip().split()
                        if len(parts) >= 3:
                            df = pd.DataFrame(
                                [{"permit_number": current_permit,
                                  "longitude": float(parts[1]),
                                  "latitude": float(parts[2])}]
                            )
                            df.to_sql("records14_tmp", conn, if_exists="append", index=False)

                    elif rec_type == "15":
                        parts = line.strip().split()
                        if len(parts) >= 3:
                            df = pd.DataFrame(
                                [{"permit_number": current_permit,
                                  "longitude": float(parts[1]),
                                  "latitude": float(parts[2])}]
                            )
                            df.to_sql("records15_tmp", conn, if_exists="append", index=False)

        cur.execute("""
            CREATE TABLE permit_data_tmp AS 
            WITH details AS (
                SELECT 
                    r2.*,
                    r14.latitude,
                    r14.longitude 
                FROM records02_tmp r2 
                LEFT JOIN records14_tmp r14 
                ON r2.permit_number = r14.permit_number
            )
            SELECT * FROM details;
        """)
        conn.commit()

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS permit_data AS SELECT * FROM permit_data_tmp WHERE 0;")
        cur.execute("DELETE FROM permit_data;")
        cur.execute("INSERT INTO permit_data SELECT * FROM permit_data_tmp;")
        conn.commit()
        print("permit_data table refreshed with new rows")

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        for tbl in ("records01_tmp", "records02_tmp", "records14_tmp", "records15_tmp", "permit_data_tmp"):
            cur.execute(f"DROP TABLE IF EXISTS {tbl};")
        conn.commit()
        print("Temporary tables dropped, refresh complete")

if __name__ == "__main__":
    build_database()
