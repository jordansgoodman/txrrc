import sqlite3
import pandas as pd 
from pathlib import Path 
root = Path.home() / "Documents" / "programming" / "txrrc" / "backend"
db_dir        = root / "data" / "database"   
db_path  = db_dir / "rrc_permits.db"

conn = sqlite3.connect(db_path)

df = pd.read_sql("select * from permits;",conn)

def glimpse(df, max_width=50):
    print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}\n")
    for col in df.columns:
        col_data = df[col].astype(str).head().tolist()
        values = ", ".join(col_data)
        if len(values) > max_width:
            values = values[:max_width] + "..."
        print(f"{col} ({df[col].dtype}): {values}")

glimpse(df)


