import sqlite3
import pandas as pd 

db_path = "/Users/jordangoodman/programming/txappraisal/backend/data/rrc_permits.db"

conn = sqlite3.connect(db_path)

df = pd.read_sql("select * from test_gisp_sc1",conn)

print(df)
