from src.api.dependencies.database import get_db

db_gen = get_db()
conn = next(db_gen)

print("Tables:", conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall())

db_gen.close()