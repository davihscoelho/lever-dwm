import duckdb 
from pathlib import Path

db_path = Path("lever.duckdb").resolve()
conn = duckdb.connect(str(db_path))#, read_only=True)
query = """
    CREATE SCHEMA IF NOT EXISTS bronze;
    CREATE SCHEMA IF NOT EXISTS silver;
    CREATE SCHEMA IF NOT EXISTS gold;
"""

conn.execute(query)

conn.close()
