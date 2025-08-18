import duckdb 
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

folder_save = "data/output"
conn = duckdb.connect("md:lever-dwm") #, read_only=True)

tables = conn.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'gold'
""").fetchall()

for table in tables:
    conn.sql(f"SELECT * from gold.{table[0]}").to_csv(f"{folder_save}/{table[0]}.csv")
    print(f"Saving... {table[0]}")

conn.close()
