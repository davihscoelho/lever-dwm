
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread
import re
import pandas as pd
import uuid
from gspread.exceptions import APIError
import time

def autenticar():

    SCOPES = [
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/spreadsheets.readonly",
    ]
    SA_PATH = "../../config/poc-pipeline-461721-6aff0d3b47eb.json"  # caminho para o JSON da service account
    #ROOT_FOLDER_ID = "1umg5he9khM4PP_0rGSt5W9N0J-91DfCo"  # opcionalmente substitua por um ID de pasta específico

    creds = service_account.Credentials.from_service_account_file(SA_PATH, scopes=SCOPES)
    drive = build("drive", "v3", credentials=creds)
    gc = gspread.authorize(creds)

    return drive, gc

def clean_dataframe(df):
    """
    input: cols DataFrame to be cleaned
    output: cleaned DataFrame
    """
    df.columns = [col.strip().lower() for col in df.columns]
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.map(lambda x: x.lower() if isinstance(x, str) else x)
    df = df.dropna(how="all")
    return df

def get_gsheet_sheet(gc, file_id, range):
    # Dica: este retorno estava dentro do for na sua função,
    # o que faz retornar só a primeira aba. Aqui eu retorno uma lista.
    sh = api(gc.open_by_key, file_id)
    wss = api(sh.worksheets)
    ranges = [f"'{ws.title}'!{range}" for ws in wss]

    # One batch call for all tabs
    batch = api(sh.values_batch_get, ranges)
    dfs, names = [], []

    for vr in batch.get("valueRanges", []):
            values = vr.get("values", [])
            df = values_to_dataframe(values)
            if not values or len(values) < 2:
                continue
            #header = [str(c).strip() for c in values[0]]
            #df = pd.DataFrame(values[1:], columns=header)
            tab_name = vr["range"].split("!")[0].strip("'")
            dfs.append(df)
            names.append(tab_name)

    return dfs, names

def insert_into_duckdb(conn, df, table_name):
    
    conn.register("tmp_df", df)
    
    table_name = f"bronze.{table_name}"
    
    conn.execute(
    f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM tmp_df"
    )
    print(f"Carga realizada into {table_name}")

def list_google_sheets_recursive(drive, root_id="root"):
    sheets = []
    stack = [root_id]

    while stack:
        parent = stack.pop()
        page_token = None
        while True:
            resp = drive.files().list(
                q=f"'{parent}' in parents and trashed=false",
                fields="nextPageToken, files(id, name, mimeType)",
                pageSize=1000,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=page_token,
            ).execute()

            for f in resp.get("files", []):
                if f["mimeType"] == "application/vnd.google-apps.folder":
                    stack.append(f["id"])
                elif f["mimeType"] == "application/vnd.google-apps.spreadsheet":
                    sheets.append({"id": f["id"], "name": f["name"]})

            page_token = resp.get("nextPageToken")
            if not page_token:
                break
    return sheets

def api(func, *args, **kwargs):
    for i in range(6):  # up to 6 tries
        try:
            return func(*args, **kwargs)
        except APIError as e:
            code = getattr(e.response, "status_code", None)
            if code in (429, 503):
                ra = e.response.headers.get("Retry-After")
                wait = float(ra) if ra else min(60, 2 ** i)
                print(f"Rate limited ({code}). Waiting {wait:.1f}s...")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("Max retries exceeded")



def insert_into_duckdb_with_metadata(conn, df, table_name, source_info=None):
    conn.register("tmp_df", df)
    table_name = f"bronze.{table_name}"
    
    # Add metadata columns
    df_with_meta = df.copy()
    df_with_meta['_loaded_at'] = pd.Timestamp.now()
    df_with_meta['_source'] = source_info or 'unknown'
    df_with_meta['_batch_id'] = str(uuid.uuid4())[:8]
    
    conn.register("tmp_df_meta", df_with_meta)
    
    #conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM tmp_df_meta WHERE 1=0")
    
    # Check if this source was already loaded today
    existing = conn.execute(f"""
        SELECT COUNT(*) as cnt FROM {table_name} 
        WHERE _source = '{source_info}' 
        AND DATE(_loaded_at) = CURRENT_DATE
    """).fetchone()[0]
    
    if existing > 0:
        print(f"Source {source_info} already loaded today, skipping...")
        return
    
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM tmp_df_meta")
    print(f"Loaded {len(df)} rows to {table_name} from {source_info}")

def values_to_dataframe(values):
    if not values:
        return pd.DataFrame() # Return empty DataFrame if no values

    header = [str(c).strip() for c in values[0]]
    data = values[1:]

    # Pad each data row to match the header length
    padded_data = []
    for row in data:
        if len(row) < len(header):
            padded_row = row + [None] * (len(header) - len(row))
        else:
            padded_row = row[:len(header)] # Trim if row is longer than header
        padded_data.append(padded_row)

    return pd.DataFrame(padded_data, columns=header)

