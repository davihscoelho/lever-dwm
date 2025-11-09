import pandas as pd
import gspread
from auxiliar import auth,  extract, clean_column_names, load_to_bigquery

PIPELINE_CLIENTS = [
    {
        "client_name": "TESTE", 
        "sheet_id": "17H1XDHSEDlqd3WZ1QBqJAVAgIBhNxerRZJ1o9TQxKAQ",
        "PROJECT_ID": "poc-pipeline-461721",
        "DATASET_ID": "bronze",
        "tasks": [
            {
                "source_table_id": "posicao_financeiro",           # Nome da Aba no Gsheets
                "destination_table_id": "gsheets_posicao_financeiro", # Destino BQ
                "primary_key_cols": ["id_movimento", "client_name"]
            }, 
            {
                "source_table_id": "posicao_imoveis",           # Nome da Aba no Gsheets
                "destination_table_id": "gsheets_posicao_imoveis", # Destino BQ 
                "primary_key_cols": ["id_movimento", "client_name"]
            }, 
        ]
    }
]


client_gc, client_bq = auth()
client = PIPELINE_CLIENTS[0]
SPREADSHEET_ID = client["sheet_id"]
DATASET_ID = client["DATASET_ID"]
for task in client["tasks"]:
    SHEET_NAME = task["source_table_id"]
    TABLE_ID = task["destination_table_id"]
    df = extract(client_gc, SPREADSHEET_ID, SHEET_NAME)
    df = clean_column_names(df)
    load_to_bigquery(
        df, 
        client_bq, 
        DATASET_ID, 
        TABLE_ID
    )
