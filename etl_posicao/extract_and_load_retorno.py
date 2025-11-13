import pandas as pd
import gspread
from auxiliar import *

PIPELINE_CLIENTS = [
    {
        "client_name": "TESTE", 
        "sheet_id": "17H1XDHSEDlqd3WZ1QBqJAVAgIBhNxerRZJ1o9TQxKAQ",
        "PROJECT_ID": "poc-pipeline-461721",
        "DATASET_ID": "bronze",
        "tasks": [
            {
                "source_table_id": "retorno",           # Nome da Aba no Gsheets
                "destination_table_id": "gsheets_posicao_retorno", # Destino BQ
                "primary_key_cols": ["data", "cliente", "moeda"],
                "COLUNAS_MONETARIAS": ['pl_final', 'pl_inicial', 'aporte', 'resgate', 'net', "retorno_monetario"],
                "COLUNAS_PERCENTUAIS": ["retorno"]
            }
        ]
    }
]

def standardize_numeric_data(df, colunas_monetarias, colunas_percentuais):
    for col in colunas_monetarias:
        df[col] = clean_and_convert_to_float(df[col])

    for col in colunas_percentuais:
        df[col] = clean_and_convert_percentage(df[col])
    
    return df

client_gc, client_bq = auth()
client = PIPELINE_CLIENTS[0]
SPREADSHEET_ID = client["sheet_id"]

for task in client["tasks"]:
    SHEET_NAME = task["source_table_id"]
    TABLE_ID = task["destination_table_id"]
    DATASET_ID = client["DATASET_ID"]
    
    df = extract(client_gc, SPREADSHEET_ID, SHEET_NAME)
    df = clean_column_names(df)
    df = standardize_numeric_data(
        df, 
        task["COLUNAS_MONETARIAS"], 
        task["COLUNAS_PERCENTUAIS"]
    )
    df_existente = get_table_from_bq(
        client_bq, 
        DATASET_ID, 
        TABLE_ID
    )
    if df_existente.empty:
        print("Tabela vazia. Carregando todos os dados extraídos.")
        load_to_bigquery(
            df, 
            client_bq, 
            DATASET_ID, 
            TABLE_ID
        )
        continue
    df3 = concat_dfs(df_existente, df)
    df_final = upsert_dfs(df3, task["primary_key_cols"])
    load_to_bigquery(
        df_final, 
        client_bq, 
        DATASET_ID, 
        TABLE_ID
    )
