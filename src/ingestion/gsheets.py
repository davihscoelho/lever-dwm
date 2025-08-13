from utils import autenticar, clean_dataframe, get_gsheet_sheet, insert_into_duckdb, insert_into_duckdb_with_metadata
import pandas as pd
import duckdb
from dotenv import load_dotenv

load_dotenv()

def rename_tabela_naming(nome_worksheet, nome_sheet) -> str:
    """
    input: name_clean - cleaned name of the file
           tbl_name - name of the table
    output: formatted table name
    """
    if not nome_sheet:
        return f"{nome_worksheet}"
    return f"{nome_worksheet}_{nome_sheet}"


def ingestion_gsheets():

    # 1 - Autenticacao
    drive, gc = autenticar()
    conn = duckdb.connect("md:lever-dwm") #, read_only=True)

    # 2 - Get sheets - Requisição
    file_ids = {
        "cadastro_ativos": "1EunZ8cbXRO1g8toO1HcNWXMESKJNLJ1kQOzsDbnhxX8",
        "cadastro_geral": "1QjDW3Zn8f2vrB3nb4fOWYcJtgZARKWz4odp8jcLyXlk"
    }
    # Get sheets
    for file in file_ids:
        file_id = file_ids[file]
        range = "A1:Z1000"
        dfs, tbls_name = get_gsheet_sheet(gc, file_id, range)

    # To send Duckb
        table_name = "sheets_cadastro"
        i = 0

        for df in dfs:
            if df is None or df.empty:
                print("Skip: empty DataFrame")
                continue
                
            # 3 - Limpar Dataframe
            df = clean_dataframe(df)
            
            # 4 - Naming Convention
            tbl_name = tbls_name[i]
            tbl_name = rename_tabela_naming(table_name, tbl_name)

            # 5 - To send to duckdb
            insert_into_duckdb_with_metadata(conn, df, tbl_name, file_id)
            i +=1

    conn.close()

ingestion_gsheets()