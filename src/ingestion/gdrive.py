import duckdb
from dotenv import load_dotenv
from utils import autenticar, list_google_sheets_recursive, get_gsheet_sheet, clean_dataframe, insert_into_duckdb, insert_into_duckdb_with_metadata
import time

load_dotenv()

def definir_nome_planilha(sheet):
    
    name_planilha = sheet["name"]
    #print(name_planilha)
    name_planilha = (name_planilha
     .split("-",1)[-1]
     .rstrip("]")
     .strip()
     .lower()
     )
    #print(name_planilha)
    # Só faz split por espaço se houver espaço
    if " " in name_planilha:
        name_planilha = name_planilha.split(" ", 1)[1]

    lista_planilhas_tipos = ["empresas", "imóveis", "financeiro", "movimentacao"]
    
    if name_planilha in lista_planilhas_tipos:
        return name_planilha
    else:
        return None

def get_abas(tbls_name):

    """
        l1: Padrao de abas aceitaveis
        l2: Recebe uma lista de abas da planilha
        output: posições em que as abas da planilha correspondem ao padraõ de abas
    """
    l1 = ["receitas", "despesas", "movimentacao", "posicao"] # lista abas aceitaveis
    posicoes = []
    for elemento in l1:
        try:
            posicao = tbls_name.index(elemento)
            posicoes.append(posicao)
        except ValueError:
            continue
    return posicoes

def rename_tabela_naming(nome_worksheet, nome_sheet) -> str:
    """
    input: name_clean - cleaned name of the file
           tbl_name - name of the table
    output: formatted table name
    """
    if not nome_sheet:
        return f"{nome_worksheet}"
    return f"{nome_worksheet}_{nome_sheet}"

# 1 - Autenticar
drive, gc = autenticar()
conn = duckdb.connect("md:lever-dwm") #, read_only=True)

# 2 - Get Planilhas on Drive
ROOT_FOLDER_ID = "1umg5he9khM4PP_0rGSt5W9N0J-91DfCo"  # opcionalmente substitua por um ID de pasta específico
sheets = list_google_sheets_recursive(drive, ROOT_FOLDER_ID)

def ingestion_gdrive():
    # 3 - Get Planilha 
    for sheet in sheets:
        file_id = sheet["id"]
        range = "A1:Z1000"
        dfs, tbls_name = get_gsheet_sheet(gc, file_id, range)
        #print(tbls_name)
    # 4 - Get Abas 
        posicoes = get_abas(tbls_name)
        #print(posicoes)
    # 5 - Get and clean dataframe
        for posicao in posicoes:
            df = dfs[posicao]
            if df is None or df.empty:
                print("Skip: empty DataFrame")
                continue
            df = clean_dataframe(df)
            
    # 6 - Naming Convention
            nome_worksheet = "gdrive_posicao"
            nome_sheet =  definir_nome_planilha(sheet) or tbls_name[posicao]
            tbl_name = rename_tabela_naming(nome_worksheet, nome_sheet)

    # 7 - Send to duckdb
            #insert_into_duckdb(conn, df, tbl_name)
            insert_into_duckdb_with_metadata(conn, df, tbl_name, file_id)
        time.sleep(1.0)  # small pause to stay under per-minute limits
    conn.close()

ingestion_gdrive()

sheets
