import gspread
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

CREDENTIALS_FILE = "../service_account.json"
PROJECT_ID = "poc-pipeline-461721"

def auth(credentials_file=CREDENTIALS_FILE, project_id=PROJECT_ID):
    # 1. AUTENTICAÇÃO
    try:
        # Autenticação usando o arquivo de credenciais da conta de serviço
        # Nota: Se estiver usando o modo padrão da GCF, pode usar gspread.service_account()
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE
        )
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        client_bq = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        print("Autenticação bem-sucedida.")
        return gc, client_bq
    except Exception as e:
        print(f"Erro de Autenticação: {e}")
        return "Erro de Autenticação. Verifique o service_account.json e permissões.", 500

def extract(gc, SPREADSHEET_ID, SHEET_NAME):

    # 2. EXTRAÇÃO (Extract)
    try:
        # Abre a planilha e seleciona a aba
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        
        # Obtém todos os dados como uma lista de listas (raw data)
        
        data_list = worksheet.get_all_values()
        
        # Cria o DataFrame Pandas a partir da lista
        # A primeira linha é usada como cabeçalho
        headers = data_list[0]
        
        # Garante que nenhum cabeçalho seja uma string vazia (erro do BQ "Field missing name")
        # Substitui strings vazias por 'Unnamed_col_X'
        cleaned_headers = [
            header if header else f"Unnamed_col_{i}" 
            for i, header in enumerate(headers)
        ]

        df = pd.DataFrame(data_list[1:], columns=cleaned_headers)
        return df
    except Exception as e:
        return f"Erro na Extração do Sheets: {e}", 500

def clean_column_names(df):
    """
    Padroniza os nomes das colunas para estarem em conformidade com as regras do BigQuery.
    """
    new_columns = {}
    import re
    
    df.replace('', pd.NA, inplace=True) 
    df.dropna(how='all', inplace=True)

    for col in df.columns:
        # 1. Substitui caracteres inválidos (incluindo espaços, /, (, )) por underscore
        cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '_', col)
        
        # 2. Converte para minúsculas
        cleaned_name = cleaned_name.lower()
        
        # 3. Remove underscores duplicados e no início/fim
        cleaned_name = re.sub(r'_{2,}', '_', cleaned_name).strip('_')
        
        # Garante que o nome não fique vazio
        if not cleaned_name:
            cleaned_name = f"col_{df.columns.get_loc(col)}"

        new_columns[col] = cleaned_name
        
    df.rename(columns=new_columns, inplace=True)
    return df

def load_to_bigquery(df, client_bq, DATASET_ID, TABLE_ID):

    table_ref = client_bq.dataset(DATASET_ID).table(TABLE_ID)
    
    # Configuração da carga (job_config)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True, 
    )

    try:
        # Inicia o job de carga
        job = client_bq.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()  # Aguarda a conclusão do job
        
        print(f"Carga no BigQuery concluída. Tabela: {TABLE_ID} substituída.")
        return f"ETL concluído com sucesso. {len(df)} linhas carregadas.", 200

    except Exception as e:
        print(f"Erro na Carga para BigQuery: {e}")
        return f"Erro na Carga para BigQuery: {e}", 500
    
def get_table_from_bq(client_bq, dataset_id, table_id):
    from google.cloud import bigquery
    try:
        table_ref = client_bq.dataset(dataset_id).table(table_id)
        client_bq.get_table(table_ref)
        query = f"SELECT * FROM `{table_ref}`"
        df = client_bq.query(query).to_dataframe(create_bqstorage_client=True)
        
        print(f"1.Tabela encontrada. {len(df)} linhas existentes.")    
        return df
    
    except:
        print(f"⚠️ A tabela '{table_id}' não existe. Retornando DataFrame vazio.")
        return pd.DataFrame()

def concat_dfs(df1, df2):
    df3 = pd.concat([df1, df2], ignore_index=True)
    print(f"3. Concatenação T1+T2 = T3. Total de linhas: {len(df3)}.")
    return df3

def upsert_dfs(df, primary_key_cols):
    df_final = df.drop_duplicates(
        subset=primary_key_cols, 
        keep='last'  # Mantém a última ocorrência
    ).reset_index(drop=True)

    print(f"4. Drop Duplicates (T4) concluído. Linhas finais: {len(df_final)}.")
    return df_final

def clean_and_convert_to_float(series):
    """
    Limpa strings no formato numérico brasileiro (separador de milhar: '.', decimal: ',')
    e as converte para float padrão (separador decimal: '.').
    """
    # 1. Converte a Series para tipo string para usar métodos .str
    # fillna('') é crucial para evitar erros se houver NaN ou None
    series_str = series.astype(str).str.strip()
    
    # 2. Se a string estiver vazia ou for 'None', retorna NaN para evitar erro na conversão
    series_str = series_str.replace(['None', 'nan', ''], pd.NA, regex=False)
    
    # 3. Remove o separador de milhar (ponto)
    series_cleaned = series_str.str.replace('.', '', regex=False)
    
    # 4. Substitui o separador decimal (vírgula) por ponto
    series_cleaned = series_cleaned.str.replace(',', '.', regex=False)
    
    # 5. Converte para float
    return series_cleaned.astype(float)

def clean_and_convert_percentage(series):
    """
    Limpa strings de percentual (ex: '1,50%') e as converte para float (ex: 0.015).
    """
    # 1. Usa a função de limpeza padrão para tratar a formatação de vírgula/ponto
    series_cleaned = series.str.replace('%', '', regex=False)
    
    series_cleaned = series_cleaned.str.replace(',', '.', regex=False)
    
    series_cleaned = series_cleaned.astype(float)
    # 2. Divide por 100 para converter o valor percentual para o valor decimal (padrão)
    return series_cleaned / 100.0

