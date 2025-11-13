import pandas as pd
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


df = {
    "retorno": pd.Series(['1,50%', '2,75%', None, '0,00%', '3,25%'])
}

clean_and_convert_percentage(df["retorno"])