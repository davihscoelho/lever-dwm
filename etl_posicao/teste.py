import pandas as pd

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


df1 = pd.DataFrame({
        'ID': [1, 2, 3],
        'Value': ['A', 'B', 'C']
})

df2 = pd.DataFrame({
        'ID': [3, 4, 5],
        'Value': ['C', 'D', 'E']
})

df3 = pd.DataFrame({
        'ID': [1, 6, 7],
        'Value': ['C', 'G', 'G']
})

df = concat_dfs(df1, df2)
df_final = upsert_dfs(df, ['ID'])

novas_linhas = len(df_final) - len(df1)
print(f"   -> {len(df_final) - novas_linhas} registros ATUALIZADOS.")
print(f"   -> {novas_linhas} registros INSERIDOS (Novos).")
df_final.head()

df = concat_dfs(df_final, df3)
df_final = upsert_dfs(df, ['ID'])
novas_linhas = len(df_final) - len(df1)
print(f"   -> {len(df_final) - novas_linhas} registros ATUALIZADOS.")
print(f"   -> {novas_linhas} registros INSERIDOS (Novos).")
df_final