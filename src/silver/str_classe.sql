CREATE OR REPLACE TABLE silver.sheets_cadastro_classe AS
select
  CAST(HASH(p.perfil_id, classe, peso) as varchar) as estrategia_classe_id, 
  p.perfil_id,
	classe,
	ROUND(COALESCE(TRY_CAST(REPLACE(REPLACE(peso, '%', ''), ',', '.') AS DOUBLE), 0.0) / 100,4) as peso
from bronze.sheets_cadastro_ALOCACAO_CLASSE a
LEFT join silver.sheets_cadastro_perfil p
  on a.nome_estrategia = p.nome_estrategia