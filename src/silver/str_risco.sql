CREATE OR REPLACE table silver.sheets_cadastro_risco AS
select
  CAST(HASH(p.perfil_id, "tipo risco", "categoria estrategia", peso) AS VARCHAR) AS estrategia_risco_id,
	p.perfil_id,
	"tipo risco" as tipo_risco,
	"categoria estrategia" categoria_estrategia,
  ROUND(COALESCE(TRY_CAST(REPLACE(REPLACE(peso, '%', ''), ',', '.') AS DOUBLE), 0.0) / 100,4) as peso
	--peso
FROM bronze.sheets_cadastro_ALOCAÇÃO_RISCO a
LEFT JOIN silver.sheets_cadastro_perfil p
ON a.perfil = p.nome_estrategia