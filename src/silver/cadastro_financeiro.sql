-- Check and Drop Duplicates
SELECT
  "nome ativo",
  count(*)
FROM bronze.sheets_cadastro_financeiros
GROUP BY 1
ORDER BY 2 DESC;

CREATE OR REPLACE TABLE silver.sheets_cadastro_financeiro AS 
SELECT
  CAST(HASH("nome ativo") AS VARCHAR) AS ativo_id,
	"nome ativo",
	"display name" as display_name,
	classe,
	"estrategia de risco" as risco_id,
	"estratégia de liquidez" as liquidez_id,
	"classificação" as classificacao,
	"região" as regiao_id,
	moeda as moeda_id,
	indexador,
	ROUND(COALESCE(TRY_CAST(REPLACE(REPLACE(taxa, '%', ''), ',', '.') AS DOUBLE), 0.0) / 100,4) as taxa,
	try_strptime(vencimento, '%d/%m/%Y') as vencimento,
	"rótulo de liquidez",
	CASE
    WHEN "rótulo de liquidez" = 'vencido' then 'A'
    WHEN "rótulo de liquidez" = 'd + 0' then 'B'
    WHEN "rótulo de liquidez" = 'd + 2' then 'C'
    WHEN "rótulo de liquidez" = 'd + 31' then 'D'
    WHEN "rótulo de liquidez" = 'd + 180' then 'E'
    WHEN "rótulo de liquidez" = 'd + 360' then 'F'
    WHEN "rótulo de liquidez" = 'acima de 360' then 'G'
    else 'Z'
  END as ordem_liquidez

FROM  (
  SELECT 
  ROW_NUMBER() OVER (PARTITION BY "nome ativo") as flag_last,
  *
  FROM bronze.sheets_cadastro_financeiros )
WHERE flag_last = 1