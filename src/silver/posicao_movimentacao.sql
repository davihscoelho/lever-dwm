CREATE OR REPLACE TABLE silver.gdrive_posicao_movimentacoes AS
select
	grupo_familiar as client_id,
	try_strptime(REPLACE("data", 'jun.', '06'), '%m/%y') as date_id,
	custodia as custodia_id,
	moeda as moeda_id,
	TRY_CAST(REPLACE(REPLACE(valor_aporte, '.', ''), ',', '') AS DOUBLE) AS valor_aporte,
	TRY_CAST(REPLACE(REPLACE(valor_resgate, '.', ''), ',', '') AS DOUBLE) AS valor_resgate
from "lever-dwm".bronze.gdrive_posicao_movimentacao