CREATE OR REPLACE TABLE silver.gdrive_posicao_financeiro AS 
select
	grupo_familiar as cliente_id,
	clientes as titular,
	ativo as ativo_id,
	try_strptime("data", '%m/%y') as date_id,
	moeda as moeda_id,
    CASE 
        when custodia = 'itaú' THEN 'itau'
        ELSE custodia
	END as custodia_id,
	TRY_CAST(REPLACE(REPLACE(valor, '.', ''), ',', '') AS DOUBLE) AS valor
from "lever-dwm".bronze.gdrive_posicao_financeiro
where ativo_id <> ''