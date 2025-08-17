CREATE OR REPLACE TABLE silver.gdrive_posicao_despesas AS
select
	grupo_familiar as cliente_id,
	clientes as titular,
	try_strptime("data", '%d/%m/%Y') as date_id,
	descricao,
	natureza,
	categoria,
	moeda,
	custodia,
	TRY_CAST(REPLACE(REPLACE(valor, '.', ''), ',', '') AS DOUBLE) AS valor
from "lever-dwm".bronze.gdrive_posicao_despesas