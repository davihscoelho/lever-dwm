CREATE OR REPLACE TABLE silver.gdrive_posicao_empresas AS 
select
	grupo_familiar as cliente_id,
	clientes as titular,
	ativo as ativo_id,
    try_strptime(REPLACE("data", 'jun.', '06'), '%m/%y') as date_id,
	moeda as moeda_id,
	custodia as custodia_id,
	status_estoque,
	"status compra",
	"status venda",
    TRY_CAST(REPLACE(REPLACE(valor, '.', ''), ',', '') AS DOUBLE) AS valor
from "lever-dwm".bronze.gdrive_posicao_empresas
where ativo_id <> ''