CREATE OR REPLACE TABLE silver.sheets_cadastro_clientes AS
select
  CAST(HASH("nome cliente") AS VARCHAR) as cliente_id,
	"nome cliente",
	"grupo cliente",
	link,
	p.perfil_id
from "lever-dwm".bronze.sheets_cadastro_CLIENTE c
LEFT JOIN silver.sheets_cadastro_perfil p
  ON c.perfil = p."nome_estrategia"