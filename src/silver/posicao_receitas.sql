CREATE OR REPLACE TABLE silver.gdrive_posicao_receitas AS 
select
	grupo_familiar as cliente_id,
	clientes as titular,
	try_strptime("data", '%d/%m/%Y') as date_id,
	descricao,
	natureza,
	categoria,
	moeda as moeda_id,
	custodia as custodia_id,
  TRY_CAST(
    REPLACE(REPLACE(valor, '.', ''), ',','.')
    AS DOUBLE) AS valor
FROM bronze.gdrive_posicao_receitas