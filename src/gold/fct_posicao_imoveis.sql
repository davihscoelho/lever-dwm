CREATE OR REPLACE TABLE  gold.fct_posicao_imoveis AS
select
  b.cliente_id,
  c.ativo_id, 
  date_id,
  d.moeda_id,
  e.custodia_id,
  titular,
  status_estoque,
	status_uso,
  valor
FROM silver.gdrive_posicao_imoveis a
LEFT JOIN silver.sheets_cadastro_clientes b
  ON a.cliente_id = b."nome cliente"
LEFT JOIN silver.sheets_cadastro_imoveis c
  ON a.ativo_id = c."nome ativo"
LEFT JOIN silver.sheets_cadastro_moedas d
  ON a.moeda_id = d.nome_moeda
LEFT JOIN silver.sheets_cadastro_custodia e
  ON a.custodia_id = e.custodia
--WHERE e.custodia_id IS NULL