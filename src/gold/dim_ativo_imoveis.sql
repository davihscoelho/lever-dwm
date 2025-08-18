CREATE OR REPLACE TABLE gold.dim_ativo_imoveis as 
select * from silver.sheets_cadastro_imoveis;