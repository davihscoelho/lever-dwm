CREATE OR REPLACE TABLE gold.dim_ativo_financeiro as 
select * from silver.sheets_cadastro_financeiro;