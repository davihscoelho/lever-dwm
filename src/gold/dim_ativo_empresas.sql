CREATE OR REPLACE TABLE gold.dim_ativo_empresas as 
select * from silver.sheets_cadastro_empresas;