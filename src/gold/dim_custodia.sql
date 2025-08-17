-- Dimensao custodia
CREATE OR REPLACE TABLE gold.dim_custodia as 
select * from silver.sheets_cadastro_custodia;