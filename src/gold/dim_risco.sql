-- Dimensao Risco
CREATE OR REPLACE TABLE gold.dim_risco as 
select * from silver.sheets_cadastro_risco;