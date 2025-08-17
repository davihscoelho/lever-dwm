CREATE OR REPLACE TABLE silver.sheets_cadastro_moedas AS 
-- Clean
with ta as (
  select 
    TRIM(lOWER(COALESCE("moeda base", ''))) AS nome_moeda,
  from bronze.sheets_cadastro_inputs 
  where "moeda base" IS NOT NULL AND "moeda base" <> ''
), 
-- Drop Duplicates 
tb as (
  select
  *,
  ROW_NUMBER() OVER (PARTITION BY nome_moeda) as flag_last
  FROM ta
)

-- Derived Columns: new_id
SELECT 
  CAST(HASH(nome_moeda) AS VARCHAR) as moeda_id,
  *
FROM tb WHERE flag_last = 1
