CREATE OR REPLACE TABLE silver.sheets_cadastro_custodia AS 

with ta as (
  select 
    TRIM(lOWER(COALESCE(banco, ''))) AS custodia,
  from bronze.sheets_cadastro_inputs 
  where "custodia" IS NOT NULL AND "custodia" <> ''
), 
-- Drop Duplicates 
tb as (
  select
  *,
  ROW_NUMBER() OVER (PARTITION BY custodia) as flag_last
  FROM ta
)

SELECT 
  CAST(HASH(custodia) AS VARCHAR) as custodia_id,
  *
FROM tb WHERE flag_last = 1
