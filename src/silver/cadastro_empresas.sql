-- -- Check for nulls or duplicates
-- SELECT
--   "nome ativo",
--   count(*)
-- FROM bronze.sheets_cadastro_empresas
-- GROUP BY 1;

-- -- Check unwanted spaces
-- SELECT 
--   REPLACE(SUBSTRING("data compra", 1, 10), '-', '/')
-- FROM bronze.sheets_cadastro_empresas;
-- --WHERE "cidade" != TRIM("cidade");

-- 
CREATE OR REPLACE TABLE silver.sheets_cadastro_empresas AS 
SELECT
  CAST(HASH("nome ativo") AS VARCHAR) as ativo_id,
  "nome ativo" ,
  "Display Name" as display_name,
  "descrição ativo" as descricao_ativo,
  finalidade,
  tipo as categoria,
  "estrategia de risco" as risco_id,
  "estratégia de liquidez" as liquidez_id,
  classe as classe_id,
  moeda as moeda_id,
  cidade,
  estado,
  latitude,
  longitude
from bronze.sheets_cadastro_empresas
