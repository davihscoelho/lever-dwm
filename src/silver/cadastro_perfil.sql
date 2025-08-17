CREATE OR REPLACE TABLE silver.sheets_cadastro_perfil AS
SELECT 
  id_estrategia as perfil_id,
  nome_estrategia
from bronze.sheets_cadastro_perfil