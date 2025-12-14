-- =====================================================
-- Dimensión Cliente
-- Fuente: raw.maestro
-- Grano: 1 fila por cliente
-- =====================================================

CREATE OR REPLACE TABLE `{{ project_id }}.dwh.dim_cliente` AS
SELECT
  DISTINCT cliente AS cliente_id,
  provincia,
  coordenada_latitud,
  coordenada_longitud,
  tipo_negocio,
  sucursal
FROM `{{ project_id }}.raw.maestro`
ORDER BY cliente_id;