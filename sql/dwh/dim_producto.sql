-- =====================================================
-- Dimensión Producto
-- Grano: 1 fila por SKU
-- =====================================================

CREATE OR REPLACE TABLE `{{ project_id }}.dwh.dim_producto` AS
SELECT
  DISTINCT sku AS producto_id,
  producto,
  unidad
FROM `{{ project_id }}.raw.stock`
ORDER BY producto_id;