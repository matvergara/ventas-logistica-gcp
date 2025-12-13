-- =====================================================
-- Dimensión Producto
-- Fuente: raw.stock
-- Grano: 1 fila por producto (SKU)
-- =====================================================

CREATE TABLE IF NOT EXISTS `{{ project_id }}.dwh.dim_producto` (
  producto_id STRING NOT NULL,
  producto STRING,
  unidad STRING
)
OPTIONS (
  description = "Dimensión de productos"
);

-- =====================================================
-- Carga incremental (idempotente)
-- =====================================================

MERGE `{{ project_id }}.dwh.dim_producto` t
USING (

  SELECT
    sku AS producto_id,
    producto,
    unidad
  FROM `{{ project_id }}.raw.stock`

) s
ON t.producto_id = s.producto_id

WHEN MATCHED THEN
  UPDATE SET
    producto = s.producto,
    unidad = s.unidad

WHEN NOT MATCHED THEN
  INSERT (
    producto_id,
    producto,
    unidad
  )
  VALUES (
    s.producto_id,
    s.producto,
    s.unidad
  );