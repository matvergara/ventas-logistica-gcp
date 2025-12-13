-- =====================================================
-- Fact Stock
-- Fuente: raw.stock
-- Grano: 1 producto en 1 sucursal en 1 fecha
-- =====================================================

CREATE TABLE IF NOT EXISTS `{{ project_id }}.dwh.fact_stock` (
  fecha DATE NOT NULL,
  producto_id STRING NOT NULL,
  sucursal_id INT64 NOT NULL,
  stock INT64
)
OPTIONS (
  description = "Hecho de stock diario"
);

-- =====================================================
-- Carga incremental (idempotente)
-- =====================================================

MERGE `{{ project_id }}.dwh.fact_stock` t
USING (

  SELECT
    s.fecha_cierre AS fecha,
    s.sku AS producto_id,
    s.sucursal AS sucursal_id,
    s.stock
  FROM `{{ project_id }}.raw.stock` s

) src
ON t.fecha = src.fecha
AND t.producto_id = src.producto_id
AND t.sucursal_id = src.sucursal_id

WHEN MATCHED THEN
  UPDATE SET
    stock = src.stock

WHEN NOT MATCHED THEN
  INSERT (
    fecha,
    producto_id,
    sucursal_id,
    stock
  )
  VALUES (
    src.fecha,
    src.producto_id,
    src.sucursal_id,
    src.stock
  );