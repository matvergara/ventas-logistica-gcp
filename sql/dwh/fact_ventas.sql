-- =====================================================
-- Fact Ventas
-- Fuente: raw.ventas
-- Grano: 1 producto vendido a 1 cliente en 1 sucursal en 1 fecha
-- =====================================================

CREATE TABLE IF NOT EXISTS `{{ project_id }}.dwh.fact_ventas` (
  fecha DATE NOT NULL,
  cliente_id INT64 NOT NULL,
  producto_id STRING NOT NULL,
  sucursal_id INT64 NOT NULL,
  venta_unidades INT64,
  venta_importe FLOAT64
)
OPTIONS (
  description = "Hecho de ventas"
);

-- =====================================================
-- Carga incremental (idempotente)
-- =====================================================

INSERT INTO `{{ project_id }}.dwh.fact_ventas` (
  fecha,
  cliente_id,
  producto_id,
  sucursal_id,
  venta_unidades,
  venta_importe
)

SELECT
  v.fecha_cierre AS fecha,
  v.cliente AS cliente_id,
  v.sku AS producto_id,
  v.sucursal AS sucursal_id,
  v.venta_unidades,
  v.venta_importe
FROM `{{ project_id }}.raw.ventas` v
LEFT JOIN `{{ project_id }}.dwh.fact_ventas` f
  ON f.fecha = v.fecha_cierre
 AND f.cliente_id = v.cliente
 AND f.producto_id = v.sku
 AND f.sucursal_id = v.sucursal
WHERE f.fecha IS NULL;