-- =====================================================
-- Datamart Stock
-- Orientado a consumo en Looker Studio
-- =====================================================

CREATE OR REPLACE VIEW `{{ project_id }}.datamarts.dm_stock` AS
SELECT
  fs.stock,
  df.fecha,
  df.anio,
  df.mes,
  df.semana_iso,
  dp.producto,
  ds.distribuidor
FROM `{{ project_id }}.dwh.fact_stock` fs
JOIN `{{ project_id }}.dwh.dim_fecha` df
  ON fs.fecha = df.fecha
JOIN `{{ project_id }}.dwh.dim_producto` dp
  ON fs.producto_id = dp.producto_id
JOIN `{{ project_id }}.dwh.dim_sucursal` ds
  ON fs.sucursal_id = ds.sucursal_id;
