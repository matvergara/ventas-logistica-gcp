-- =====================================================
-- Datamart Ventas
-- Orientado a consumo en Looker Studio
-- =====================================================

CREATE OR REPLACE VIEW `{{ project_id }}.datamarts.dm_ventas` AS
SELECT
  fv.venta_unidades,
  fv.venta_importe,
  df.fecha,
  df.anio,
  df.mes,
  df.semana_iso,
  dp.producto,
  dc.provincia,
  CONCAT(CAST(dc.coordenada_latitud AS STRING), ',', CAST(dc.coordenada_longitud AS STRING)) AS coordenadas,
  dc.tipo_negocio,
  dc.sucursal
FROM `{{ project_id }}.dwh.fact_ventas` fv
JOIN `{{ project_id }}.dwh.dim_fecha` df
  ON fv.fecha = df.fecha
JOIN `{{ project_id }}.dwh.dim_cliente` dc
  ON fv.cliente_id = dc.cliente_id
JOIN `{{ project_id }}.dwh.dim_producto` dp
  ON fv.producto_id = dp.producto_id
JOIN `{{ project_id }}.dwh.dim_sucursal` ds
  ON fv.sucursal_id = ds.sucursal_id;
