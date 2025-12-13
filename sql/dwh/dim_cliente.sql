-- =====================================================
-- Dimensión Cliente
-- Fuente: raw.maestro
-- Grano: 1 fila por cliente
-- =====================================================

CREATE TABLE IF NOT EXISTS `{{ project_id }}.dwh.dim_cliente` (
  cliente_id INT64 NOT NULL,
  provincia STRING,
  ciudad STRING,
  tipo_negocio STRING,
  condicion_venta STRING,
  sucursal_id INT64,
  latitud FLOAT64,
  longitud FLOAT64
)
OPTIONS (
  description = "Dimensión de clientes"
);

-- =====================================================
-- Carga incremental (idempotente)
-- =====================================================

MERGE `{{ project_id }}.dwh.dim_cliente` t
USING (

  SELECT
    cliente AS cliente_id,
    provincia,
    ciudad,
    tipo_negocio,
    condicion_venta,
    sucursal AS sucursal_id,
    coordenada_latitud AS latitud,
    coordenada_longitud AS longitud
  FROM `{{ project_id }}.raw.maestro`

) s
ON t.cliente_id = s.cliente_id

WHEN MATCHED THEN
  UPDATE SET
    provincia = s.provincia,
    ciudad = s.ciudad,
    tipo_negocio = s.tipo_negocio,
    condicion_venta = s.condicion_venta,
    sucursal_id = s.sucursal_id,
    latitud = s.latitud,
    longitud = s.longitud

WHEN NOT MATCHED THEN
  INSERT (
    cliente_id,
    provincia,
    ciudad,
    tipo_negocio,
    condicion_venta,
    sucursal_id,
    latitud,
    longitud
  )
  VALUES (
    s.cliente_id,
    s.provincia,
    s.ciudad,
    s.tipo_negocio,
    s.condicion_venta,
    s.sucursal_id,
    s.latitud,
    s.longitud
  );