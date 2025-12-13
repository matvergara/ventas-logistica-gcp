-- =====================================================
-- Dimensión Sucursal
-- Fuente: raw.maestro
-- Grano: 1 fila por sucursal
-- =====================================================

CREATE TABLE IF NOT EXISTS `{{ project_id }}.dwh.dim_sucursal` (
  sucursal_id INT64 NOT NULL,
  distribuidor INT64,
  provincia STRING,
  ciudad STRING
)
OPTIONS (
  description = "Dimensión de sucursales"
);

-- =====================================================
-- Carga incremental (idempotente)
-- =====================================================

MERGE `{{ project_id }}.dwh.dim_sucursal` t
USING (

  SELECT
    sucursal AS sucursal_id,
    distribuidor,
    provincia,
    ciudad
  FROM `{{ project_id }}.raw.maestro`

) s
ON t.sucursal_id = s.sucursal_id

WHEN MATCHED THEN
  UPDATE SET
    distribuidor = s.distribuidor,
    provincia = s.provincia,
    ciudad = s.ciudad

WHEN NOT MATCHED THEN
  INSERT (
    sucursal_id,
    distribuidor,
    provincia,
    ciudad
  )
  VALUES (
    s.sucursal_id,
    s.distribuidor,
    s.provincia,
    s.ciudad
  );