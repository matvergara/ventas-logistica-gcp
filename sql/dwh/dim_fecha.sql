-- =====================================================
-- Dimensión Fecha
-- Grano: 1 fila por fecha
-- =====================================================

CREATE OR REPLACE TABLE `{{ project_id }}.dwh.dim_fecha` AS
WITH fechas AS (
  SELECT
    fecha
  FROM
    UNNEST(
      GENERATE_DATE_ARRAY(
        (SELECT MIN(fecha_cierre) FROM `{{ project_id }}.raw.ventas`),
        (SELECT MAX(fecha_cierre) FROM `{{ project_id }}.raw.ventas`)
      )
    ) AS fecha
)

SELECT
  fecha,
  EXTRACT(YEAR FROM fecha) AS anio,
  EXTRACT(MONTH FROM fecha) AS mes,
  EXTRACT(DAY FROM fecha) AS dia,
  EXTRACT(ISOYEAR FROM fecha) AS anio_iso,
  EXTRACT(ISOWEEK FROM fecha) AS semana_iso,
  CAST(FORMAT_DATE('%u', fecha) AS INT64) AS dia_semana_iso
FROM fechas;