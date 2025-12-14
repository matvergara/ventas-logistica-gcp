-- =====================================================
-- Dimensión Sucursal
-- Fuente: raw.maestro (base) + raw.stock (complemento)
-- Grano: 1 fila por sucursal
-- Descripción: Carga todas las sucursales del maestro y agrega
--              las sucursales de stock QUE NO EXISTEN en maestro.
-- =====================================================

CREATE OR REPLACE TABLE `{{ project_id }}.dwh.dim_sucursal` AS

-- 1. Tomamos todas las sucursales y distribuidores del Maestro (Fuente de Verdad)
SELECT DISTINCT 
    sucursal AS sucursal_id,
    distribuidor
FROM `{{ project_id }}.raw.maestro`

UNION ALL

-- 2. Agregamos las de Stock, pero SOLO si NO están ya en el Maestro
SELECT DISTINCT 
    sucursal AS sucursal_id,
    distribuidor
FROM `{{ project_id }}.raw.stock`
WHERE sucursal NOT IN (
    SELECT DISTINCT sucursal 
    FROM `{{ project_id }}.raw.maestro` 
    WHERE sucursal IS NOT NULL
)
ORDER BY sucursal_id;