/*
Datamart: dm_ventas_resumen

Grano:
- fecha
- provincia
- producto
- tipo_negocio

Uso:
- Fuente principal para Reporte Ventas en Looker
- KPIs, gráficos de distribución y mapa geográfico

Fuente:
- fact_ventas
- dim_producto
- dim_sucursal
*/

CREATE OR REPLACE VIEW datamart.dm_ventas_resumen AS
SELECT
    fv.fecha                              AS fecha,
    EXTRACT(YEAR FROM fv.fecha)           AS anio,
    EXTRACT(MONTH FROM fv.fecha)          AS mes,
    EXTRACT(WEEK FROM fv.fecha)           AS semana,

    ds.provincia                          AS provincia,
    dp.nombre_producto                    AS producto,
    dp.tipo_negocio                       AS tipo_negocio,

    SUM(fv.unidades_vendidas)             AS unidades_vendidas,
    SUM(fv.monto_vendido)                 AS monto_vendido

FROM dwh.fact_ventas fv
LEFT JOIN dwh.dim_producto dp
    ON fv.id_producto = dp.id_producto
LEFT JOIN dwh.dim_sucursal ds
    ON fv.id_sucursal = ds.id_sucursal

GROUP BY
    fecha,
    anio,
    mes,
    semana,
    provincia,
    producto,
    tipo_negocio;
