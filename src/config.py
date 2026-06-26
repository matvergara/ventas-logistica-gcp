"""Configuración centralizada del pipeline de ventas-logística."""

# ── Google Cloud Storage ──────────────────────────────────────────────────────
BUCKET_NAME = "ventas-logistica-raw"
GCS_BASE_PATH = "data"

# ── BigQuery ──────────────────────────────────────────────────────────────────
LOCATION = "US"

RAW_DATASET = "raw"
DWH_DATASET = "dwh"
DATAMARTS_DATASET = "datamarts"
INFRA_DATASET = "infra"
CONTROL_TABLE = "control_archivos_cargados"

TABLAS_RAW = ["ventas", "stock", "maestro"]

# ── Rutas SQL ─────────────────────────────────────────────────────────────────
SQL_DWH_PATH = "sql/dwh"
SQL_DATAMARTS_PATH = "sql/datamarts"

SQL_DWH_ORDER = [
    "dim_fecha.sql",
    "dim_cliente.sql",
    "dim_producto.sql",
    "dim_sucursal.sql",
    "fact_ventas.sql",
    "fact_stock.sql",
]

SQL_DATAMARTS_ORDER = [
    "dm_ventas.sql",
    "dm_stock.sql",
]
