"""
Ejecución del Data Warehouse (DWH).

- Ejecuta scripts SQL del esquema estrella
- No contiene lógica de negocio
- Orquesta en orden correcto
"""

from pathlib import Path
from google.cloud import bigquery

from src.common.gcp_auth import get_bq_client


# ======================
# CONFIGURACIÓN
# ======================

SQL_BASE_PATH = Path("sql/dwh")

SQL_FILES_ORDER = [
    "dim_fecha.sql",
    "dim_cliente.sql",
    "dim_producto.sql",
    "dim_sucursal.sql",
    "fact_ventas.sql",
    "fact_stock.sql",
]


# ======================
# FUNCIONES
# ======================

def load_sql_file(path: Path, project_id: str) -> str:
    """
    Carga un archivo SQL y reemplaza placeholders.

    Args:
        path: Ruta al archivo SQL
        project_id: ID del proyecto GCP

    Returns:
        SQL listo para ejecutar
    """
    sql = path.read_text(encoding="utf-8")

    return sql.replace("{{ project_id }}", project_id)


def run_sql(client: bigquery.Client, sql: str) -> None:
    """
    Ejecuta una query SQL en BigQuery y espera a que termine.
    """
    job = client.query(sql)
    job.result()


# ======================
# MAIN
# ======================

def main() -> None:
    client = get_bq_client()
    project_id = client.project

    print("Ejecutando Data Warehouse")
    print(f"Proyecto: {project_id}")
    print("-" * 60)

    for sql_file in SQL_FILES_ORDER:
        path = SQL_BASE_PATH / sql_file

        if not path.exists():
            raise FileNotFoundError(f"No se encontró el archivo SQL: {path}")

        print(f"Ejecutando {sql_file}")

        sql = load_sql_file(path, project_id)
        run_sql(client, sql)

        print(f"{sql_file} ejecutado correctamente")

    print("\nDWH actualizado correctamente.")


if __name__ == "__main__":
    main()
