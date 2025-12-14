"""
Ejecución de Datamarts en BigQuery.

- Crea el dataset datamarts si no existe
- Ejecuta las vistas SQL orientadas a BI
"""

from pathlib import Path
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src.common.gcp_auth import get_bq_client


# ======================
# CONFIGURACIÓN
# ======================

SQL_BASE_PATH = Path("sql/datamarts")

SQL_FILES_ORDER = [
    "dm_ventas.sql",
    "dm_stock.sql",
]


# ======================
# FUNCIONES
# ======================

def ensure_dataset(client: bigquery.Client, dataset_name: str) -> None:
    """
    Crea el dataset si no existe.
    """
    dataset_id = f"{client.project}.{dataset_name}"

    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)


def load_sql(path: Path, project_id: str) -> str:
    """
    Carga un SQL y reemplaza placeholders.
    """
    sql = path.read_text(encoding="utf-8")
    return sql.replace("{{ project_id }}", project_id)


def run_sql(client: bigquery.Client, sql: str) -> None:
    """
    Ejecuta una query SQL y espera a que finalice.
    """
    job = client.query(sql)
    job.result()


# ======================
# MAIN
# ======================

def main() -> None:
    client = get_bq_client()
    project_id = client.project

    print("Ejecutando Datamarts")
    print(f"Proyecto: {project_id}")
    print("-" * 60)

    ensure_dataset(client, "datamarts")

    for sql_file in SQL_FILES_ORDER:
        path = SQL_BASE_PATH / sql_file

        if not path.exists():
            raise FileNotFoundError(f"No se encontró el archivo SQL: {path}")

        print(f"➡ Ejecutando {sql_file}")

        sql = load_sql(path, project_id)
        run_sql(client, sql)

        print(f"{sql_file} ejecutado correctamente")

    print("\nDatamarts creados correctamente.")


if __name__ == "__main__":
    main()