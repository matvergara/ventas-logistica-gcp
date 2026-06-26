"""
Ejecución de Datamarts en BigQuery.

- Crea el dataset datamarts si no existe
- Ejecuta las vistas SQL orientadas a BI
"""

import time
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError, NotFound

from src.common.gcp_auth import get_bq_client
from src.common.logger import get_logger
from src.config import DATAMARTS_DATASET, LOCATION, SQL_DATAMARTS_ORDER, SQL_DATAMARTS_PATH

logger = get_logger(__name__)

SQL_BASE_PATH = Path(SQL_DATAMARTS_PATH)


def ensure_dataset(client: bigquery.Client, dataset_name: str) -> None:
    """Crea el dataset si no existe."""
    dataset_id = f"{client.project}.{dataset_name}"

    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = LOCATION
        client.create_dataset(dataset)
        logger.info("Dataset creado: %s", dataset_id)


def load_sql(path: Path, project_id: str) -> str:
    """Carga un SQL y reemplaza placeholders."""
    sql = path.read_text(encoding="utf-8")
    return sql.replace("{{ project_id }}", project_id)


def run_sql(client: bigquery.Client, sql: str, label: str) -> None:
    """Ejecuta una query SQL y espera a que finalice."""
    t0 = time.time()
    job = client.query(sql)
    job.result()
    elapsed = time.time() - t0
    logger.info("%s completado en %.1fs", label, elapsed)


def main() -> None:
    client = get_bq_client()
    project_id = client.project

    logger.info("Ejecutando Datamarts | proyecto=%s", project_id)

    ensure_dataset(client, DATAMARTS_DATASET)

    for sql_file in SQL_DATAMARTS_ORDER:
        path = SQL_BASE_PATH / sql_file

        if not path.exists():
            raise FileNotFoundError(f"No se encontró el archivo SQL: {path}")

        logger.info("Ejecutando %s...", sql_file)
        sql = load_sql(path, project_id)

        try:
            run_sql(client, sql, sql_file)
        except GoogleCloudError as e:
            logger.error("Error ejecutando %s: %s", sql_file, e)
            raise

    logger.info("Datamarts creados correctamente.")


if __name__ == "__main__":
    main()
