"""
Ejecución del Data Warehouse (DWH).

- Ejecuta scripts SQL del esquema estrella
- No contiene lógica de negocio
- Orquesta en orden correcto
"""

import time
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from src.common.gcp_auth import get_bq_client
from src.common.logger import get_logger
from src.config import SQL_DWH_ORDER, SQL_DWH_PATH

logger = get_logger(__name__)

SQL_BASE_PATH = Path(SQL_DWH_PATH)


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


def run_sql(client: bigquery.Client, sql: str, label: str) -> None:
    """Ejecuta una query SQL en BigQuery y espera a que termine."""
    t0 = time.time()
    job = client.query(sql)
    job.result()
    elapsed = time.time() - t0
    logger.info("%s completado en %.1fs", label, elapsed)


def main() -> None:
    client = get_bq_client()
    project_id = client.project

    logger.info("Ejecutando Data Warehouse | proyecto=%s", project_id)

    for sql_file in SQL_DWH_ORDER:
        path = SQL_BASE_PATH / sql_file

        if not path.exists():
            raise FileNotFoundError(f"No se encontró el archivo SQL: {path}")

        logger.info("Ejecutando %s...", sql_file)
        sql = load_sql_file(path, project_id)

        try:
            run_sql(client, sql, sql_file)
        except GoogleCloudError as e:
            logger.error("Error ejecutando %s: %s", sql_file, e)
            raise

    logger.info("DWH actualizado correctamente.")


if __name__ == "__main__":
    main()
