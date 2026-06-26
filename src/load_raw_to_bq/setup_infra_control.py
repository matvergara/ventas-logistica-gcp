"""
Setup de infraestructura para control de cargas incrementales.

Crea:
- Dataset infra
- Tabla infra.control_archivos_cargados

Este script es idempotente.
"""

import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src.common.gcp_auth import get_bq_client
from src.common.logger import get_logger
from src.config import INFRA_DATASET, CONTROL_TABLE, LOCATION

logger = get_logger(__name__)


def dataset_exists(client: bigquery.Client, dataset_id: str) -> bool:
    try:
        client.get_dataset(dataset_id)
        return True
    except NotFound:
        return False


def table_exists(client: bigquery.Client, table_id: str) -> bool:
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def wait_table_ready(client: bigquery.Client, table_id: str, timeout: int = 60) -> None:
    start = time.time()
    while True:
        try:
            client.get_table(table_id)
            return
        except NotFound:
            if time.time() - start > timeout:
                raise TimeoutError(f"Timeout esperando tabla {table_id}")
            time.sleep(2)


def create_infra_dataset(client: bigquery.Client) -> None:
    dataset_ref = f"{client.project}.{INFRA_DATASET}"

    if dataset_exists(client, dataset_ref):
        logger.info("Dataset ya existe: %s", dataset_ref)
        return

    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = LOCATION
    client.create_dataset(dataset)
    logger.info("Dataset creado: %s", dataset_ref)


def create_control_table(client: bigquery.Client) -> None:
    table_ref = f"{client.project}.{INFRA_DATASET}.{CONTROL_TABLE}"

    if table_exists(client, table_ref):
        logger.info("Tabla de control ya existe: %s", table_ref)
        return

    schema = [
        bigquery.SchemaField("bucket", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("object_path", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("generation", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("crc32c", "STRING"),
        bigquery.SchemaField("tabla", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("distribuidor", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("fecha_actualizacion", "TIMESTAMP"),
    ]

    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    logger.info("Tabla de control creada: %s", table_ref)

    wait_table_ready(client, table_ref)


def main() -> None:
    client = get_bq_client()

    logger.info("Setup infraestructura de control | proyecto=%s", client.project)

    create_infra_dataset(client)
    create_control_table(client)

    logger.info("Infraestructura de control lista.")


if __name__ == "__main__":
    main()
