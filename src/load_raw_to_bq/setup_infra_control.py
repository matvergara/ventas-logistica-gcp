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


# ======================
# CONFIGURACIÓN
# ======================

DATASET_ID = "infra"
CONTROL_TABLE_ID = "control_archivos_cargados"
LOCATION = "US"


# ======================
# FUNCIONES
# ======================

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
    dataset_ref = f"{client.project}.{DATASET_ID}"

    if dataset_exists(client, dataset_ref):
        print(f"Dataset ya existe: {dataset_ref}")
        return

    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = LOCATION
    client.create_dataset(dataset)
    print(f"Dataset creado: {dataset_ref}")


def create_control_table(client: bigquery.Client) -> None:
    table_ref = f"{client.project}.{DATASET_ID}.{CONTROL_TABLE_ID}"

    if table_exists(client, table_ref):
        print(f"Tabla de control ya existe: {table_ref}")
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
    print(f"Tabla de control creada: {table_ref}")

    wait_table_ready(client, table_ref)


# ======================
# MAIN
# ======================

def main() -> None:
    client = get_bq_client()

    print("Setup infraestructura de control")
    print(f"Proyecto: {client.project}")
    print("-" * 60)

    create_infra_dataset(client)
    create_control_table(client)

    print("\nInfraestructura de control lista.")


if __name__ == "__main__":
    main()
