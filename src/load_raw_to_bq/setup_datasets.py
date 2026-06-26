"""
Creación de datasets en BigQuery para el pipeline analítico.

Este script prepara la infraestructura mínima en BigQuery:
- raw
- dwh
- datamarts

Es idempotente: puede ejecutarse múltiples veces sin romper nada.
"""

from google.api_core.exceptions import Conflict
from google.cloud import bigquery

from src.common.gcp_auth import get_bq_client
from src.common.logger import get_logger
from src.config import LOCATION

logger = get_logger(__name__)

DATASETS = [
    {
        "dataset_id": "raw",
        "description": "Capa RAW: copia fiel de archivos provenientes de Cloud Storage",
    },
    {
        "dataset_id": "dwh",
        "description": "Data Warehouse: modelo dimensional (esquema estrella)",
    },
    {
        "dataset_id": "datamarts",
        "description": "Datamarts analíticos para consumo BI (Looker Studio)",
    },
]


def create_dataset(client: bigquery.Client, dataset_id: str, description: str) -> None:
    """
    Crea un dataset en BigQuery si no existe.

    Args:
        client: Cliente autenticado de BigQuery
        dataset_id: Nombre del dataset
        description: Descripción del dataset
    """
    dataset_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
    dataset_ref.location = LOCATION
    dataset_ref.description = description

    try:
        client.create_dataset(dataset_ref)
        logger.info("Dataset creado: %s", dataset_id)
    except Conflict:
        logger.info("Dataset ya existe: %s", dataset_id)


def main() -> None:
    client = get_bq_client()

    logger.info("Creando datasets en BigQuery | proyecto=%s", client.project)

    for ds in DATASETS:
        create_dataset(
            client=client,
            dataset_id=ds["dataset_id"],
            description=ds["description"],
        )

    logger.info("Setup de datasets completado.")


if __name__ == "__main__":
    main()
