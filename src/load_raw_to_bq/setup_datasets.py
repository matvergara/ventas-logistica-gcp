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


# ======================
# CONFIGURACIÓN
# ======================

PROJECT_ID = None  # Si es None, se toma del cliente autenticado
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


# ======================
# FUNCIONES
# ======================

def create_dataset(client: bigquery.Client, dataset_id: str, description: str) -> None:
    """
    Crea un dataset en BigQuery si no existe.

    Args:
        client: Cliente autenticado de BigQuery
        dataset_id: Nombre del dataset
        description: Descripción del dataset
    """
    dataset_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
    dataset_ref.location = "US"
    dataset_ref.description = description

    try:
        client.create_dataset(dataset_ref)
        print(f"Dataset creado: {dataset_id}")
    except Conflict:
        print(f"Dataset ya existe: {dataset_id}")


# ======================
# MAIN
# ======================

def main() -> None:
    """
    Punto de entrada del script.
    """
    client = get_bq_client()

    print("Creando datasets en BigQuery")
    print(f"Proyecto: {client.project}")
    print("-" * 50)

    for ds in DATASETS:
        create_dataset(
            client=client,
            dataset_id=ds["dataset_id"],
            description=ds["description"],
        )

    print("\nSetup de datasets completado.")


if __name__ == "__main__":
    main()
