"""
Subida de archivos locales a Google Cloud Storage.

Estructura destino en GCS:

data/
└── distribuidor_{N}/
    ├── ventas/
    ├── stock/
    └── maestro/
"""

from pathlib import Path

from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

from src.common.gcp_auth import get_gcs_client
from src.common.logger import get_logger
from src.config import BUCKET_NAME, GCS_BASE_PATH

logger = get_logger(__name__)

LOCAL_BASE_PATH = Path("data")

TIPO_MAP = {
    "Archivos_VentaClientes": "ventas",
    "Archivos_Stock": "stock",
    "Archivos_Maestro": "maestro",
}


def get_or_create_bucket(client: storage.Client, bucket_name: str) -> storage.Bucket:
    try:
        bucket = client.get_bucket(bucket_name)
        logger.info("Bucket encontrado: %s", bucket_name)
        return bucket
    except GoogleCloudError:
        logger.info("Bucket no encontrado. Creando: %s", bucket_name)
        return client.create_bucket(bucket_name)


def upload_all_files(bucket: storage.Bucket) -> None:
    if not LOCAL_BASE_PATH.exists():
        raise FileNotFoundError("No existe la carpeta local 'data/'")

    total = 0
    for tipo_local, tipo_gcs in TIPO_MAP.items():
        base_tipo_path = LOCAL_BASE_PATH / tipo_local

        if not base_tipo_path.exists():
            logger.warning("Carpeta no encontrada: %s", base_tipo_path)
            continue

        for distribuidor_dir in base_tipo_path.iterdir():
            if not distribuidor_dir.is_dir():
                continue

            distribuidor = distribuidor_dir.name.lower()  # Distribuidor_1 → distribuidor_1

            for archivo in distribuidor_dir.glob("*.csv"):
                blob_path = f"{GCS_BASE_PATH}/{distribuidor}/{tipo_gcs}/{archivo.name}"
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(archivo)
                total += 1
                logger.info("Subido: gs://%s/%s", bucket.name, blob_path)

    logger.info("Subida a GCS completada. Total archivos: %d", total)


def main() -> None:
    client = get_gcs_client()
    bucket = get_or_create_bucket(client, BUCKET_NAME)
    upload_all_files(bucket)


if __name__ == "__main__":
    main()
