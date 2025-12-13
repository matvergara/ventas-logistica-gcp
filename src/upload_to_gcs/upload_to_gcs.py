"""
Subida de archivos locales a Google Cloud Storage
respetando la estructura original del proyecto.

Estructura destino en GCS:

data/
└── distribuidor_{N}/
    ├── ventas/
    ├── stock/
    └── maestro/
"""

from pathlib import Path
from typing import Dict

from google.cloud import storage
from src.common.gcp_auth import get_gcs_client


# ======================
# CONFIGURACIÓN
# ======================

BUCKET_NAME = "ventas-logistica-raw"
LOCAL_BASE_PATH = Path("data")
GCS_BASE_PATH = "data"

TIPO_MAP = {
    "Archivos_VentaClientes": "ventas",
    "Archivos_Stock": "stock",
    "Archivos_Maestro": "maestro",
}


# ======================
# FUNCIONES
# ======================

def get_or_create_bucket(client: storage.Client, bucket_name: str) -> storage.Bucket:
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"Bucket encontrado: {bucket_name}")
        return bucket
    except Exception:
        print(f"Bucket no encontrado. Creando: {bucket_name}")
        return client.create_bucket(bucket_name)


def upload_all_files(bucket: storage.Bucket) -> None:
    if not LOCAL_BASE_PATH.exists():
        raise FileNotFoundError("No existe la carpeta local 'data/'")

    for tipo_local, tipo_gcs in TIPO_MAP.items():
        base_tipo_path = LOCAL_BASE_PATH / tipo_local

        if not base_tipo_path.exists():
            print(f"⚠ Carpeta no encontrada: {base_tipo_path}")
            continue

        for distribuidor_dir in base_tipo_path.iterdir():
            if not distribuidor_dir.is_dir():
                continue

            distribuidor = distribuidor_dir.name.lower()  # Distribuidor_1 → distribuidor_1

            for archivo in distribuidor_dir.glob("*.csv"):
                blob_path = f"{GCS_BASE_PATH}/{distribuidor}/{tipo_gcs}/{archivo.name}"
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(archivo)

                print(f"Subido: gs://{bucket.name}/{blob_path}")


# ======================
# MAIN
# ======================

def main() -> None:
    client = get_gcs_client()
    bucket = get_or_create_bucket(client, BUCKET_NAME)
    upload_all_files(bucket)
    print("\nSubida a GCS completada.")


if __name__ == "__main__":
    main()
