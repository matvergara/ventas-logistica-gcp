"""
Carga incremental de datos RAW desde GCS a BigQuery.

- Idempotencia por archivo
- Control por tabla infra.control_archivos_cargados
"""

from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple

from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound

from src.common.gcp_auth import get_bq_client, get_gcs_client


# ======================
# CONFIGURACIÓN
# ======================

RAW_DATASET = "raw"
INFRA_DATASET = "infra"
CONTROL_TABLE = "control_archivos_cargados"

BUCKET_NAME = "ventas-logistica-raw"
GCS_BASE_PATH = "data"

TABLAS = ["ventas", "stock", "maestro"]


# ======================
# ESQUEMAS
# ======================

SCHEMAS = {
    "ventas": [
        bigquery.SchemaField("sucursal", "INTEGER"),
        bigquery.SchemaField("cliente", "INTEGER"),
        bigquery.SchemaField("fecha_cierre", "DATE"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("venta_unidades", "INTEGER"),
        bigquery.SchemaField("venta_importe", "FLOAT"),
        bigquery.SchemaField("condicion_venta", "STRING"),
        bigquery.SchemaField("distribuidor", "INTEGER"),
    ],
    "stock": [
        bigquery.SchemaField("sucursal", "INTEGER"),
        bigquery.SchemaField("fecha_cierre", "DATE"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("producto", "STRING"),
        bigquery.SchemaField("stock", "INTEGER"),
        bigquery.SchemaField("unidad", "STRING"),
        bigquery.SchemaField("distribuidor", "INTEGER"),
    ],
    "maestro": [
        bigquery.SchemaField("sucursal", "INTEGER"),
        bigquery.SchemaField("cliente", "INTEGER"),
        bigquery.SchemaField("ciudad", "STRING"),
        bigquery.SchemaField("provincia", "STRING"),
        bigquery.SchemaField("estado", "STRING"),
        bigquery.SchemaField("nombre_cliente", "STRING"),
        bigquery.SchemaField("cuit", "STRING"),
        bigquery.SchemaField("razon_social", "STRING"),
        bigquery.SchemaField("direccion", "STRING"),
        bigquery.SchemaField("dia_visita", "STRING"),
        bigquery.SchemaField("telefono", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("fecha_alta", "DATE"),
        bigquery.SchemaField("fecha_baja", "DATE"),
        bigquery.SchemaField("coordenada_latitud", "FLOAT"),
        bigquery.SchemaField("coordenada_longitud", "FLOAT"),
        bigquery.SchemaField("condicion_venta", "STRING"),
        bigquery.SchemaField("deuda_vencida", "FLOAT"),
        bigquery.SchemaField("tipo_negocio", "STRING"),
        bigquery.SchemaField("distribuidor", "INTEGER"),
    ],
}


# ======================
# FUNCIONES
# ======================

def obtener_distribuidores(storage_client: storage.Client) -> list[int]:
    """
    Detecta distribuidores existentes en GCS bajo data/distribuidor_X/
    """
    bucket = storage_client.bucket(BUCKET_NAME)
    prefix = f"{GCS_BASE_PATH}/"

    distribuidores = set()

    for blob in bucket.list_blobs(prefix=prefix):
        partes = blob.name.split("/")
        if len(partes) >= 2 and partes[1].startswith("distribuidor_"):
            try:
                dist = int(partes[1].replace("distribuidor_", ""))
                distribuidores.add(dist)
            except ValueError:
                continue

    return sorted(distribuidores)

def listar_blobs(
    storage_client: storage.Client,
    distribuidor: int,
    tabla: str,
) -> List[Dict]:
    bucket = storage_client.bucket(BUCKET_NAME)
    prefix = f"{GCS_BASE_PATH}/distribuidor_{distribuidor}/{tabla}/"

    archivos = []
    for blob in bucket.list_blobs(prefix=prefix):
        if not blob.name.lower().endswith(".csv"):
            continue

        archivos.append({
            "bucket": BUCKET_NAME,
            "object_path": blob.name,
            "generation": int(blob.generation),
            "crc32c": blob.crc32c,
            "tabla": tabla,
            "distribuidor": distribuidor,
            "fecha_actualizacion": blob.updated,
        })

    return archivos


def obtener_ya_cargados(
    bq_client: bigquery.Client,
    tabla: str,
    distribuidor: int,
) -> Set[Tuple]:
    query = f"""
    SELECT bucket, object_path, generation, fecha_actualizacion
    FROM `{bq_client.project}.{INFRA_DATASET}.{CONTROL_TABLE}`
    WHERE tabla = @tabla AND distribuidor = @dist
    """

    job = bq_client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("tabla", "STRING", tabla),
                bigquery.ScalarQueryParameter("dist", "INT64", distribuidor),
            ]
        ),
    )

    return {
        (r.bucket, r.object_path, r.generation, r.fecha_actualizacion)
        for r in job.result()
    }


def filtrar_pendientes(
    archivos: List[Dict],
    ya_cargados: Set[Tuple],
) -> List[Dict]:
    pendientes = []
    for a in archivos:
        clave = (a["bucket"], a["object_path"], a["generation"], a["fecha_actualizacion"])
        if clave not in ya_cargados:
            pendientes.append(a)
    return pendientes


def cargar_archivo(
    bq_client: bigquery.Client,
    gcs_uri: str,
    tabla: str,
) -> None:
    table_id = f"{bq_client.project}.{RAW_DATASET}.{tabla}"

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMAS[tabla],
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
    )

    job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    job.result()


def registrar_control(
    bq_client: bigquery.Client,
    registros: List[Dict],
) -> None:
    if not registros:
        return

    table_id = f"{bq_client.project}.{INFRA_DATASET}.{CONTROL_TABLE}"
    now = datetime.now(timezone.utc).isoformat()

    for r in registros:
        r["loaded_at"] = now
        if isinstance(r.get("fecha_actualizacion"), datetime):
            r["fecha_actualizacion"] = r["fecha_actualizacion"].isoformat()

    job = bq_client.load_table_from_json(
        registros,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()


# ======================
# MAIN
# ======================

def main() -> None:
    bq_client = get_bq_client()
    storage_client = get_gcs_client()

    print("Carga RAW incremental")
    print(f"Proyecto: {bq_client.project}")
    print("-" * 60)

    distribuidores = obtener_distribuidores(storage_client)

    for distribuidor in distribuidores:
        for tabla in TABLAS:
            print(f"\n➡ Distribuidor {distribuidor} | Tabla {tabla}")

            archivos = listar_blobs(storage_client, distribuidor, tabla)
            ya_cargados = obtener_ya_cargados(bq_client, tabla, distribuidor)
            pendientes = filtrar_pendientes(archivos, ya_cargados)

            print(f"   Archivos en GCS: {len(archivos)}")
            print(f"   Ya cargados: {len(ya_cargados)}")
            print(f"   Pendientes: {len(pendientes)}")

            registros_control = []

            for a in pendientes:
                uri = f"gs://{a['bucket']}/{a['object_path']}"
                try:
                    cargar_archivo(bq_client, uri, tabla)
                    registros_control.append({
                        "bucket": a["bucket"],
                        "object_path": a["object_path"],
                        "generation": a["generation"],
                        "crc32c": a["crc32c"],
                        "tabla": tabla,
                        "distribuidor": a["distribuidor"],
                        "fecha_actualizacion": a["fecha_actualizacion"],
                    })
                except Exception as e:
                    print(f"Error cargando {uri}: {e}")

            registrar_control(bq_client, registros_control)

    print("\nCarga RAW incremental finalizada.")


if __name__ == "__main__":
    main()
