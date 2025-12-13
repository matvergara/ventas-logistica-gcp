from google.cloud import storage, bigquery


def get_gcs_client() -> storage.Client:
    """
    Retorna un cliente autenticado de Google Cloud Storage
    usando Application Default Credentials.
    """
    return storage.Client()


def get_bq_client() -> bigquery.Client:
    """
    Retorna un cliente autenticado de BigQuery
    usando Application Default Credentials.
    """
    return bigquery.Client()
