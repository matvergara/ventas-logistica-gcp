# Análisis de Ventas y Logística en Google Cloud Platform

Este proyecto implementa una solución analítica *end-to-end* en ***Google Cloud Platform*** para el análisis de ventas y logística, abarcando desde la generación de datos hasta la construcción de un ***Data Warehouse*** modelado en esquema estrella.

El objetivo es simular un escenario realista de operación de distribuidores, integrando datos de ventas, stock y clientes, y transformarlos en un repositorio analítico preparado para consumo en herramientas de ***Business Intelligence***.

---

## Contexto

El proyecto se basa en un escenario de distribución comercial con múltiples distribuidores, sucursales y clientes, donde se busca responder preguntas operativas y comerciales como:

- Evolución de stock en el tiempo
- Desempeño en ventas por región y producto
- Análisis de stock y reposición
- Comparación entre distribuidores

> Dado que no se dispone de datos reales, se implementó un generador de datos sintéticos en Python que respeta reglas de negocio realistas (condiciones de venta, comportamiento de clientes, reposición de stock, etc.).

---

## Arquitectura

```
Generador de datos (Python)
  → data/  (CSVs locales)
  → Google Cloud Storage  (Data Lake)
  → BigQuery: raw  (copia fiel de los archivos)
  → BigQuery: dwh  (star schema: 4 dims + 2 facts)
  → BigQuery: datamarts  (vistas para BI)
  → Looker Studio  (dashboards)
```

### Capas de BigQuery

| Dataset | Contenido | Estrategia de carga |
|---------|-----------|---------------------|
| `raw` | Tablas `ventas`, `stock`, `maestro` | Append incremental con control de idempotencia |
| `dwh` | 4 dimensiones + 2 facts | Dims: reemplazo total; facts: INSERT anti-duplicado / MERGE |
| `datamarts` | Vistas `dm_ventas`, `dm_stock` | Vistas (sin almacenamiento) |
| `infra` | Tabla de control de cargas | Tracking por (bucket, path, generation) |

### Star Schema

**Dimensiones:** `dim_fecha` · `dim_cliente` · `dim_producto` · `dim_sucursal`

**Hechos:**
- `fact_ventas` — grano: producto × cliente × sucursal × fecha; métricas: unidades e importe
- `fact_stock` — grano: producto × sucursal × fecha; métrica: stock diario

---

## Estructura del repositorio

```
├── run_pipeline.py           # Orquestador único del pipeline
├── requirements.txt          # Dependencias de producción
├── requirements-dev.txt      # Dependencias de desarrollo (pytest)
│
├── src/
│   ├── config.py             # Configuración centralizada (bucket, datasets, rutas)
│   ├── common/
│   │   ├── gcp_auth.py       # Clientes autenticados de BQ y GCS
│   │   └── logger.py         # Logging centralizado
│   ├── generate_data/        # Generador de datos sintéticos
│   ├── upload_to_gcs/        # Subida de archivos a Cloud Storage
│   ├── load_raw_to_bq/       # Ingesta RAW en BigQuery con control de idempotencia
│   ├── dwh/                  # Orquestación del Data Warehouse
│   └── datamarts/            # Orquestación de Datamarts
│
├── sql/
│   ├── dwh/                  # SQL del star schema (dims y facts)
│   └── datamarts/            # SQL de datamarts (vistas)
│
├── terraform/                # Infraestructura como código (GCS + BigQuery)
│   ├── main.tf
│   ├── variables.tf
│   ├── storage.tf
│   ├── bigquery.tf
│   └── outputs.tf
│
└── tests/                    # Tests unitarios (sin dependencia de GCP)
    ├── test_generate_data.py
    └── test_load_raw.py
```

---

## Replicar el proyecto

### Prerequisitos

| Herramienta | Versión mínima | Instalación |
|-------------|----------------|-------------|
| Python | 3.10+ | [python.org](https://www.python.org/downloads/) |
| gcloud CLI | cualquiera | [cloud.google.com/sdk](https://cloud.google.com/sdk/docs/install) |
| Terraform | 1.5+ | [developer.hashicorp.com/terraform](https://developer.hashicorp.com/terraform/install) |

También necesitás un **proyecto de GCP** con billing habilitado. El costo es prácticamente cero para el volumen de datos de este proyecto (ambos servicios tienen free tier).

---

### 1. Clonar el repositorio

```bash
git clone https://github.com/TU_USUARIO/ventas-logistica-gcp.git
cd ventas-logistica-gcp
```

### 2. Crear entorno virtual e instalar dependencias Python

```bash
python -m venv .venv

# Linux / macOS
source .venv/bin/activate

# Windows
.venv\Scripts\activate

pip install -r requirements.txt
```

### 3. Autenticarse en GCP

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project TU_PROJECT_ID
```

### 4. Habilitar las APIs necesarias

```bash
gcloud services enable bigquery.googleapis.com storage.googleapis.com
```

### 5. Crear la infraestructura con Terraform

```bash
cd terraform/
terraform init
terraform apply -var="project_id=TU_PROJECT_ID"
cd ..
```

Esto crea el bucket de GCS y los cuatro datasets de BigQuery.

### 6. Ejecutar el pipeline

```bash
python run_pipeline.py
```

El orquestador ejecuta los 7 pasos en orden: generación de datos → subida a GCS → setup BigQuery → carga RAW → DWH → datamarts.

---

## Ejecución del pipeline

El pipeline completo se controla desde un único punto de entrada:

```bash
# Pipeline completo
python run_pipeline.py

# Desde un paso en adelante (útil para reejecutar parcialmente)
python run_pipeline.py --from dwh
python run_pipeline.py --from load_raw

# Un solo paso
python run_pipeline.py --only generate
```

Pasos disponibles: `generate` · `upload` · `setup_datasets` · `setup_infra` · `load_raw` · `dwh` · `datamarts`

La carga de datos es **incremental e idempotente**: el pipeline puede reejecutarse sin duplicar datos gracias a la tabla `infra.control_archivos_cargados` que registra cada archivo procesado.

---

## Tests

Los tests unitarios cubren la lógica de negocio pura y no requieren conexión a GCP:

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

---

## Dashboard

Los datamarts se conectan a Looker Studio para la visualización de indicadores clave con foco en análisis de ventas, distribución geográfica, desempeño por producto y control de stock.

[Ver dashboard en Looker Studio](https://lookerstudio.google.com/reporting/15bc3841-a472-4f1c-9b98-7ece170edba9/page/N89dF)

---

## Tecnologías

Python · Google Cloud Storage · Google BigQuery · Terraform · SQL · Looker Studio

---

## Autores

- **Emanuel Pinasco** · [LinkedIn](https://www.linkedin.com/in/bruno-inguanzo-974021212/)
- **Matías Vergara** · [LinkedIn](https://www.linkedin.com/in/matiasvergaravicencio/)
