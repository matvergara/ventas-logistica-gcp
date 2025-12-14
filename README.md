# Análisis de Ventas y Logística en Google Cloud Platform

Este proyecto implementa una solución analítica *end-to-end* en ***Google Cloud Platform*** para el análisis de ventas y logística, abarcando desde la generación de datos hasta la construcción de un ***Data Warehouse*** modelado en esquema estrella.

El objetivo es simular un escenario realista de operación de distribuidores, integrando datos de ventas, stock y clientes, y transformarlos en un repositorio analítico preparado para consumo en herramientas de ***Business Intelligence***.

---

## 📚 Contexto

El proyecto se basa en un escenario de distribución comercial con múltiples distribuidores, sucursales y clientes, donde se busca responder preguntas operativas y comerciales como:

- Evolución de stock en el tiempo
- Desempeño en ventas por región y producto
- Análisis de stock y reposición
- Comparación entre distribuidores

> Dado que no se dispone de datos reales, se implementó un generador de datos sintéticos en Python que respeta reglas de negocio realistas (condiciones de venta, comportamiento de clientes, reposición de stock, etc.).

## 🧱 Arquitectura de la solución
La solución sigue una arquitectura analítica por capas:
1. Generación de datos (Python) → 
2. Google Cloud Storage (Data Lake) →
3. BigQuery capa raw →
4. Data Warehouse →
5. Datamarts →
6. Dashboard en Looker Studio

## 🧠 Decisiones de diseño

- Separación por capas para desacoplar ingesta, modelado y consumo
- Uso de esquema estrella para facilitar análisis
- Datamarts para simplificar la lógica en BI
- SQL versionado y orquestación desde Python


## 📁 Estructura del repositorio
```
├── src/
│   ├── generate_data/        # Generación de datos sintéticos
│   ├── upload_to_gcs/        # Carga de archivos a Cloud Storage
│   ├── load_raw_to_bq/       # Ingesta RAW en BigQuery con control de idempotencia
│   ├── dwh/                  # Orquestación del Data Warehouse
│   ├── datamarts/            # Orquestación de Datamarts
│   └── common/               # Utilidades comunes (auth, clientes GCP)
│
├── sql/
│   ├── dwh/                  # SQL del Data Warehouse (dimensiones y hechos)
│   └── datamarts/            # SQL de Datamarts
│
├── requirements.txt
└── README.md
```

## 🧮 Data Warehouse

El ***Data Warehouse*** está modelado bajo un esquema estrella, separando dimensiones y tablas de hechos.
Dimensiones:
- `dim_fecha`
- `dim_cliente`
- `dim_producto`
- `dim_sucursal`

Hechos:
- `fact_ventas`
- `fact_stock` 

La carga es incremental e idempotente, permitiendo la reejecución del pipeline sin duplicación de datos. 
Se implementa una tabla de control para evitar reprocesos y duplicación de archivos provenientes de ***Cloud Storage***.

## 📊 Datamarts

Para facilitar el consumo analítico y simplificar la lógica en herramientas de BI, se construyó una capa de ***datamarts*** sobre el *Data Warehouse*.

Los *datamarts* se implementan como vistas en ***BigQuery*** y están orientados al consumo analítico.

### Datamart de Ventas (`dm_ventas`)
Incluye métricas y dimensiones necesarias para el análisis comercial:
- Unidades vendidas
- Importe vendido
- Producto
- Provincia
- Tipo de negocio
- Sucursal
- Dimensiones temporales (año, mes, semana ISO)

### Datamart de Stock (`dm_stock`)
Orientado al análisis operativo y logístico:
- Stock diario por producto y distribuidor
- Métricas agregables (promedio, mínimo, desvío estándar)
- Dimensiones temporales (año, mes, semana ISO)

## 📈 Business Intelligence

Los *datamarts* se conectan a ***Looker Studio*** para la visualización de indicadores clave.

Se desarrollaron *dashboards* con foco en:
- Análisis de ventas
- Distribución geográfica
- Desempeño por producto y tipo de negocio
- Control de stock y variabilidad

La capa de BI se apoya exclusivamente en los *datamarts*, evitando *joins* y lógica compleja en la herramienta de visualización.

[Clickea aquí para visualizar el Dashboard](https://lookerstudio.google.com/reporting/15bc3841-a472-4f1c-9b98-7ece170edba9/page/N89dF)

## ➡️ Ejecución del pipeline
1. Generación de datos sintéticos.
```bash
python -m src.generate_data.generate_data
```
2. Carga de archivos en Cloud Storage.
```bash
python -m src.upload_to_gcs.upload_to_gcs
```
3. Ingesta incremental en BigQuery raw.
    a. Creacion de capas raw, dwh y datamarts si no existen:
    ```bash
    python -m src.load_raw_to_bq.setup_datasets
    ```
    b. Creacion de tabla de control
    ```bash
    python -m src.load_raw_to_bq.setup_infra_control
    ```
    c. Ingesta de datos desde Cloud Storage a raw
    ```bash
    python -m src.load_raw_to_bq.load_raw
    ```
4. Ejecución del Data Warehouse.
```bash
python -m src.dwh.run_dwh
```
5. Ejecución de Datamarts
```bash
python -m src.datamarts.run_datamarts
```

## 🛠️ Tecnologías utilizadas
- Python
- Google Cloud Storage
- Google BigQuery
- SQL
- Looker Studio

## 🧑‍💻 Autores | Contacto
- **Emanuel Pinasco** • [LinkedIn](https://www.linkedin.com/in/bruno-inguanzo-974021212/)
- **Matías Vergara** • [LinkedIn](https://www.linkedin.com/in/matiasvergaravicencio/)