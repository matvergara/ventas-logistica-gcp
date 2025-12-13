# Análisis de Ventas y Logística en Google Cloud Platform

Este proyecto implementa una solución analítica end-to-end en Google Cloud Platform para el análisis de ventas y logística, abarcando desde la generación de datos hasta la construcción de un Data Warehouse modelado en esquema estrella.

El objetivo es simular un escenario realista de operación de distribuidores, integrando datos de ventas, stock y clientes, y transformarlos en un repositorio analítico preparado para consumo en herramientas de Business Intelligence.

---

## 📚 Contexto

El proyecto se basa en un escenario de distribución comercial con múltiples distribuidores, sucursales y clientes, donde se busca responder preguntas operativas y comerciales como:

- Evolución de ventas en el tiempo
- Desempeño por región y sucursal
- Análisis de stock y reposición
- Segmentación de clientes
- Comparación entre distribuidores

> Dado que no se dispone de datos reales, se implementó un generador de datos sintéticos en Python que respeta reglas de negocio realistas (condiciones de venta, comportamiento de clientes, reposición de stock, etc.).

## Arquitectura de la solución
La solución sigue una arquitectura analítica por capas:
1. Generación de datos (Python) → 
2. Google Cloud Storage (Data Lake) →
3. BigQuery capa raw →
4. Data Warehouse →
5. Datamarts →
6. Dashboard en Looker Studio

## 📁 Estructura del repositorio
```
├── src/
│   ├── generate_data/        # Generación de datos sintéticos
│   ├── upload_to_gcs/        # Carga de archivos a Cloud Storage
│   ├── load_raw_to_bq/       # Ingesta RAW en BigQuery con control de idempotencia
│   ├── dwh/                  # Orquestación del Data Warehouse
│   └── common/               # Utilidades comunes (auth, clientes GCP)
│
├── sql/
│   └── dwh/                  # SQL del Data Warehouse (dimensiones y hechos)
│
├── requirements.txt
└── README.md
```

## 🧮 Data Warehouse

El Data Warehouse está modelado bajo un esquema estrella, separando dimensiones y tablas de hechos.
Dimensiones:
- `dim_fecha`
- `dim_cliente`
- `dim_producto`
- `dim_sucursal`

Hechos:
- `fact_ventas`
- `fact_stock` 

La carga es incremental e idempotente, permitiendo la reejecución del pipeline sin duplicación de datos.

### Ejecución del pipeline
1. Generación de datos sintéticos.
2. Carga de archivos en Cloud Storage.
3. Ingesta incremental en BigQuery raw.
4. Ejecución del Data Warehouse.
```bash
python -m src.dwh.run_dwh
```

## 🛠️ Tecnologías utilizadas
- Python
- Google Cloud Storage
- Google BigQuery
- SQL
- Looker Studio

## 🚧 Estado actual y próximos pasos

Actualmente el proyecto cuenta con un pipeline completo hasta la capa de Data Warehouse.

Las próximas etapas incluyen:
- Construcción de datamarts orientados a consumo analítico
- Reconexión y diseño de dashboards en Looker Studio

## 🧑‍💻 Autores | Contacto
- **Emanuel Pinasco** • [LinkedIn](https://www.linkedin.com/in/bruno-inguanzo-974021212/)
- **Matías Vergara** • [LinkedIn](https://www.linkedin.com/in/matiasvergaravicencio/)