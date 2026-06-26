"""
Orquestador del pipeline completo de ventas-logística.

Ejecuta los pasos en orden:
  1. Generar datos sintéticos locales
  2. Subir datos a GCS
  3. Crear datasets en BigQuery
  4. Crear tabla de control de idempotencia
  5. Cargar datos RAW desde GCS a BigQuery
  6. Construir el Data Warehouse (star schema)
  7. Crear datamarts para Looker Studio

Uso:
  python run_pipeline.py              # pipeline completo
  python run_pipeline.py --from dwh   # desde el paso dwh en adelante
"""

import argparse
import sys
import time

from src.common.logger import get_logger

logger = get_logger("pipeline")

STEPS = [
    "generate",
    "upload",
    "setup_datasets",
    "setup_infra",
    "load_raw",
    "dwh",
    "datamarts",
]


def run_step(name: str) -> None:
    logger.info("=" * 60)
    logger.info("PASO: %s", name.upper())
    logger.info("=" * 60)
    t0 = time.time()

    if name == "generate":
        from src.generate_data.generate_data import main
    elif name == "upload":
        from src.upload_to_gcs.upload_to_gcs import main
    elif name == "setup_datasets":
        from src.load_raw_to_bq.setup_datasets import main
    elif name == "setup_infra":
        from src.load_raw_to_bq.setup_infra_control import main
    elif name == "load_raw":
        from src.load_raw_to_bq.load_raw import main
    elif name == "dwh":
        from src.dwh.run_dwh import main
    elif name == "datamarts":
        from src.datamarts.run_datamarts import main
    else:
        raise ValueError(f"Paso desconocido: {name}")

    main()
    logger.info("Paso '%s' completado en %.1fs", name, time.time() - t0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline ventas-logística GCP")
    parser.add_argument(
        "--from",
        dest="from_step",
        choices=STEPS,
        default=None,
        help="Ejecutar desde este paso en adelante (inclusive)",
    )
    parser.add_argument(
        "--only",
        dest="only_step",
        choices=STEPS,
        default=None,
        help="Ejecutar únicamente este paso",
    )
    args = parser.parse_args()

    if args.only_step:
        steps_to_run = [args.only_step]
    elif args.from_step:
        idx = STEPS.index(args.from_step)
        steps_to_run = STEPS[idx:]
    else:
        steps_to_run = STEPS

    logger.info("Pipeline ventas-logística GCP")
    logger.info("Pasos a ejecutar: %s", steps_to_run)
    t_total = time.time()

    for step in steps_to_run:
        try:
            run_step(step)
        except Exception as e:
            logger.error("Fallo en paso '%s': %s", step, e)
            sys.exit(1)

    logger.info("Pipeline completado en %.1fs", time.time() - t_total)


if __name__ == "__main__":
    main()
