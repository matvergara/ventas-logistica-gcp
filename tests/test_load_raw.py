"""Tests unitarios para la lógica de carga incremental (sin conexión a GCP)."""

from datetime import datetime, timezone

import pytest

from src.load_raw_to_bq.load_raw import filtrar_pendientes


def make_archivo(bucket="bucket", path="data/dist_1/ventas/f.csv", gen=1, fecha=None):
    return {
        "bucket": bucket,
        "object_path": path,
        "generation": gen,
        "crc32c": "abc123",
        "tabla": "ventas",
        "distribuidor": 1,
        "fecha_actualizacion": fecha or datetime(2024, 1, 1, tzinfo=timezone.utc),
    }


class TestFiltrarPendientes:
    def test_archivo_nuevo_es_pendiente(self):
        archivos = [make_archivo()]
        ya_cargados = set()
        pendientes = filtrar_pendientes(archivos, ya_cargados)
        assert len(pendientes) == 1

    def test_archivo_ya_cargado_no_es_pendiente(self):
        fecha = datetime(2024, 1, 1, tzinfo=timezone.utc)
        archivos = [make_archivo(fecha=fecha)]
        ya_cargados = {("bucket", "data/dist_1/ventas/f.csv", 1, fecha)}
        pendientes = filtrar_pendientes(archivos, ya_cargados)
        assert len(pendientes) == 0

    def test_nueva_generacion_del_mismo_archivo_es_pendiente(self):
        fecha = datetime(2024, 1, 1, tzinfo=timezone.utc)
        archivos = [make_archivo(gen=2, fecha=fecha)]  # generation=2
        ya_cargados = {("bucket", "data/dist_1/ventas/f.csv", 1, fecha)}  # generation=1
        pendientes = filtrar_pendientes(archivos, ya_cargados)
        assert len(pendientes) == 1

    def test_lista_vacia_no_tiene_pendientes(self):
        pendientes = filtrar_pendientes([], set())
        assert pendientes == []

    def test_multiples_archivos_filtra_correctamente(self):
        fecha = datetime(2024, 1, 1, tzinfo=timezone.utc)
        archivos = [
            make_archivo(path="data/d1/ventas/a.csv", gen=1, fecha=fecha),
            make_archivo(path="data/d1/ventas/b.csv", gen=1, fecha=fecha),
            make_archivo(path="data/d1/ventas/c.csv", gen=1, fecha=fecha),
        ]
        ya_cargados = {("bucket", "data/d1/ventas/a.csv", 1, fecha)}
        pendientes = filtrar_pendientes(archivos, ya_cargados)
        assert len(pendientes) == 2
        rutas = [p["object_path"] for p in pendientes]
        assert "data/d1/ventas/b.csv" in rutas
        assert "data/d1/ventas/c.csv" in rutas
