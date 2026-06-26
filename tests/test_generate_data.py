"""Tests unitarios para el módulo de generación de datos."""

import pytest

from src.generate_data.generate_data import (
    ESTADOS_CLIENTE,
    PRODUCTOS,
    Cliente,
    GeneradorDatos,
)


class TestGeneradorDatos:
    def setup_method(self):
        self.gen = GeneradorDatos(cant_distribuidores=2, cant_dias=3, clientes_por_dist=4, seed=0)

    def test_genera_cantidad_correcta_de_clientes(self):
        self.gen.generar_clientes()
        assert len(self.gen.clientes) == 2
        for dist, clientes in self.gen.clientes.items():
            assert len(clientes) == 4, f"Distribuidor {dist} debería tener 4 clientes"

    def test_estados_de_clientes_son_validos(self):
        self.gen.generar_clientes()
        estados_validos = set(ESTADOS_CLIENTE.keys())
        for clientes in self.gen.clientes.values():
            for cliente in clientes:
                assert cliente.estado in estados_validos

    def test_stock_inicial_contiene_todos_los_skus(self):
        stock = self.gen.generar_stock_inicial(distribuidor=1)
        assert set(stock.keys()) == set(PRODUCTOS.keys())

    def test_stock_inicial_no_tiene_valores_negativos(self):
        stock = self.gen.generar_stock_inicial(distribuidor=1)
        for sku, info in stock.items():
            assert int(info["cantidad"]) >= 0, f"Stock negativo para {sku}"

    def test_genera_archivos_locales(self, tmp_path):
        self.gen.escribir_archivos_locales(tmp_path)

        # Al menos stock y maestro deben existir
        stock_dir = tmp_path / "Archivos_Stock"
        maestro_dir = tmp_path / "Archivos_Maestro"
        assert stock_dir.exists()
        assert maestro_dir.exists()

        # Resumen generado
        assert (tmp_path / "resumen_generacion.json").exists()

    def test_resumen_contiene_campos_esperados(self, tmp_path):
        import json
        self.gen.escribir_archivos_locales(tmp_path)
        resumen = json.loads((tmp_path / "resumen_generacion.json").read_text())

        assert resumen["distribuidores"] == 2
        assert resumen["dias_generados"] == 3
        assert resumen["total_clientes"] == 8
        assert resumen["productos"] == len(PRODUCTOS)


class TestCliente:
    def test_cuit_formato_valido(self):
        cliente = Cliente(id_cliente=1, sucursal=100, nombre="Test", provincia="Córdoba", ciudad="Córdoba Capital")
        partes = cliente.cuit.split("-")
        assert len(partes) == 3

    def test_estado_inicial_es_valido(self):
        cliente = Cliente(id_cliente=1, sucursal=100, nombre="Test", provincia="Córdoba", ciudad="Córdoba Capital")
        assert cliente.estado in ESTADOS_CLIENTE

    def test_coordenadas_dentro_de_rango(self):
        for provincia in ["Buenos Aires", "Córdoba", "Santa Fe", "Mendoza", "Tucumán"]:
            cliente = Cliente(id_cliente=1, sucursal=100, nombre="Test", provincia=provincia, ciudad="Ciudad")
            assert -90 <= cliente.coordenadas.lat <= 90
            assert -180 <= cliente.coordenadas.lon <= 180

    def test_cliente_baja_tiene_fecha_baja(self):
        # Con seed fija, forzamos estado BAJA
        import random
        random.seed(99)
        clientes_con_baja = []
        for i in range(50):
            c = Cliente(id_cliente=i, sucursal=100, nombre=f"Test{i}", provincia="Córdoba", ciudad="Córdoba Capital")
            if c.estado == "BAJA":
                clientes_con_baja.append(c)

        for c in clientes_con_baja:
            assert c.fecha_baja is not None
