"""
Generador de datos sintéticos para un sistema de ventas y logística.

Salida (estructura tipo Data Lake local):

data/
├── Archivos_VentaClientes/
│   └── Distribuidor_1/
│       ├── Venta_Clientes_YYYY-MM-DD.csv
│       └── ...
├── Archivos_Stock/
│   └── Distribuidor_1/
│       ├── StockPeriodo_YYYY-MM-DD.csv
│       └── ...
└── Archivos_Maestro/
    └── Distribuidor_1/
        └── Maestro_YYYY-MM-DD.csv
"""

from __future__ import annotations

import json
import os
import random as rd
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ====================
# MASTER DATA
# ====================

CONDICIONES_VENTA = {
    "CONT": {"nombre": "Contado", "dias_pago": 0, "descuento": 0.05},
    "CTA7": {"nombre": "Cuenta Corriente 7 días", "dias_pago": 7, "descuento": 0.03},
    "CTA15": {"nombre": "Cuenta Corriente 15 días", "dias_pago": 15, "descuento": 0.02},
    "CTA30": {"nombre": "Cuenta Corriente 30 días", "dias_pago": 30, "descuento": 0.00},
    "CTA60": {"nombre": "Cuenta Corriente 60 días", "dias_pago": 60, "descuento": -0.02},
}

TIPOS_NEGOCIO = [
    "Supermercado",
    "Almacén",
    "Kiosco",
    "Mayorista",
    "Minorista",
    "Hipermercado",
    "Despensa",
    "Autoservicio",
    "Farmacia",
    "Perfumería",
]

DIAS_VISITA = {
    "LUN": "Lunes",
    "MAR": "Martes",
    "MIE": "Miércoles",
    "JUE": "Jueves",
    "VIE": "Viernes",
    "SAB": "Sábado",
    "L-V": "Lunes a Viernes",
    "L-S": "Lunes a Sábado",
}

PRODUCTOS = {
    "PROD001": {"nombre": "Gaseosa Cola 2.25L", "categoria": "Bebidas", "precio_base": 850, "unidad_default": "UNI"},
    "PROD002": {"nombre": "Gaseosa Lima-Limón 2.25L", "categoria": "Bebidas", "precio_base": 800, "unidad_default": "UNI"},
    "PROD003": {"nombre": "Gaseosa Naranja 2.25L", "categoria": "Bebidas", "precio_base": 820, "unidad_default": "UNI"},
    "PROD004": {"nombre": "Gaseosa Pomelo 2.25L", "categoria": "Bebidas", "precio_base": 810, "unidad_default": "UNI"},
    "PROD005": {"nombre": "Agua Mineral 2L", "categoria": "Bebidas", "precio_base": 400, "unidad_default": "UNI"},
    "PROD006": {"nombre": "Cerveza Rubia 1L", "categoria": "Bebidas", "precio_base": 1200, "unidad_default": "UNI"},
    "PROD007": {"nombre": "Arroz Largo Fino 1Kg", "categoria": "Alimentos", "precio_base": 600, "unidad_default": "KG"},
    "PROD008": {"nombre": "Fideos Secos 500g", "categoria": "Alimentos", "precio_base": 450, "unidad_default": "UNI"},
    "PROD009": {"nombre": "Aceite Girasol 1.5L", "categoria": "Alimentos", "precio_base": 1500, "unidad_default": "UNI"},
    "PROD010": {"nombre": "Harina de Trigo 1Kg", "categoria": "Alimentos", "precio_base": 400, "unidad_default": "KG"},
}

LOCALIDADES = {
    "Buenos Aires": ["CABA", "La Plata", "Mar del Plata", "Bahía Blanca", "Tandil", "Pergamino", "San Nicolás"],
    "Córdoba": ["Córdoba Capital", "Villa Carlos Paz", "Río Cuarto", "Villa María", "San Francisco"],
    "Santa Fe": ["Rosario", "Santa Fe Capital", "Rafaela", "Venado Tuerto", "Reconquista"],
    "Mendoza": ["Mendoza Capital", "San Rafael", "Godoy Cruz", "Luján de Cuyo", "Maipú"],
    "Tucumán": ["San Miguel de Tucumán", "Yerba Buena", "Tafí Viejo", "Concepción", "Banda del Río Salí"],
}

ESTADOS_CLIENTE = {
    "ACTIVO": 0.75,
    "INACTIVO": 0.15,
    "SUSPENDIDO": 0.08,
    "BAJA": 0.02,
}


# ====================
# MODELOS
# ====================

@dataclass
class Coordenadas:
    lat: float
    lon: float


class Cliente:
    """Representa un cliente del distribuidor con su ciclo de vida completo."""

    def __init__(self, id_cliente: int, sucursal: int, nombre: str, provincia: str, ciudad: str):
        self.id_cliente = id_cliente
        self.sucursal = sucursal
        self.nombre = nombre

        self.provincia = provincia
        self.ciudad = ciudad
        self.coordenadas = self._generar_coordenadas(provincia)

        self.cuit = f"20-{rd.randint(10000000, 99999999)}-{rd.randint(0, 9)}"
        self.razon_social = f"RS {nombre} S.A."
        self.direccion = self._generar_direccion()
        self.telefono = f"11-{rd.randint(1000, 9999)}-{rd.randint(1000, 9999)}"
        self.email = f"contacto@{nombre.lower().replace(' ', '')}.com.ar"

        self.tipo_negocio = rd.choice(TIPOS_NEGOCIO)
        self.condicion_venta = rd.choice(list(CONDICIONES_VENTA.keys()))
        self.dia_visita = rd.choice(list(DIAS_VISITA.keys()))

        self.fecha_alta = self._generar_fecha_alta()
        self.estado = rd.choices(list(ESTADOS_CLIENTE.keys()), weights=list(ESTADOS_CLIENTE.values()))[0]
        self.fecha_baja = self._calcular_fecha_baja()

        self.deuda_vencida = self._calcular_deuda_inicial()
        self.ultima_compra: Optional[datetime] = None

    def _generar_direccion(self) -> str:
        calles = ["San Martín", "Belgrano", "Rivadavia", "Mitre", "Sarmiento", "Moreno", "Alsina"]
        return f"{rd.choice(calles)} {rd.randint(100, 5000)}"

    def _generar_coordenadas(self, provincia: str) -> Coordenadas:
        coords_base = {
            "Buenos Aires": (-35.0, -60.0),
            "Córdoba": (-31.4, -64.2),
            "Santa Fe": (-31.6, -60.7),
            "Mendoza": (-32.9, -68.8),
            "Tucumán": (-26.8, -65.2),
        }
        lat_base, lon_base = coords_base.get(provincia, (-34.0, -64.0))
        lat = lat_base + rd.uniform(-2, 2)
        lon = lon_base + rd.uniform(-2, 2)
        return Coordenadas(lat=round(lat, 6), lon=round(lon, 6))

    def _generar_fecha_alta(self) -> datetime:
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2023, 12, 31)
        random_days = rd.randint(0, (end_date - start_date).days)
        return start_date + timedelta(days=random_days)

    def _calcular_fecha_baja(self) -> Optional[datetime]:
        if self.estado in ["BAJA", "INACTIVO"]:
            dias_activo = rd.randint(30, 365)
            return self.fecha_alta + timedelta(days=dias_activo)
        return None

    def _calcular_deuda_inicial(self) -> float:
        if self.estado == "ACTIVO":
            return round(rd.uniform(0, 5000), 2) if rd.random() > 0.7 else 0.0
        if self.estado == "SUSPENDIDO":
            return round(rd.uniform(5000, 50000), 2)
        if self.estado == "INACTIVO":
            return round(rd.uniform(1000, 10000), 2) if rd.random() > 0.5 else 0.0
        return 0.0

    def actualizar_ultima_compra(self, fecha: datetime) -> None:
        self.ultima_compra = fecha
        if self.estado == "INACTIVO" and rd.random() > 0.3:
            self.estado = "ACTIVO"
            self.fecha_baja = None


class GeneradorDatos:
    """Generador principal de datos realistas para el sistema de distribución."""

    def __init__(self, cant_distribuidores: int = 3, cant_dias: int = 30, clientes_por_dist: int = 50, seed: Optional[int] = 42):
        self.cant_distribuidores = cant_distribuidores
        self.cant_dias = cant_dias
        self.clientes_por_dist = clientes_por_dist
        self.fecha_actual = datetime.now()
        self.clientes: Dict[int, List[Cliente]] = {}
        self.stock_por_producto: Dict[int, Dict[str, Dict[str, object]]] = {}

        if seed is not None:
            rd.seed(seed)

    def generar_clientes(self) -> None:
        """Genera el master de clientes por distribuidor."""
        for dist in range(1, self.cant_distribuidores + 1):
            self.clientes[dist] = []
            sucursal_base = dist * 100

            for i in range(self.clientes_por_dist):
                provincia = rd.choice(list(LOCALIDADES.keys()))
                ciudad = rd.choice(LOCALIDADES[provincia])

                cliente = Cliente(
                    id_cliente=1000 + (dist * 1000) + i,
                    sucursal=sucursal_base + rd.randint(1, 10),
                    nombre=f"Cliente_{dist}_{i}",
                    provincia=provincia,
                    ciudad=ciudad,
                )
                self.clientes[dist].append(cliente)

    def generar_stock_inicial(self, distribuidor: int) -> Dict[str, Dict[str, object]]:
        """Genera stock inicial por producto y distribuidor."""
        stock_inicial: Dict[str, Dict[str, object]] = {}
        for sku, prod_info in PRODUCTOS.items():
            if prod_info["categoria"] == "Bebidas":
                stock_base = rd.randint(500, 2000)
            else:
                stock_base = rd.randint(200, 800)

            stock_inicial[sku] = {
                "cantidad": stock_base,
                "unidad": prod_info["unidad_default"],
                "ultima_reposicion": self.fecha_actual - timedelta(days=rd.randint(1, 7)),
            }
        return stock_inicial

    def generar_venta_realista(self, cliente: Cliente, producto_sku: str, stock_disponible: int, fecha: datetime) -> Optional[Dict[str, object]]:
        """Genera una venta realista basada en múltiples factores de negocio."""
        if cliente.estado in ["BAJA", "SUSPENDIDO"]:
            return None

        prob_compra = 0.7 if cliente.estado == "ACTIVO" else 0.3

        if cliente.ultima_compra:
            dias_sin_compra = (fecha - cliente.ultima_compra).days
            if dias_sin_compra > 30:
                prob_compra *= 0.5

        if rd.random() > prob_compra:
            return None

        if cliente.tipo_negocio in ["Hipermercado", "Supermercado", "Mayorista"]:
            cantidad_base = rd.randint(10, 50)
        elif cliente.tipo_negocio in ["Almacén", "Autoservicio"]:
            cantidad_base = rd.randint(5, 20)
        else:
            cantidad_base = rd.randint(1, 10)

        cantidad = min(cantidad_base, int(stock_disponible * 0.3))
        if cantidad <= 0:
            return None

        precio_base = PRODUCTOS[producto_sku]["precio_base"]
        descuento = CONDICIONES_VENTA[cliente.condicion_venta]["descuento"]
        precio_final = precio_base * (1 - descuento)
        importe = round(cantidad * precio_final, 2)

        return {"cantidad": cantidad, "importe": importe, "precio_unitario": round(precio_final, 2)}

    def generar_datos_por_dia(self, distribuidor: int, fecha: datetime) -> Tuple[List[Dict[str, object]], Dict[str, Dict[str, object]]]:
        """Genera datos de venta y stock para un día específico."""
        ventas: List[Dict[str, object]] = []
        stock_actual = self.stock_por_producto.get(distribuidor, self.generar_stock_inicial(distribuidor))

        # Actualizar estados de clientes
        for cliente in self.clientes[distribuidor]:
            if cliente.ultima_compra:
                dias_inactivo = (fecha - cliente.ultima_compra).days
                if dias_inactivo > 60 and cliente.estado == "ACTIVO":
                    cliente.estado = "INACTIVO"
                    cliente.fecha_baja = fecha
                elif dias_inactivo > 180 and cliente.estado == "INACTIVO":
                    cliente.estado = "BAJA"

        # Generar ventas
        for cliente in self.clientes[distribuidor]:
            productos_a_comprar = rd.sample(list(PRODUCTOS.keys()), k=rd.randint(1, min(5, len(PRODUCTOS))))

            for producto_sku in productos_a_comprar:
                venta = self.generar_venta_realista(cliente, producto_sku, int(stock_actual[producto_sku]["cantidad"]), fecha)
                if venta:
                    ventas.append(
                        {
                            "sucursal": cliente.sucursal,
                            "cliente": cliente.id_cliente,
                            "fecha_cierre": fecha.strftime("%Y-%m-%d"),
                            "sku": producto_sku,
                            "venta_unidades": venta["cantidad"],
                            "venta_importe": venta["importe"],
                            "condicion_venta": cliente.condicion_venta,
                            "distribuidor": distribuidor,
                        }
                    )

                    stock_actual[producto_sku]["cantidad"] = int(stock_actual[producto_sku]["cantidad"]) - int(venta["cantidad"])
                    cliente.actualizar_ultima_compra(fecha)

        # Reposición de stock bajo
        for sku, info in stock_actual.items():
            if int(info["cantidad"]) < 100:
                reposicion = rd.randint(200, 500)
                info["cantidad"] = int(info["cantidad"]) + reposicion
                info["ultima_reposicion"] = fecha

        self.stock_por_producto[distribuidor] = stock_actual
        return ventas, stock_actual

    def escribir_archivos_locales(self, output_base_path: Path) -> None:
        """
        Genera todos los archivos de datos con estructura por distribuidor:
        - ventas (diario)
        - stock (diario)
        - maestro (1 vez por distribuidor)
        """
        output_base_path.mkdir(parents=True, exist_ok=True)
        self.generar_clientes()

        for distribuidor in range(1, self.cant_distribuidores + 1):
            print(f"\nGenerando datos para Distribuidor {distribuidor}...")

            paths = {
                "ventas": output_base_path / "Archivos_VentaClientes" / f"Distribuidor_{distribuidor}",
                "stock": output_base_path / "Archivos_Stock" / f"Distribuidor_{distribuidor}",
                "maestro": output_base_path / "Archivos_Maestro" / f"Distribuidor_{distribuidor}",
            }

            for p in paths.values():
                p.mkdir(parents=True, exist_ok=True)

            # Generación día a día
            for dia in range(self.cant_dias):
                fecha = self.fecha_actual - timedelta(days=dia)
                fecha_str = fecha.strftime("%Y-%m-%d")

                ventas, stock_actual = self.generar_datos_por_dia(distribuidor, fecha)

                # Ventas (si hay)
                if ventas:
                    df_ventas = pd.DataFrame(ventas)
                    (paths["ventas"] / f"Venta_Clientes_{fecha_str}.csv").write_text(
                        df_ventas.to_csv(index=False, encoding="utf-8", lineterminator="\n"),
                        encoding="utf-8",
                    )

                # Stock (siempre)
                stock_data = []
                for sku, info in stock_actual.items():
                    stock_data.append(
                        {
                            "sucursal": distribuidor * 100 + 1,  # mantenemos tu convención
                            "fecha_cierre": fecha_str,
                            "sku": sku,
                            "producto": PRODUCTOS[sku]["nombre"],
                            "stock": int(info["cantidad"]),
                            "unidad": str(info["unidad"]),
                            "distribuidor": distribuidor,
                        }
                    )

                df_stock = pd.DataFrame(stock_data)
                (paths["stock"] / f"StockPeriodo_{fecha_str}.csv").write_text(
                    df_stock.to_csv(index=False, encoding="utf-8", lineterminator="\n"),
                    encoding="utf-8",
                )

                # Maestro (solo primer día)
                if dia == 0:
                    maestro_data = []
                    for cliente in self.clientes[distribuidor]:
                        maestro_data.append(
                            {
                                "sucursal": cliente.sucursal,
                                "cliente": cliente.id_cliente,
                                "ciudad": cliente.ciudad,
                                "provincia": cliente.provincia,
                                "estado": cliente.estado,
                                "nombre_cliente": cliente.nombre,
                                "cuit": cliente.cuit,
                                "razon_social": cliente.razon_social,
                                "direccion": cliente.direccion,
                                "dia_visita": cliente.dia_visita,
                                "telefono": cliente.telefono,
                                "email": cliente.email,
                                "fecha_alta": cliente.fecha_alta.strftime("%Y-%m-%d"),
                                "fecha_baja": cliente.fecha_baja.strftime("%Y-%m-%d") if cliente.fecha_baja else "",
                                "coordenada_latitud": cliente.coordenadas.lat,
                                "coordenada_longitud": cliente.coordenadas.lon,
                                "condicion_venta": cliente.condicion_venta,
                                "deuda_vencida": cliente.deuda_vencida,
                                "tipo_negocio": cliente.tipo_negocio,
                                "distribuidor": distribuidor,
                            }
                        )

                    df_maestro = pd.DataFrame(maestro_data)
                    (paths["maestro"] / f"Maestro_{fecha_str}.csv").write_text(
                        df_maestro.to_csv(index=False, encoding="utf-8", lineterminator="\n"),
                        encoding="utf-8",
                    )

        # Resumen
        self._generar_resumen(output_base_path)

    def _generar_resumen(self, output_base_path: Path) -> None:
        """Genera resumen estadístico de los datos generados."""
        resumen = {
            "fecha_generacion": datetime.now().isoformat(),
            "version": "portfolio",
            "distribuidores": self.cant_distribuidores,
            "dias_generados": self.cant_dias,
            "total_clientes": sum(len(clientes) for clientes in self.clientes.values()),
            "productos": len(PRODUCTOS),
            "estadisticas_clientes": {},
        }

        for dist, clientes in self.clientes.items():
            estados: Dict[str, int] = {}
            for c in clientes:
                estados[c.estado] = estados.get(c.estado, 0) + 1

            resumen["estadisticas_clientes"][f"distribuidor_{dist}"] = {"total": len(clientes), "por_estado": estados}

        resumen_path = output_base_path / "resumen_generacion.json"
        with open(resumen_path, "w", encoding="utf-8") as f:
            json.dump(resumen, f, indent=2, ensure_ascii=False)

        print("\nResumen de generación creado:", resumen_path)


# ====================
# MAIN
# ====================

def main() -> None:
    # Output local: handoff al pipeline (upload_to_gcs.py)
    output_path = Path("data")

    config = {
        "cant_distribuidores": 3, # Valores originales: 5
        "cant_dias": 7, # Valores originales: 93
        "clientes_por_dist": 5, # Valores originales: 50
        "seed": 42, # Valores originales: 42
    }

    print("Generador de Datos")
    print("Salida local en:", output_path.resolve())
    print("-" * 60)

    generador = GeneradorDatos(
        cant_distribuidores=config["cant_distribuidores"],
        cant_dias=config["cant_dias"],
        clientes_por_dist=config["clientes_por_dist"],
        seed=config["seed"],
    )

    generador.escribir_archivos_locales(output_path)

    print("\nGeneración finalizada.")
    print("Estructura creada bajo /data con ventas, stock y maestro por distribuidor.")


if __name__ == "__main__":
    main()
