"""
Tests de la metadata que recibe el LLM: que vea los problemas de los datos.

El fixture reproduce, en pequeño y con datos inventados, los patrones de una
base de ventas real: índice "Row", filas duplicadas, números escritos como
texto, negativos, extremos, fechas en varios formatos e imposibles, y datos
personales (nombres, emails, teléfonos).
"""

import json
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402
from src import quality_checker as qc  # noqa: E402

NOMBRES = ["José Muñoz", "Ana Pérez", "Luis Ortega", "María Paz", "Pedro Gil"]
FORMATOS = ["2025-01-{d:02d}", "{d:02d}/02/2025", "2025/03/{d:02d}", "{d:02d}-04-2025"]
N_BASE = 30
N_DUPLICADOS = 3


def _ventas() -> pd.DataFrame:
    filas = []
    for i in range(N_BASE):
        filas.append({
            "ID_Venta": f"V-{1000 + i}",
            "Fecha": FORMATOS[i % 4].format(d=i % 28 + 1),
            "Nombre_Cliente": NOMBRES[i % 5] + f" {chr(65 + i % 26)}",
            "Email": f"cliente{i}@tienda.com",
            "Telefono": 3001234500 + i,
            "Nombre_Producto": ["Gorra", "Camiseta", "Balón"][i % 3],
            "Edad": 20 + i,
            "Cantidad_Articulos": 1 + i % 5,
            "Precio_Unitario": round(10 + i * 1.5, 2),
            "Satisfaccion": 1 + i % 5,
            "Ciudad": ["Sevilla", "sevilla ", "MADRID"][i % 3],
            "Monto_Acumulado": 1_000_000 + i * 1234,
            "Total_Compra": round((1 + i % 5) * (10 + i * 1.5), 2),
        })
    df = pd.DataFrame(filas).astype({"Edad": object, "Cantidad_Articulos": object,
                                     "Precio_Unitario": object, "Satisfaccion": object})
    # Problemas sembrados
    df.loc[1, "Edad"] = "treinta"
    df.loc[2, "Edad"] = -7
    df.loc[3, "Edad"] = 150
    df.loc[4, "Cantidad_Articulos"] = "tres"
    df.loc[5, "Cantidad_Articulos"] = -2
    df.loc[6, "Precio_Unitario"] = "veinte"
    df.loc[7, "Precio_Unitario"] = -19.99
    df.loc[8, "Precio_Unitario"] = "19,99"          # coma decimal: es un número
    df.loc[9, "Satisfaccion"] = "excelente"
    df.loc[10, "Fecha"] = "2025-13-45"
    df.loc[11, "Fecha"] = "32/01/2025"
    df.loc[12, "Total_Compra"] = 26955.0
    # Duplicados exactos (salvo el índice) y columna índice consecutiva
    df = pd.concat([df, df.iloc[20:20 + N_DUPLICADOS]], ignore_index=True)
    df.insert(0, "Row", range(1, len(df) + 1))
    return df


@pytest.fixture
def ventas():
    return _ventas()


@pytest.fixture
def meta(ventas, monkeypatch):
    monkeypatch.delenv("DATAPULSE_ENMASCARAR", raising=False)
    return dp.serialize_metadata(ventas, "ventas.xlsx")


# ── 1. Columnas mayormente numéricas ────────────────────────────────────────

def test_columna_mayormente_numerica(meta):
    edad = meta["columnas_info"]["Edad"]
    assert edad["lectura"] == "mayormente_numerica"
    assert (edad["min"], edad["max"]) == (-7.0, 150.0)
    assert edad["negativos"] == 1 and edad["ceros"] == 0
    assert edad["no_numericos"] == {"conteo": 1, "ejemplos": ["treinta"]}
    assert edad["pct_numerico"] == pytest.approx(100 * 32 / 33, abs=0.01)
    assert edad["mean"] is not None and edad["skewness"] is not None

    precio = meta["columnas_info"]["Precio_Unitario"]
    assert precio["no_numericos"]["ejemplos"] == ["veinte"]  # "19,99" sí es número
    assert precio["min"] == -19.99 and precio["negativos"] == 1

    satisf = meta["columnas_info"]["Satisfaccion"]
    assert satisf["no_numericos"]["ejemplos"] == ["excelente"]


def test_negativos_y_ceros_en_numericas(meta):
    total = meta["columnas_info"]["Total_Compra"]
    assert total["lectura"] == "numerica"
    assert total["negativos"] == 0 and total["ceros"] == 0
    assert total["max"] == 26955.0


def test_maximo_cinco_ejemplos_no_numericos():
    serie = pd.Series([str(i) for i in range(60)] + [f"txt{i}" for i in range(8)],
                      name="Cantidad")
    meta = dp.serialize_metadata(serie.to_frame(), "x.csv")
    info = meta["columnas_info"]["Cantidad"]
    assert info["no_numericos"]["conteo"] == 8
    assert len(info["no_numericos"]["ejemplos"]) == 5


# ── 2. Muestra anómala ──────────────────────────────────────────────────────

def test_muestra_anomala(meta):
    cols = meta["columnas_info"]
    assert cols["Edad"]["muestra_anomala"][:2] == ["treinta", "150"]
    assert len(cols["Edad"]["muestra_anomala"]) <= 3
    assert "tres" in cols["Cantidad_Articulos"]["muestra_anomala"]
    assert cols["Total_Compra"]["muestra_anomala"][0] == "26955"
    assert set(cols["Fecha"]["muestra_anomala"]) == {"2025-13-45", "32/01/2025"}
    # La muestra normal sigue siendo las primeras filas
    assert cols["Edad"]["muestra"] == ["20", "treinta", "-7"]


def test_columna_de_fechas(meta):
    fecha = meta["columnas_info"]["Fecha"]
    assert fecha["lectura"] == "fecha"
    assert fecha["fechas_imposibles"] == 2
    assert set(fecha["formatos_fecha"]) == {"9999-99-99", "99/99/9999", "9999/99/99", "99-99-9999"}
    assert fecha["rango_fechas"] == {"min": "2025-01-01", "max": "2025-04-28"}


def test_texto_sin_muestra_anomala(meta):
    assert "muestra_anomala" not in meta["columnas_info"]["Ciudad"]


# ── 3. Columnas índice y duplicados ─────────────────────────────────────────

def test_indice_excluido_de_duplicados(ventas, meta):
    assert meta["columnas_indice"] == ["Row"]
    assert meta["columnas_info"]["Row"]["es_indice"] is True
    assert meta["duplicados_exactos"] == N_DUPLICADOS
    # El analizador de calidad usa el mismo criterio
    dup = qc.check_duplicates(ventas)
    assert dup["count"] == N_DUPLICADOS and dup["columnas_ignoradas"] == ["Row"]
    # Sin excluir el índice no se verían
    assert int(ventas.duplicated().sum()) == 0


@pytest.mark.parametrize("valores, es_indice", [
    ([1, 2, 3, 4], True),
    ([4, 2, 3, 1], True),          # desordenado pero consecutivo
    ([0, 1, 2], True),
    ([10, 20, 30], False),          # únicos pero no consecutivos
    ([1, 2, 2, 3], False),          # repetidos
    ([1.0, 2.0, 3.0], False),       # no enteros (dtype float)
    ([True, False], False),
])
def test_columnas_indice(valores, es_indice):
    df = pd.DataFrame({"c": valores})
    assert (qc.columnas_indice(df) == ["c"]) is es_indice


def test_indice_con_nulos_o_una_fila():
    assert qc.columnas_indice(pd.DataFrame({"c": pd.array([1, None, 3], dtype="Int64")})) == []
    assert qc.columnas_indice(pd.DataFrame({"c": [1]})) == []


def test_solo_columna_indice_no_rompe():
    assert qc.check_duplicates(pd.DataFrame({"Row": [1, 2, 3]}))["count"] == 0


# ── 4. Enmascarado por tipo de dato personal ────────────────────────────────

def test_enmascarado_por_tipo_de_dato(meta, ventas):
    cols = meta["columnas_info"]
    # Nombres de persona → forma
    assert cols["Nombre_Cliente"]["muestra"][0] == "Aaaa Aaaaa A"
    # Emails → u***@dominio
    assert cols["Email"]["muestra"][0] == "c***@tienda.com"
    # Teléfono numérico: identificador personal → dígitos enmascarados y sin
    # estadísticos (min/max/media mostrarían números reales)
    assert cols["Telefono"]["muestra"][0] == "9999999999"
    assert cols["Telefono"]["identificador_personal"] is True
    assert not {"min", "max", "mean", "muestra_anomala"} & set(cols["Telefono"])
    # Nombre de producto: no es una persona → intacto
    assert cols["Nombre_Producto"]["muestra"] == ["Gorra", "Camiseta", "Balón"]
    # Texto de alta cardinalidad sin datos personales → intacto (ya no por cardinalidad)
    assert cols["ID_Venta"]["muestra"] == ["V-1000", "V-1001", "V-1002"]
    # Números y fechas nunca se enmascaran (aunque tengan 6+ dígitos)
    assert cols["Monto_Acumulado"]["muestra"][0] == "1000000"
    assert cols["Precio_Unitario"]["muestra"] == ["10", "11.5", "13"]
    assert cols["Fecha"]["muestra"][0] == "2025-01-01"
    # Ningún dato personal real llega en el JSON
    texto = json.dumps(meta, ensure_ascii=False)
    for personal in ("José", "Muñoz", "cliente0@", "3001234500"):
        assert personal not in texto, personal


def test_enmascarado_desactivable(ventas, monkeypatch):
    monkeypatch.setenv("DATAPULSE_ENMASCARAR", "0")
    cols = dp.serialize_metadata(ventas, "v.xlsx")["columnas_info"]
    assert cols["Nombre_Cliente"]["muestra"][0] == "José Muñoz A"
    assert cols["Telefono"]["muestra"][0] == "3001234500"


@pytest.mark.parametrize("nombre, es_persona", [
    ("Nombre_Cliente", True), ("first_name", True), ("Apellidos", True),
    ("Nombre_Producto", False), ("ciudad_nombre", False), ("file_name", False),
    ("Cliente", False),
])
def test_deteccion_columnas_de_nombres(nombre, es_persona):
    assert dp._es_columna_nombre(nombre) is es_persona


# ── Ruta completa desde Excel y prompt ──────────────────────────────────────

def test_desde_excel(ventas, tmp_path, monkeypatch):
    """Mismo resultado leyendo el fixture desde .xlsx, como hace el pipeline."""
    monkeypatch.delenv("DATAPULSE_ENMASCARAR", raising=False)
    ruta = tmp_path / "ventas.xlsx"
    ventas.to_excel(ruta, index=False)
    meta = dp.node_ingestar({"filename_path": str(ruta), "file_ext": "xlsx",
                             "filename": "ventas.xlsx"})["metadata"]
    cols = meta["columnas_info"]
    assert meta["columnas_indice"] == ["Row"] and meta["duplicados_exactos"] == N_DUPLICADOS
    assert cols["Edad"]["lectura"] == "mayormente_numerica"
    assert cols["Edad"]["no_numericos"]["ejemplos"] == ["treinta"]
    assert cols["Fecha"]["fechas_imposibles"] == 2
    json.dumps(meta)  # serializable para el prompt


def test_prompt_explica_los_campos_y_el_enmascarado():
    prompt = dp.construir_prompt_diagnostico({})
    for campo in ("muestra_anomala", "no_numericos", "fechas_imposibles", "formatos_fecha",
                  "columnas_indice", "negativos", "u***@dominio", "Aaaa Aaaaa"):
        assert campo in prompt, campo


def test_skewness_de_columnas_mayormente_numericas(meta):
    """La skewness de la parte convertible llega al LLM como información."""
    assert abs(meta["columnas_info"]["Edad"]["skewness"]) > 1  # sesgada por el 150


# ── Utilidades ──────────────────────────────────────────────────────────────

def test_utilidades():
    assert qc.convertir_numerico(pd.Series(["1,5", " 2 ", "x", "1.234,5"])).tolist()[:2] == [1.5, 2.0]
    assert pd.isna(qc.convertir_numerico(pd.Series(["1.234,5"]))[0])
    assert qc.es_mayormente_numerica(pd.Series(["1", "2", "3", "4", "x"]))
    assert not qc.es_mayormente_numerica(pd.Series(["1", "2", "x", "y", "z"]))
    assert not qc.es_mayormente_numerica(pd.Series([1, 2, 3]))  # ya es numérica
    assert qc.extremos_iqr(pd.Series([1, 2, 3, 4, 5, 6, 100, -50])).tolist() == [100, -50]
    assert qc.extremos_iqr(pd.Series([1, 1, 1, 1])).empty
    fechas = qc.parsear_fechas_flexible(pd.Series(["2025-01-05", "05/02/2025", "2025/03/05"]))
    assert fechas.notna().all()
    # Orden día/mes según la evidencia de la columna
    dmy = qc.parsear_fechas_flexible(pd.Series(["10/02/2025", "25/12/2025"]))
    assert dmy.tolist()[0] == pd.Timestamp("2025-02-10")
    # Año primero: siempre año-mes-día, aunque la columna sea de día primero
    mezcla = qc.parsear_fechas_flexible(pd.Series(["2025/03/07", "25/12/2025", "2025-03-07"]))
    assert mezcla.tolist() == [pd.Timestamp("2025-03-07"), pd.Timestamp("2025-12-25"),
                               pd.Timestamp("2025-03-07")]
    mdy = qc.parsear_fechas_flexible(pd.Series(["10/02/2025", "12/25/2025"]))
    assert mdy.tolist()[0] == pd.Timestamp("2025-10-02")
    raras = qc.fechas_imposibles(pd.Series(["1850-01-01", "2199-01-01", "2025-01-01", None]))
    assert raras.tolist() == ["1850-01-01", "2199-01-01"]
    assert qc.fechas_imposibles(pd.Series(pd.to_datetime(["1850-01-01", "2020-01-01"]))).size == 1
    assert not qc.es_mayormente_fecha(pd.Series(["Gorra", "Camiseta"], name="Producto"))
    assert qc.es_mayormente_fecha(pd.Series(pd.to_datetime(["2020-01-01"])))
