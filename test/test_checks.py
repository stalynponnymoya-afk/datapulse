"""
Tests de los checks de calidad y del score (Bloque B).
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.quality_checker import (  # noqa: E402
    _tokens_nombre,
    analyze_variable_types,
    check_column_relationships,
    check_duplicates,
    check_invalid_dates,
    check_negative_values,
    check_nulls,
    check_outliers,
    check_text_inconsistencies,
    generate_report,
    generate_report_df,
)

DATA = os.path.join(os.path.dirname(__file__), "..", "data")


# ── Tokens de nombres ────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "nombre, esperado",
    [
        ("orderDate", ["order", "date"]),
        ("Fecha_Nacimiento", ["fecha", "nacimiento"]),
        ("Ship Date", ["ship", "date"]),
        ("HTTPStatus-Code", ["http", "status", "code"]),
        ("duración", ["duracion"]),
        (2024, ["2024"]),
    ],
)
def test_tokens_nombre(nombre, esperado):
    assert _tokens_nombre(nombre) == esperado


# ── Fechas ───────────────────────────────────────────────────────────────────

def test_width_no_es_fecha():
    df = pd.DataFrame({"width": ["ancho", "alto", "bajo"], "bandwidth": [1, 2, 3]})
    assert check_invalid_dates(df) == {}


def test_columna_numerica_no_se_evalua_como_fecha():
    df = pd.DataFrame({"created_dt": [20240101, 20240102, 99999999]})
    assert check_invalid_dates(df) == {}


def test_coincidencia_por_tokens_del_nombre():
    valores = ["2024-01-01", "no es fecha", "2024-03-01"]
    for nombre in ("order_dt", "orderDate", "Fecha-Alta", "created at"):
        res = check_invalid_dates(pd.DataFrame({nombre: valores}))
        assert nombre in res, nombre
        assert res[nombre]["invalid_count"] == 1
    # Subcadenas que no son tokens no cuentan (contenido sin aspecto de fecha)
    textos = ["ancho", "alto", "medio"]
    for nombre in ("width", "update_flag_x", "datum", "dtype"):
        assert check_invalid_dates(pd.DataFrame({nombre: textos})) == {}, nombre


def test_dd_mm_aaaa_sin_falsos_invalidos():
    df = pd.DataFrame({"fecha": ["01/02/2024", "13/01/2024", "25/12/2023", "31/12/2023"]})
    assert check_invalid_dates(df) == {}


def test_fecha_realmente_invalida_se_reporta():
    df = pd.DataFrame({"fecha": ["2024-01-01", "2024-02-31", "2024-03-01"]})
    res = check_invalid_dates(df)
    assert res["fecha"]["invalid_count"] == 1
    assert "2024-02-31" in res["fecha"]["examples"]


def test_deteccion_por_contenido_en_columna_str():
    # Columna sin palabra clave pero con contenido de fecha (dtype str en pandas 3)
    df = pd.DataFrame({"alta": ["2024-01-01", "2024-02-30", "2024-03-01", "2024-04-01"]})
    assert "alta" in check_invalid_dates(df)


def test_tope_superior_relativo_a_hoy():
    hoy = pd.Timestamp.now()
    dentro = (hoy + pd.DateOffset(years=4)).strftime("%Y-%m-%d")
    fuera = (hoy + pd.DateOffset(years=6)).strftime("%Y-%m-%d")
    assert check_invalid_dates(pd.DataFrame({"fecha": [dentro, "2020-01-01"]})) == {}
    res = check_invalid_dates(pd.DataFrame({"fecha": [fuera, "1850-01-01", "2020-01-01"]}))
    assert res["fecha"]["out_of_range_count"] == 2


def test_columnas_datetime_y_vacias_se_ignoran():
    df = pd.DataFrame(
        {
            "fecha": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "fecha_texto": [None, "  "],
        }
    )
    assert check_invalid_dates(df) == {}


# ── Porcentajes ──────────────────────────────────────────────────────────────

def test_heart_rate_no_es_porcentaje():
    df = pd.DataFrame({"heart_rate": [60, 120, 180], "tasa_cambio": [3800, 3900, 4000],
                       "debt_ratio": [150, 200, 250]})
    assert "percentage_out_of_range" not in check_column_relationships(df)


def test_porcentaje_fuera_de_rango():
    df = pd.DataFrame({"discount_pct": [10, 150, -5], "porc_avance": [10, 20, 30],
                       "CompletionPercentage": [50, 101, 20]})
    res = check_column_relationships(df)["percentage_out_of_range"]
    assert set(res) == {"discount_pct", "CompletionPercentage"}
    assert res["discount_pct"]["out_of_range_count"] == 2


def test_relacion_producto_rota():
    n = 20
    df = pd.DataFrame({"quantity": np.arange(1, n + 1), "unit_price": [2.0] * n})
    df["total"] = df["quantity"] * df["unit_price"]
    df.loc[0, "total"] = 999
    res = check_column_relationships(df)
    assert res["product_relationships"][0]["mismatches"] == 1


# ── Negativos ────────────────────────────────────────────────────────────────

def test_longitude_y_profit_no_penalizan_edad_si():
    df = pd.DataFrame({
        "longitude": [-122.2, -118.3, -121.0],
        "Profit": [-10.0, 25.0, 3.0],
        "edad": [30, -1, 45],
        "Unit Price": [1.0, -2.0, 3.0],
        "ok": [1, 2, 3],
    })
    neg = check_negative_values(df)
    assert set(neg) == {"longitude", "Profit", "edad", "Unit Price"}
    assert neg["longitude"]["penalizado"] is False
    assert neg["Profit"]["penalizado"] is False
    assert neg["edad"]["penalizado"] is True
    assert neg["Unit Price"]["penalizado"] is True


@pytest.mark.parametrize("nombre", [
    "monto_transaccion", "Billing Amount", "importe", "Shipping Cost", "costo_envio",
    "peso_kg", "altura", "Order Quantity", "qty", "unidades", "units_sold", "conteo",
    "item_count", "stock", "distancia_km", "duración", "duration_min", "edades", "prices",
])
def test_no_negativos_penalizan(nombre):
    df = pd.DataFrame({nombre: [1.0, -2.0, 3.0]})
    assert check_negative_values(df)[nombre]["penalizado"] is True, nombre


@pytest.mark.parametrize("nombre", [
    "saldo", "saldo_monto", "balance_amount", "Account Balance", "margen_precio",
    "Product Base Margin", "profit_margin", "Profit", "longitude", "variacion", "temperatura",
])
def test_excepciones_y_otras_columnas_no_penalizan(nombre):
    df = pd.DataFrame({nombre: [1.0, -2.0, 3.0]})
    assert check_negative_values(df)[nombre]["penalizado"] is False, nombre


def test_lista_unica_en_checker_y_prompt():
    import datapulse_prod as dp
    from src import quality_checker as qc

    # datapulse_prod usa exactamente las mismas constantes (no una copia)
    assert dp.PALABRAS_NO_NEGATIVAS is qc.PALABRAS_NO_NEGATIVAS
    assert dp.PALABRAS_NEGATIVOS_PERMITIDOS is qc.PALABRAS_NEGATIVOS_PERMITIDOS

    prompt = dp.construir_prompt_diagnostico({"filas": 1})
    regla4 = next(l for l in prompt.splitlines() if l.startswith("4. NEGATIVOS INESPERADOS"))
    for palabra in qc.PALABRAS_NO_NEGATIVAS + qc.PALABRAS_NEGATIVOS_PERMITIDOS:
        assert palabra in regla4, palabra
    assert '"filas": 1' in prompt
    # La lista base pedida, con equivalentes en inglés y los añadidos
    esperadas = {
        "monto", "amount", "importe", "precio", "price", "costo", "cost", "edad", "age",
        "peso", "weight", "altura", "height", "cantidad", "quantity", "stock", "distancia",
        "distance", "duración", "duration", "unidades", "units", "conteo", "count",
    }
    assert esperadas <= set(qc.PALABRAS_NO_NEGATIVAS)
    assert set(qc.PALABRAS_NEGATIVOS_PERMITIDOS) == {"saldo", "balance", "margen", "margin"}


def test_score_penaliza_solo_negativos_marcados():
    base = pd.DataFrame({"x": [1.0, 2.0, 3.0, 4.0]})
    score_base = generate_report_df(base)["quality_score"]
    con_long = base.assign(longitude=[-1.0, -2.0, -3.0, -4.0])
    con_edad = base.assign(edad=[1, -2, 3, 4])
    assert generate_report_df(con_long)["quality_score"] == score_base
    assert generate_report_df(con_edad)["quality_score"] == score_base - 5


def test_datasets_reales_no_penalizan_longitude_ni_profit():
    rep = generate_report(os.path.join(DATA, "california_housing.csv"))
    assert rep["negative_values"]["longitude"]["penalizado"] is False
    rep = generate_report(os.path.join(DATA, "superstore_sales.csv"))
    assert rep["negative_values"]["Profit"]["penalizado"] is False
    rep = generate_report(os.path.join(DATA, "healthcare_dataset.csv"))
    assert rep["negative_values"]["Billing Amount"]["penalizado"] is True


# ── Reporte ──────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("df", [pd.DataFrame(), pd.DataFrame({"a": []}), None])
def test_dataframe_vacio(df):
    with pytest.raises(ValueError, match="dataset vacío"):
        generate_report_df(df)


def test_columnas_no_string():
    df = pd.DataFrame({0: [1, 2, 3], 1.5: ["a", "b", "c"], "fecha": ["x", "y", "z"]})
    rep = generate_report_df(df, "numeros")
    assert rep["file"] == "numeros"
    assert set(rep["data_types"]) == {"0", "1.5", "fecha"}
    # El DataFrame original no se modifica
    assert list(df.columns) == [0, 1.5, "fecha"]


def test_generate_report_envoltorio(tmp_path):
    ruta = tmp_path / "d.csv"
    ruta.write_text("a,b\n1,x\n1,x\n2,  y\n", encoding="utf-8")
    rep = generate_report(str(ruta))
    assert rep["file"] == str(ruta)
    assert rep["shape"] == {"rows": 3, "columns": 2}
    assert rep["duplicate_analysis"]["count"] == 1
    assert 0 <= rep["quality_score"] <= 100


def test_veredictos():
    limpio = pd.DataFrame({"a": range(10), "b": list("abcdefghij")})
    assert generate_report_df(limpio)["verdict"] == "EXCELENTE"
    sucio = pd.DataFrame({"a": [None] * 9 + [1.0], "b": ["x"] * 10, "edad": [-1] * 10})
    sucio = pd.concat([sucio, sucio])
    assert generate_report_df(sucio)["quality_score"] < 50
    assert generate_report_df(sucio)["verdict"].startswith("POBRE")


def test_checks_basicos():
    df = pd.DataFrame({
        "n": [1.0, None, 3.0, 1000.0, 2.0, 2.5, 1.5, 2.2],
        "t": [" Madrid", "madrid", "MADRID", "Lima", "Lima", "Lima", "Quito  ", "Quito"],
        "b": [True, False, True, True, False, True, True, False],
    })
    assert check_nulls(df) == {"n": 12.5}
    assert check_duplicates(df)["count"] == 0
    assert "n" in check_outliers(df)
    txt = check_text_inconsistencies(df)["t"]
    assert "madrid" in txt["case_variations"]
    assert txt["whitespace_issues"] == 2
    tipos = analyze_variable_types(df)
    assert tipos["type_groups"]["boolean"] == ["b"]
    assert "float" in tipos["type_summary"]
