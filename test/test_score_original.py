"""
Tests del punto 3: score sobre las columnas originales, relaciones por tokens
e IQR solo en columnas con más de 10 valores distintos.
"""

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402
from src import quality_checker as qc  # noqa: E402


def _original():
    return pd.DataFrame({"edad": [30 + 2 * i for i in range(20)],   # no consecutivas (no índice)
                         "ciudad": ["quito", "lima"] * 10,
                         "comentario": ["ok"] * 20})


def _estado(tmp_path, processed):
    ruta_o, ruta_p = tmp_path / "orig.csv", tmp_path / "processed_data.csv"
    _original().to_csv(ruta_o, index=False)
    processed.to_csv(ruta_p, index=False)
    meta = dp.serialize_metadata(_original(), "orig.csv")
    return {"filename": "orig.csv", "filename_path": str(ruta_o), "metadata": meta,
            "openhands_status": "finished", "ruta_processed": str(ruta_p), "intentos": 1}


def test_score_solo_con_columnas_originales(tmp_path):
    processed = _original().drop(columns=["comentario"])
    # Columnas nuevas con problemas: no deben bajar el score
    processed["edad_rango_invalido"] = [None] * 20
    processed["ciudad_quito"] = [1, 0] * 10
    out = dp.node_validar_score(_estado(tmp_path, processed))

    esperado = qc.generate_report_df(processed[["edad", "ciudad"]])["quality_score"]
    assert out["score_despues"] == esperado == 100
    assert out["columnas_nuevas"] == ["edad_rango_invalido", "ciudad_quito"]
    assert out["columnas_eliminadas"] == ["comentario"]
    assert not any(f["columna"] in out["columnas_nuevas"] for f in out["residuales"])


def test_sin_columnas_originales_es_processed_invalido(tmp_path):
    processed = pd.DataFrame({"EDAD_LIMPIA": range(20)})
    out = dp.node_validar_score(_estado(tmp_path, processed))
    assert out["score_despues"] == 0 and "openhands_status" not in out
    assert "sin ninguna columna del original" in out["error_history"][0]
    assert out["columnas_eliminadas"] == ["edad", "ciudad", "comentario"]


def test_informe_lista_columnas_nuevas_y_eliminadas(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    inf = dp.generar_informe_final({
        "openhands_status": "finished", "intentos": 1, "score_antes": 60, "score_despues": 90,
        "residuales": [], "columnas_nuevas": ["edad_rango_invalido"],
        "columnas_eliminadas": ["comentario"]})
    md = open(os.path.join(inf["ruta"], "informe_final.md"), encoding="utf-8").read()
    assert "Nuevas (flags, dummies), no evaluadas: edad_rango_invalido" in md
    assert "Originales eliminadas: comentario" in md


def test_relaciones_por_tokens_no_por_subcadenas():
    n = 30
    qty = np.arange(1, n + 1)
    precio = np.full(n, 2.0)
    total = qty * precio
    total[0] = 999
    # "Discount" contiene "count" como subcadena: antes se tomaba como cantidad
    df = pd.DataFrame({"Discount": qty, "Unit Price": precio, "Total": total})
    assert "product_relationships" not in qc.check_column_relationships(df)
    df = pd.DataFrame({"Order Quantity": qty, "Unit Price": precio, "Total": total})
    rel = qc.check_column_relationships(df)["product_relationships"]
    assert rel[0]["expected"] == "Total = Order Quantity * Unit Price"
    assert rel[0]["mismatches"] == 1


def test_iqr_no_se_aplica_a_escalas():
    df = pd.DataFrame({
        "Satisfaccion": [5] * 30 + [4] * 8 + [1] * 2,          # escala 1–5
        "Monto": list(np.linspace(10, 20, 39)) + [500.0],      # continua con un extremo
    })
    out = qc.check_outliers(df)
    assert "Satisfaccion" not in out
    assert out["Monto"]["count"] == 1
    meta = dp.serialize_metadata(df, "x.csv")["columnas_info"]
    assert meta["Satisfaccion"]["muestra_anomala"] == []
    assert meta["Monto"]["muestra_anomala"][0] == "500"
