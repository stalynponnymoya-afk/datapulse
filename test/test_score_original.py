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


# ── Punto 4: el commit exige no empeorar ────────────────────────────────────

import json  # noqa: E402
from types import SimpleNamespace  # noqa: E402

import pytest  # noqa: E402


@pytest.mark.parametrize("antes, despues, intentos, esperado", [
    (60, 90, 1, "commit"),
    (90, 90, 1, "commit"),                          # igual: no empeora
    (95, 80, 1, "retry"),                           # pasa el umbral pero empeora
    (95, 80, dp.OH_MAX_INTENTOS_SCORE, "end_no_commit"),
    (None, 80, 1, "commit"),                        # sin score_antes: solo el umbral
    (40, 50, 1, "retry"),                           # mejora pero bajo el umbral
])
def test_router_exige_no_empeorar(antes, despues, intentos, esperado):
    estado = {"openhands_status": "finished", "score_antes": antes, "score_despues": despues,
              "intentos": intentos}
    assert dp.router_validar_score(estado) == esperado


def test_empeorar_va_a_error_history_con_residuales(tmp_path):
    processed = pd.DataFrame({"edad": [30 + 2 * i for i in range(19)] + [None],
                              "ciudad": ["quito", "lima"] * 10, "comentario": ["ok"] * 20})
    estado = _estado(tmp_path, processed)
    estado["score_antes"] = 100
    out = dp.node_validar_score(estado)
    assert dp.MIN_SCORE_PASS <= out["score_despues"] < 100
    assert "el score empeoró" in out["error_history"][0]
    assert "edad: nulos" in out["error_history"][0]
    assert dp.router_validar_score({**estado, **out}) == "retry"


def test_langgraph_sigue_peor_al_agotar_intentos(tmp_path, monkeypatch):
    """Original limpio, el script lo empeora siempre: 3 intentos, sin commit, informe."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "OPENHANDS_API_KEY", "")
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "")
    monkeypatch.setattr(dp, "MIN_SCORE_PASS", 50)

    class LLM:
        def invoke(self, mensajes):
            p = mensajes[0].content
            if "planificador técnico" in p:
                return SimpleNamespace(content=json.dumps(
                    {"orden": [], "acciones_fallback": [], "brief_tecnico": "B"}))
            if "Escribe el script Python COMPLETO" in p:
                return SimpleNamespace(content=(
                    "import sys\nimport pandas as pd\ndf = pd.read_csv(sys.argv[1])\n"
                    "df.loc[:4, 'edad'] = None\ndf.to_csv('processed_data.csv', index=False)\n"))
            return SimpleNamespace(content=json.dumps(
                {"edad": {"tipo_inferido": "numerica_discreta", "problemas": ["ninguno"]}}))

    monkeypatch.setattr(dp, "get_llm", lambda: LLM())
    datos = tmp_path / "d.csv"
    pd.DataFrame({"edad": [30 + 2 * i for i in range(20)]}).to_csv(datos, index=False)

    final = dp.run_datapulse(str(datos))
    assert final["intentos"] == dp.OH_MAX_INTENTOS_SCORE
    assert final["score_despues"] < final["score_antes"]
    assert final["score_despues"] >= dp.MIN_SCORE_PASS       # pasaba el umbral
    assert final["github_url"] is None
    inf = final["informe_final"]
    assert inf["score_empeoro"] is True
    assert "Terminó sin commit porque el score empeoró" in inf["resumen"]
