"""
Tests de la tabla de residuales de validar_score y de su uso en
error_history para el siguiente intento (arquitectura de 7 nodos).
"""

import json
import os
import sys
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402
from src.quality_checker import generate_report_df  # noqa: E402


def test_problemas_residuales():
    df = pd.DataFrame({
        "Row": range(1, 9),
        "Edad": [30, "treinta", 41, 35, None, 30, 30, 33],
        "Ciudad": ["Sevilla", "sevilla ", "Madrid", "Madrid", "Madrid", "Sevilla", "Sevilla", "Lima"],
        "Fecha": ["2025-01-01", "02/01/2025", "2025-13-45", "2025-01-04",
                  "2025-01-05", "2025-01-01", "2025-01-01", "2025-01-08"],
    })
    df = pd.concat([df, df.iloc[[0]].assign(Row=9)], ignore_index=True)
    filas = dp._problemas_residuales(df, generate_report_df(df))
    por_problema = {(f["columna"], f["problema"]): f for f in filas}
    assert por_problema[("Edad", "valores no numéricos")]["ejemplos"] == ["treinta"]
    assert por_problema[("Edad", "nulos")]["cantidad"] == 1
    assert por_problema[("Fecha", "fechas imposibles")]["ejemplos"] == ["2025-13-45"]
    assert ("Fecha", "formatos de fecha mezclados") in por_problema
    assert ("Ciudad", "texto inconsistente") in por_problema
    assert por_problema[("(filas)", "filas duplicadas")]["cantidad"] == 3
    assert not any(f["columna"] == "Row" for f in filas)


def test_resumir_residuales():
    filas = [{"columna": "Edad", "problema": "valores no numéricos", "cantidad": 1,
              "ejemplos": ["treinta"]},
             {"columna": "(filas)", "problema": "filas duplicadas", "cantidad": 15, "ejemplos": []}]
    texto = dp._resumir_residuales(filas)
    assert texto == "Edad: valores no numéricos (1; ej.: treinta); (filas): filas duplicadas (15)"
    assert dp._resumir_residuales([]) == "sin problemas detectados"
    assert "y 2 más" in dp._resumir_residuales(filas * 5, max_items=8)


def test_validar_score_usa_los_residuales_en_error_history(tmp_path):
    processed = tmp_path / "processed_data.csv"
    pd.DataFrame({"edad": [30, "treinta", -5, None, None, None, 41, 33],
                  "ciudad": [" Lima", "lima", "LIMA", "Quito", None, None, "Quito", "quito "]}
                 ).to_csv(processed, index=False)
    out = dp.node_validar_score({"score_antes": 40, "openhands_status": "finished",
                                 "ruta_processed": str(processed)})
    assert out["score_despues"] < dp.MIN_SCORE_PASS
    assert out["residuales"]
    assert dp._resumir_residuales(out["residuales"]) in out["error_history"][0]


# ── Integración LangGraph: score bajo → reintento con los residuales ─────────

class LLMGuion:
    """Diagnóstico, plan y dos scripts: el primero deja texto, el segundo limpia."""

    def __init__(self):
        self.generaciones = []

    def invoke(self, mensajes):
        p = mensajes[0].content
        if "planificador técnico" in p:
            return SimpleNamespace(content=json.dumps(
                {"orden": ["a1"], "acciones_fallback": [], "brief_tecnico": "Brief."}))
        if "Escribe el script Python COMPLETO" in p:
            self.generaciones.append(p)
            if len(self.generaciones) == 1:
                codigo = ("import sys, shutil\n"
                          "shutil.copy(sys.argv[1], 'processed_data.csv')\n")
            else:
                codigo = ("import sys\nimport pandas as pd\n"
                          "df = pd.read_csv(sys.argv[1])\n"
                          "df['edad'] = pd.to_numeric(df['edad'], errors='coerce')\n"
                          "df = df.dropna().drop_duplicates()\n"
                          "df.to_csv('processed_data.csv', index=False)\n")
            return SimpleNamespace(content=codigo)
        return SimpleNamespace(content=json.dumps(
            {"edad": {"tipo_inferido": "numerica_discreta", "problemas": ["nulos_bajos"]}}))


def test_flujo_langgraph_reintenta_con_los_residuales(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "OPENHANDS_API_KEY", "")
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "")
    llm = LLMGuion()
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    datos = tmp_path / "ventas.csv"
    # Score bajo: nulos, duplicados y un número escrito como texto
    filas = (["edad,ciudad", "treinta,Lima"] + [",Lima"] * 6 + [",Quito"] * 2
             + [f"{20 + i},Quito" for i in range(6)])
    datos.write_text("\n".join(filas) + "\n", encoding="utf-8")

    final = dp.run_datapulse(str(datos))

    assert final["intentos"] == 2
    assert "ERRORES INTENTOS PREVIOS" not in llm.generaciones[0]
    segundo = llm.generaciones[1]
    assert "Problemas que quedan en processed_data.csv" in segundo
    assert "edad: valores no numéricos" in segundo and "treinta" in segundo
    assert final["score_despues"] >= dp.MIN_SCORE_PASS
    assert final["github_url"].startswith("local://outputs/")
    assert final["informe_final"]["score_despues"] == final["score_despues"]
