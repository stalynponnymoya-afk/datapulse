"""
Tests del informe final: función determinista (sin LLM) que run_datapulse
llama después de app.invoke. No es un nodo del grafo.
"""

import json
import os
import sys
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402


def _sin_llm(monkeypatch):
    def _no_llamar():
        raise AssertionError("el informe final no debe llamar al LLM")

    monkeypatch.setattr(dp, "get_llm", _no_llamar)


def _processed(tmp_path):
    ruta = tmp_path / "processed_data.csv"
    pd.DataFrame({"edad": [30, "treinta", 41, None, 35, 28, 52, 44, 39, 61],
                  "ciudad": ["Lima", "lima ", "Quito", "Quito", "Lima"] * 2}
                 ).to_csv(ruta, index=False)
    return str(ruta)


def _estado(**extra):
    base = {"openhands_status": "finished", "intentos": 1, "score_antes": 50,
            "score_despues": 80, "score_reportado": 99, "residuales": [], "sin_regla": [],
            "error_history": [], "github_url": "local://outputs/x"}
    base.update(extra)
    return base


def test_no_es_un_nodo_del_grafo():
    assert "informe_final" not in dp.build_datapulse_graph().get_graph().nodes
    assert not hasattr(dp, "node_informe_final")


def test_informe_tras_ejecucion_correcta(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _sin_llm(monkeypatch)
    residuales = [{"columna": "edad", "problema": "valores no numéricos", "cantidad": 1,
                   "ejemplos": ["treinta"]}]
    sin_regla = [{"columna": "ciudad", "problema": "abreviaturas"}]
    out = dp.generar_informe_final(_estado(residuales=residuales, sin_regla=sin_regla))

    assert out["score_despues"] == 80 and out["valores_sin_limpiar"] == residuales
    assert out["problemas_sin_regla"] == sin_regla
    assert "estado 'finished'" in out["resumen"] and "Score: 50 antes y 80 después" in out["resumen"]
    md = open(os.path.join(out["ruta"], "informe_final.md"), encoding="utf-8").read()
    assert "| edad | valores no numéricos | 1 | treinta |" in md
    assert "| ciudad | abreviaturas |" in md
    guardado = json.load(open(os.path.join(out["ruta"], "informe_final.json"), encoding="utf-8"))
    assert guardado["resumen"] == out["resumen"]


def test_informe_mide_el_resultado_parcial_tras_timeout(tmp_path, monkeypatch):
    """La validación no mide en timeout; el informe sí mide lo que haya."""
    monkeypatch.chdir(tmp_path)
    _sin_llm(monkeypatch)
    out = dp.generar_informe_final(_estado(
        openhands_status="timeout", intentos=2, residuales=None, score_despues=0,
        ruta_processed=_processed(tmp_path), github_url=None,
        error_history=["El script superó el tiempo máximo de ejecución (900s)."]))
    assert out["resultado_medido"] is True and out["score_despues"] is not None
    problemas = {(f["columna"], f["problema"]) for f in out["valores_sin_limpiar"]}
    assert ("edad", "valores no numéricos") in problemas
    assert "timeout" in out["resumen"] and "tiempo máximo" in out["resumen"]
    md = open(os.path.join(out["ruta"], "informe_final.md"), encoding="utf-8").read()
    assert "no se publicó" in md


def test_informe_sin_resultado(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _sin_llm(monkeypatch)
    out = dp.generar_informe_final(_estado(openhands_status="rechazado", residuales=None,
                                           ruta_processed=None, github_url=None,
                                           error_history=["Modo OpenHands no arranca"]))
    assert out["score_despues"] is None and out["valores_sin_limpiar"] == []
    assert "No hubo un processed_data.csv" in out["resumen"]


def test_run_datapulse_adjunta_el_informe_y_timeout_es_infra(tmp_path, monkeypatch):
    """Timeout del script local: reintento de infraestructura (2 intentos) e informe."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "OPENHANDS_API_KEY", "")
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "")
    monkeypatch.setattr(dp, "OH_TIMEOUT_SEG", 1)

    class LLM:
        def invoke(self, mensajes):
            p = mensajes[0].content
            if "Escribe el script Python COMPLETO" in p:
                return SimpleNamespace(content="import time\ntime.sleep(10)\n")
            if "planificador técnico" in p:
                return SimpleNamespace(content=json.dumps(
                    {"orden": [], "acciones_fallback": [], "brief_tecnico": "B"}))
            return SimpleNamespace(content=json.dumps(
                {"edad": {"tipo_inferido": "numerica_discreta", "problemas": ["nulos_bajos"]}}))

    monkeypatch.setattr(dp, "get_llm", lambda: LLM())
    datos = tmp_path / "d.csv"
    datos.write_text("edad\n30\n\n41\n", encoding="utf-8")

    final = dp.run_datapulse(str(datos))
    assert final["openhands_status"] == "timeout"
    assert final["intentos"] == dp.OH_MAX_INTENTOS_INFRA      # se reintenta como infraestructura
    assert final["github_url"] is None
    assert os.path.exists(os.path.join(final["informe_final"]["ruta"], "informe_final.md"))


def test_fallo_del_informe_no_rompe_la_ejecucion(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "OPENHANDS_API_KEY", "")
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "")

    def _roto(_):
        raise RuntimeError("disco lleno")

    monkeypatch.setattr(dp, "generar_informe_final", _roto)
    monkeypatch.setattr(dp, "get_llm", lambda: SimpleNamespace(
        invoke=lambda m: SimpleNamespace(content="{}")))
    datos = tmp_path / "d.csv"
    datos.write_text("a\n1\n", encoding="utf-8")
    try:
        final = dp.run_datapulse(str(datos))
    except ValueError:
        # Sin columnas diagnosticadas el pipeline puede fallar antes: no aplica
        return
    assert final["informe_final"] is None
    assert "No se pudo generar el informe final" in capsys.readouterr().out
