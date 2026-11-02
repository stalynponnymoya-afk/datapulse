"""
Tests del nodo informe_final: tabla con el score y los valores que no se
pudieron limpiar, resumen del LLM y cierre de toda ejecución en LangGraph.
"""

import json
import os
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402


def _llm(texto="Resumen del LLM.", fallar=False):
    prompts = []

    def invoke(mensajes):
        prompts.append(mensajes[0].content)
        if fallar:
            raise RuntimeError("sin red")
        return SimpleNamespace(content=texto)

    return SimpleNamespace(invoke=invoke, prompts=prompts)


def _processed(tmp_path):
    ruta = tmp_path / "processed_data.csv"
    pd.DataFrame({"edad": [30, "treinta", 41, None, 35, 28, 52, 44, 39, 61],
                  "ciudad": ["Lima", "lima ", "Quito", "Quito", "Lima"] * 2}
                 ).to_csv(ruta, index=False)
    return str(ruta)


def _estado(**extra):
    base = {"openhands_status": "finished", "intentos": 1, "score_antes": 50,
            "score_despues": 80, "score_reportado": 99, "residuales": [], "sin_regla": [],
            "plan_acciones": [], "error_history": [], "github_url": "local://outputs/x",
            "veredicto_revision": "aprobado", "revision": {"analisis": "ok"}}
    base.update(extra)
    return base


def test_informe_tras_ejecucion_correcta(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    llm = _llm()
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    residuales = [{"columna": "edad", "problema": "valores no numéricos", "cantidad": 1,
                   "ejemplos": ["treinta"]}]
    sin_regla = [{"columna": "ciudad", "problema": "abreviaturas"}]
    out = dp.node_informe_final(_estado(residuales=residuales, sin_regla=sin_regla))["informe_final"]

    assert out["score_despues"] == 80 and out["valores_sin_limpiar"] == residuales
    assert out["problemas_sin_regla"] == sin_regla and out["resumen"] == "Resumen del LLM."
    # El LLM solo recibe la tabla medida
    assert '"score_despues": 80' in llm.prompts[0] and "no inventes números" in llm.prompts[0]
    # Archivos locales
    md = open(os.path.join(out["ruta"], "informe_final.md"), encoding="utf-8").read()
    assert "| edad | valores no numéricos | 1 | treinta |" in md
    assert "| ciudad | abreviaturas |" in md
    guardado = json.load(open(os.path.join(out["ruta"], "informe_final.json"), encoding="utf-8"))
    assert guardado["resumen"] == "Resumen del LLM."


def test_informe_tras_timeout_mide_el_resultado_parcial(tmp_path, monkeypatch):
    """OpenHands tardó demasiado pero dejó un processed_data.csv: se mide igual."""
    monkeypatch.chdir(tmp_path)
    llm = _llm("OpenHands superó el tiempo; el resultado parcial tiene 2 problemas.")
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    estado = _estado(openhands_status="timeout", residuales=None, score_despues=0,
                     ruta_processed=_processed(tmp_path), github_url=None,
                     veredicto_revision=None, revision=None,
                     error_history=["El script superó el tiempo máximo de ejecución (900s)."])
    assert dp.router_validar_score(estado) == "end_no_commit"

    out = dp.node_informe_final(estado)["informe_final"]
    assert out["resultado_medido"] is True and out["score_despues"] is not None
    problemas = {(f["columna"], f["problema"]) for f in out["valores_sin_limpiar"]}
    assert ("edad", "valores no numéricos") in problemas
    assert '"estado": "timeout"' in llm.prompts[0] and "tiempo máximo" in llm.prompts[0]
    assert "no se publicó" in open(os.path.join(out["ruta"], "informe_final.md"), encoding="utf-8").read()


def test_informe_sin_resultado_y_sin_llm(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "get_llm", lambda: _llm(fallar=True))
    out = dp.node_informe_final(_estado(openhands_status="stuck", residuales=None,
                                        ruta_processed=None, github_url=None,
                                        error_history=["stuck en OpenHands"]))["informe_final"]
    assert out["score_despues"] is None and out["valores_sin_limpiar"] == []
    assert "stuck" in out["resumen"] and "No hubo un processed_data.csv" in out["resumen"]


def test_informe_rechazado_no_llama_al_llm(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    def _no_llamar():
        raise AssertionError("no debería llamarse al LLM")

    monkeypatch.setattr(dp, "get_llm", _no_llamar)
    out = dp.node_informe_final(_estado(openhands_status="rechazado", residuales=None,
                                        error_history=["Modo OpenHands no arranca"]))
    assert "rechazado" in out["informe_final"]["resumen"]


def test_langgraph_timeout_local_termina_en_informe(tmp_path, monkeypatch):
    """Grafo completo: el script tarda demasiado → 1 intento → informe final."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "OPENHANDS_API_KEY", "")
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "")
    monkeypatch.setattr(dp, "OH_TIMEOUT_SEG", 1)

    class LLM:
        def __init__(self):
            self.informes = 0

        def invoke(self, mensajes):
            p = mensajes[0].content
            if "Escribe el script Python COMPLETO" in p:
                return SimpleNamespace(content="import time\ntime.sleep(10)\n")
            if "planificador técnico" in p:
                return SimpleNamespace(content=json.dumps({"orden": [], "brief_tecnico": "B"}))
            if "informe final de DataPulse" in p:
                self.informes += 1
                return SimpleNamespace(content="El script superó el tiempo máximo.")
            return SimpleNamespace(content=json.dumps(
                {"edad": {"tipo_inferido": "numerica_discreta", "problemas": ["nulos_bajos"]}}))

    llm = LLM()
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    datos = tmp_path / "d.csv"
    datos.write_text("edad\n30\n\n41\n", encoding="utf-8")

    final = dp.run_datapulse(str(datos))
    assert final["openhands_status"] == "timeout"
    assert final["intentos"] == 1                       # no se re-lanza
    assert final["github_url"] is None                  # sin commit
    assert llm.informes == 1
    assert final["informe_final"]["resumen"] == "El script superó el tiempo máximo."
    assert os.path.exists(os.path.join(final["informe_final"]["ruta"], "informe_final.md"))


def test_langgraph_commit_tambien_termina_en_informe(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "OPENHANDS_API_KEY", "")
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "")

    class LLM:
        def invoke(self, mensajes):
            p = mensajes[0].content
            if "Escribe el script Python COMPLETO" in p:
                return SimpleNamespace(content=(
                    "import sys\nimport pandas as pd\n"
                    "pd.read_csv(sys.argv[1]).fillna(30).to_csv('processed_data.csv', index=False)\n"))
            if "planificador técnico" in p:
                return SimpleNamespace(content=json.dumps({"orden": ["a1"], "brief_tecnico": "B"}))
            if "revisor de DataPulse" in p:
                return SimpleNamespace(content=json.dumps(
                    {"veredicto": "aprobado", "analisis": "ok", "correcciones": ""}))
            if "informe final de DataPulse" in p:
                return SimpleNamespace(content="Todo bien.")
            return SimpleNamespace(content=json.dumps(
                {"edad": {"tipo_inferido": "numerica_discreta", "problemas": ["nulos_bajos"]}}))

    monkeypatch.setattr(dp, "get_llm", lambda: LLM())
    datos = tmp_path / "d.csv"
    datos.write_text("edad\n30\n\n41\n35\n", encoding="utf-8")
    final = dp.run_datapulse(str(datos))
    assert final["github_url"].startswith("local://")
    assert final["informe_final"]["destino"] == final["github_url"]
    assert final["informe_final"]["resumen"] == "Todo bien."
