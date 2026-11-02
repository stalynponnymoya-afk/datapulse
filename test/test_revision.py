"""
Tests del nodo revisor (el LLM revisa el resultado de OpenHands) y de su
integración en el grafo LangGraph. Sin red: el LLM es un guion.
"""

import json
import os
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402
from src.quality_checker import generate_report_df  # noqa: E402

RESIDUAL = [{"columna": "Edad", "problema": "valores no numéricos", "cantidad": 1,
             "ejemplos": ["treinta"]}]


def _estado(**extra):
    base = {
        "openhands_status": "finished", "intentos": 1, "score_antes": 60,
        "score_despues": 85, "score_reportado": 99, "residuales": RESIDUAL,
        "plan_acciones": [{"columna": "Edad", "problema": "tipos_mixtos",
                           "accion": "pd_to_numeric_coerce"}],
        "sin_regla": [], "error_history": [],
    }
    base.update(extra)
    return base


def _llm(respuesta):
    llamadas = []

    def invoke(mensajes):
        llamadas.append(mensajes[0].content)
        if isinstance(respuesta, Exception):
            raise respuesta
        return SimpleNamespace(content=respuesta)

    return SimpleNamespace(invoke=invoke, llamadas=llamadas)


# ── Residuales ──────────────────────────────────────────────────────────────

def test_problemas_residuales():
    df = pd.DataFrame({
        "Row": range(1, 9),
        "Edad": [30, "treinta", 41, 35, None, 30, 30, 33],
        "Ciudad": ["Sevilla", "sevilla ", "Madrid", "Madrid", "Madrid", "Sevilla", "Sevilla", "Lima"],
        "Fecha": ["2025-01-01", "02/01/2025", "2025-13-45", "2025-01-04",
                  "2025-01-05", "2025-01-01", "2025-01-01", "2025-01-08"],
    })
    df.loc[6, "Row"] = 7
    df = pd.concat([df, df.iloc[[0]].assign(Row=9)], ignore_index=True)
    filas = dp._problemas_residuales(df, generate_report_df(df.astype({"Row": int})))
    por_problema = {(f["columna"], f["problema"]): f for f in filas}
    assert por_problema[("Edad", "valores no numéricos")]["ejemplos"] == ["treinta"]
    assert por_problema[("Edad", "nulos")]["cantidad"] == 1
    assert por_problema[("Fecha", "fechas imposibles")]["ejemplos"] == ["2025-13-45"]
    assert ("Fecha", "formatos de fecha mezclados") in por_problema
    assert ("Ciudad", "texto inconsistente") in por_problema
    assert por_problema[("(filas)", "filas duplicadas")]["cantidad"] >= 1
    assert not any(f["columna"] == "Row" for f in filas)


# ── Nodo revisor ────────────────────────────────────────────────────────────

def test_revisor_pide_correccion(monkeypatch):
    llm = _llm(json.dumps({"veredicto": "corregir", "analisis": "Edad sigue con texto",
                           "correcciones": "Aplicar pd_to_numeric_coerce a Edad."}))
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    out = dp.node_revisar_resultado(_estado())
    assert out["veredicto_revision"] == "corregir"
    assert out["instrucciones_correccion"] == "Aplicar pd_to_numeric_coerce a Edad."
    # El revisor recibe lo medido y el plan del grafo, no los datos crudos
    contexto = llm.llamadas[0]
    assert '"score_despues": 85' in contexto and "pd_to_numeric_coerce" in contexto
    assert "treinta" in contexto and "No inventes reglas" in contexto


@pytest.mark.parametrize("veredicto", ["aprobado", "terminar"])
def test_revisor_sin_correccion(monkeypatch, veredicto):
    monkeypatch.setattr(dp, "get_llm", lambda: _llm(json.dumps(
        {"veredicto": veredicto, "analisis": "ok", "correcciones": "no debería usarse"})))
    out = dp.node_revisar_resultado(_estado())
    assert out["veredicto_revision"] == veredicto
    assert out["instrucciones_correccion"] is None


@pytest.mark.parametrize("respuesta", ['{"veredicto": "quizas"}', "no es json",
                                       RuntimeError("sin red")])
def test_revisor_invalido_o_caido_no_decide(monkeypatch, respuesta):
    monkeypatch.setattr(dp, "get_llm", lambda: _llm(respuesta))
    out = dp.node_revisar_resultado(_estado())
    assert out["veredicto_revision"] is None


def test_revisor_no_se_llama_sin_resultado_medido(monkeypatch):
    def _no_llamar():
        raise AssertionError("no debería llamarse al LLM")

    monkeypatch.setattr(dp, "get_llm", _no_llamar)
    out = dp.node_revisar_resultado(_estado(residuales=None, openhands_status="error"))
    assert out == {"veredicto_revision": None, "revision": None}


# ── Router con el veredicto ─────────────────────────────────────────────────

@pytest.mark.parametrize("score, veredicto, intentos, esperado", [
    (85, "aprobado", 1, "commit"),
    (85, None, 1, "commit"),
    (85, "corregir", 1, "retry"),                                 # el LLM pide otro intento
    (85, "corregir", dp.OH_MAX_INTENTOS_SCORE, "commit"),         # sin presupuesto: vale lo medido
    (50, "aprobado", 1, "retry"),                                 # el LLM no aprueba bajo el umbral
    (50, "aprobado", dp.OH_MAX_INTENTOS_SCORE, "end_no_commit"),
    (50, "terminar", 1, "end_no_commit"),                         # el LLM corta antes
    (50, "corregir", 1, "retry"),
])
def test_router_con_veredicto(score, veredicto, intentos, esperado):
    estado = {"openhands_status": "finished", "score_despues": score,
              "veredicto_revision": veredicto, "intentos": intentos}
    assert dp.router_validar_score(estado) == esperado


# ── Las correcciones llegan al siguiente intento ────────────────────────────

def test_correcciones_en_el_prompt_local_y_en_el_brief(monkeypatch):
    estado = {"instrucciones_correccion": "Aplicar pd_to_numeric_coerce a Edad.",
              "error_history": [], "brief_tecnico": "brief", "file_ext": "csv",
              "metadata": {"filas": 1, "columnas": 1}}
    assert "CORRECCIONES DEL REVISOR" in dp._seccion_errores_previos(estado)
    brief = dp._construir_brief_openhands(estado, "data/x.csv", "rama")
    assert "CORRECCIONES DEL REVISOR" in brief and "pd_to_numeric_coerce" in brief
    assert "CORRECCIONES" not in dp._seccion_errores_previos({"error_history": []})


# ── Integración con LangGraph (modo local, script real) ─────────────────────

SCRIPT = (
    "import sys\nimport pandas as pd\n"
    "df = pd.read_csv(sys.argv[1]).drop_duplicates()\n"
    "df['edad'] = pd.to_numeric(df['edad'], errors='coerce').fillna(30)\n"
    "df.to_csv('processed_data.csv', index=False)\n"
)


class LLMGuion:
    """Diagnostica, planifica, genera el script y revisa: corregir y luego aprobar."""

    def __init__(self):
        self.revisiones = 0
        self.generaciones = []

    def invoke(self, mensajes):
        p = mensajes[0].content
        if "revisor de DataPulse" in p:
            self.revisiones += 1
            veredicto = "corregir" if self.revisiones == 1 else "aprobado"
            return SimpleNamespace(content=json.dumps({
                "veredicto": veredicto, "analisis": f"revisión {self.revisiones}",
                "correcciones": "Convertir edad a número." if veredicto == "corregir" else ""}))
        if "planificador técnico" in p:
            return SimpleNamespace(content=json.dumps({"orden": ["a1"], "brief_tecnico": "Brief."}))
        if "Escribe el script Python COMPLETO" in p:
            self.generaciones.append(p)
            return SimpleNamespace(content=SCRIPT)
        return SimpleNamespace(content=json.dumps({
            "edad": {"tipo_inferido": "numerica_discreta", "problemas": ["nulos_bajos"]}}))


def test_flujo_langgraph_revisor_corrige_y_aprueba(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "OPENHANDS_API_KEY", "")
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "")
    llm = LLMGuion()
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    datos = tmp_path / "ventas.csv"
    datos.write_text("edad\n30\n\n41\n35\n35\n", encoding="utf-8")

    final = dp.run_datapulse(str(datos))

    assert final["intentos"] == 2                       # corregir → segundo intento
    assert llm.revisiones == 2
    assert "CORRECCIONES DEL REVISOR" not in llm.generaciones[0]
    assert "Convertir edad a número." in llm.generaciones[1]
    assert final["veredicto_revision"] == "aprobado"
    assert final["github_url"].startswith("local://outputs/")
    assert final["score_despues"] >= dp.MIN_SCORE_PASS
