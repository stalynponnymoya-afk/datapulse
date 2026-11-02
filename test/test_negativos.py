"""
Tests de la decisión explícita sobre negativos (válidos / inválidos /
incierto), del contexto y las decisiones del usuario, del analizador con
diagnóstico y de la trazabilidad (audit_log e informe final).
"""

import glob
import json
import os
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402
from src import quality_checker as qc  # noqa: E402

N = 20


def _df():
    return pd.DataFrame({
        "temperatura": [-5.0, 3.2, -1.1, 8.4] * (N // 4),
        "edad": [30, -2, 41, 35] * (N // 4),
        "x7_val": [12, -3, 7, 9] * (N // 4),
        "ciudad": ["Quito", "Lima", "Cusco", "La Paz"] * (N // 4),
    })


def _llm(diagnostico):
    prompts = []

    def invoke(mensajes):
        prompts.append(mensajes[0].content)
        return SimpleNamespace(content=json.dumps(diagnostico))

    return SimpleNamespace(invoke=invoke, prompts=prompts)


DIAG_LLM = {
    "temperatura": {"tipo_inferido": "numerica_continua",
                    "problemas": ["valores_negativos_inesperados"],   # contradice "validos"
                    "negativos": "validos",
                    "razon_negativos": "Las temperaturas bajo cero son físicamente posibles."},
    "edad": {"tipo_inferido": "numerica_discreta", "problemas": ["ninguno"],
             "negativos": "invalidos", "razon_negativos": "Una edad no puede ser negativa."},
    "x7_val": {"tipo_inferido": "numerica_continua", "problemas": [],
               "negativos": "incierto", "razon_negativos": "El nombre no indica qué se mide."},
    "ciudad": {"tipo_inferido": "categorica_nominal", "problemas": ["ninguno"]},
}


@pytest.fixture
def diagnostico(monkeypatch):
    """Ejecuta node_diagnosticar con el LLM falso y la metadata real del fixture."""
    monkeypatch.setattr(dp, "DECISIONES_NEGATIVOS", {})
    monkeypatch.setattr(dp, "CONTEXTO_DATASET", "")
    monkeypatch.delenv("DATAPULSE_CONTEXTO", raising=False)

    def correr(respuesta=None, decisiones_usuario=None):
        if decisiones_usuario is not None:
            monkeypatch.setattr(dp, "DECISIONES_NEGATIVOS", decisiones_usuario)
        llm = _llm(respuesta or json.loads(json.dumps(DIAG_LLM)))
        monkeypatch.setattr(dp, "get_llm", lambda: llm)
        meta = dp.serialize_metadata(_df(), "medidas.csv")
        out = dp.node_diagnosticar({"metadata": meta})
        out["metadata"] = meta
        out["prompt"] = llm.prompts[0]
        return out

    return correr


def _accion(out, col):
    grafo = dp.node_consultar_grafo({"diagnostico": out["diagnostico"],
                                     "metadata": out["metadata"]})["acciones_grafo"]
    return [a["accion"] for a in grafo[col]["acciones"]]


# ── Decisión del LLM ────────────────────────────────────────────────────────

def test_temperatura_validos_sin_penalizacion(diagnostico):
    out = diagnostico()
    d = out["decisiones_negativos"]["temperatura"]
    assert d == {"decision": "validos", "fuente": "llm",
                 "razon": "Las temperaturas bajo cero son físicamente posibles."}
    # "validos" gana a la etiqueta contradictoria del LLM
    assert "valores_negativos_inesperados" not in out["diagnostico"]["temperatura"]["problemas"]
    assert "set_nan_y_flag" not in _accion(out, "temperatura")

    neg = qc.check_negative_values(_df(), out["decisiones_negativos"])["temperatura"]
    assert neg["decision"] == "validos" and neg["penalizado"] is False
    assert neg["discrepancia"] is None      # la lista tampoco la penaliza


def test_edad_invalidos_y_set_nan_y_flag(diagnostico):
    out = diagnostico()
    assert out["decisiones_negativos"]["edad"]["decision"] == "invalidos"
    problemas = out["diagnostico"]["edad"]["problemas"]
    assert problemas == ["valores_negativos_inesperados"]     # "ninguno" se reemplaza
    assert "set_nan_y_flag" in _accion(out, "edad")
    assert qc.check_negative_values(_df(), out["decisiones_negativos"])["edad"]["penalizado"] is True


def test_nombre_critico_incierto_y_flag_sin_modificar(diagnostico):
    out = diagnostico()
    assert out["decisiones_negativos"]["x7_val"]["decision"] == "incierto"
    assert "negativos_inciertos" in out["diagnostico"]["x7_val"]["problemas"]
    assert _accion(out, "x7_val") == ["flag_sin_modificar"]
    neg = qc.check_negative_values(_df(), out["decisiones_negativos"])["x7_val"]
    assert neg["decision"] == "incierto" and neg["penalizado"] is False   # se reporta


def test_respuesta_sin_el_campo_es_incierto_con_aviso(diagnostico, capsys):
    respuesta = json.loads(json.dumps(DIAG_LLM))
    del respuesta["edad"]["negativos"]
    respuesta["x7_val"]["negativos"] = "quizas"
    out = diagnostico(respuesta)
    for col in ("edad", "x7_val"):
        assert out["decisiones_negativos"][col]["decision"] == "incierto"
        assert "negativos_inciertos" in out["diagnostico"][col]["problemas"]
    salida = capsys.readouterr().out
    assert "'edad' tiene negativos" in salida and "'x7_val' tiene negativos" in salida


def test_columnas_sin_negativos_no_reciben_decision(diagnostico):
    out = diagnostico()
    assert set(out["decisiones_negativos"]) == {"temperatura", "edad", "x7_val"}
    assert "negativos" not in out["diagnostico"]["ciudad"]


# ── Decisiones y contexto del usuario ───────────────────────────────────────

def test_decisiones_del_usuario_reemplazan_al_llm(diagnostico, capsys):
    out = diagnostico(decisiones_usuario={"x7_val": "invalidos", "temperatura": "Válidos",
                                          "edad": "tal vez"})
    x7 = out["decisiones_negativos"]["x7_val"]
    assert (x7["decision"], x7["fuente"]) == ("invalidos", "usuario")
    assert "'incierto'" in x7["razon"]                  # deja constancia de lo que dijo el LLM
    assert "valores_negativos_inesperados" in out["diagnostico"]["x7_val"]["problemas"]
    assert "negativos_inciertos" not in out["diagnostico"]["x7_val"]["problemas"]
    assert "set_nan_y_flag" in _accion(out, "x7_val")
    assert out["decisiones_negativos"]["temperatura"]["fuente"] == "usuario"
    # Valor inválido del usuario: se ignora con aviso y manda el LLM
    assert out["decisiones_negativos"]["edad"]["fuente"] == "llm"
    assert "DECISIONES_NEGATIVOS['edad']" in capsys.readouterr().out


def test_contexto_del_dataset_en_el_prompt(diagnostico, monkeypatch):
    assert "No indicado." in diagnostico()["prompt"]
    monkeypatch.setenv("DATAPULSE_CONTEXTO", "Estaciones meteorológicas andinas")
    assert "Estaciones meteorológicas andinas" in diagnostico()["prompt"]
    monkeypatch.setattr(dp, "CONTEXTO_DATASET", "Ventas de una tienda")
    prompt = diagnostico()["prompt"]
    assert "Ventas de una tienda" in prompt and "andinas" not in prompt


# ── Analizador ──────────────────────────────────────────────────────────────

def test_analizador_sin_diagnostico_usa_la_lista():
    neg = qc.check_negative_values(_df())
    assert neg["edad"] == {"count": 5, "pct": 25.0, "penalizado": True}
    assert neg["temperatura"]["penalizado"] is False and neg["x7_val"]["penalizado"] is False
    # La API sigue igual: generate_report sin decisiones
    assert "decision" not in qc.generate_report_df(_df())["negative_values"]["edad"]


def test_analizador_con_diagnostico_cambia_el_score():
    base = qc.generate_report_df(_df())["quality_score"]
    # El usuario/LLM dice que las edades negativas son válidas y x7 inválidos
    con = qc.generate_report_df(_df(), decisiones_negativos={
        "edad": {"decision": "validos", "fuente": "usuario"}, "x7_val": "invalidos",
        "temperatura": "validos"})
    assert con["negative_values"]["edad"]["penalizado"] is False
    assert con["negative_values"]["x7_val"]["penalizado"] is True
    assert con["quality_score"] == base     # una penalizada en cada caso
    assert con["negative_values"]["edad"]["discrepancia"]


def test_discrepancia_negativos():
    assert "no admite negativos" in qc.discrepancia_negativos("edad", "validos")
    assert "no incluye" in qc.discrepancia_negativos("x7_val", "invalidos")
    assert qc.discrepancia_negativos("edad", "invalidos") is None
    assert qc.discrepancia_negativos("edad", "incierto") is None


# ── Grafo ───────────────────────────────────────────────────────────────────

def test_nodo_negativos_inciertos():
    reglas = [(u, d) for u, v, d in dp.kg.edges(data=True) if v == "Negativos_Inciertos"]
    assert {u for u, _ in reglas} == {"Variable_Numerica_Continua", "Variable_Numerica_Discreta",
                                     "Variable_Financiera"}
    assert all(d["accion"] == "flag_sin_modificar" and d["parametros"]["modifica_valores"] is False
               for _, d in reglas)
    assert dp.PROBLEMA_MAP["negativos_inciertos"] == "Negativos_Inciertos"
    assert dp.kg.number_of_edges() == 142


# ── Trazabilidad: audit_log e informe, nunca error_history ──────────────────

class LLMFlujo:
    """Diagnóstico con decisiones de negativos, plan y un script que no limpia."""

    def invoke(self, mensajes):
        p = mensajes[0].content
        if "planificador técnico" in p:
            acciones = json.loads(p.split("Acciones del grafo (no modificables):\n", 1)[1]
                                  .split("\n\nProblemas sin regla")[0])
            return SimpleNamespace(content=json.dumps(
                {"orden": [a["id"] for a in acciones], "acciones_fallback": [],
                 "brief_tecnico": "B"}))
        if "Escribe el script Python COMPLETO" in p:
            return SimpleNamespace(content=("import sys, shutil\n"
                                            "shutil.copy(sys.argv[1], 'processed_data.csv')\n"))
        return SimpleNamespace(content=json.dumps(DIAG_LLM))


def test_discrepancias_en_informe_y_audit_pero_no_en_error_history(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "OPENHANDS_API_KEY", "")
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "")
    monkeypatch.setattr(dp, "MIN_SCORE_PASS", 101)          # fuerza reintentos con residuales
    monkeypatch.setattr(dp, "DECISIONES_NEGATIVOS", {"edad": "validos"})   # discrepa de la lista
    monkeypatch.setattr(dp, "get_llm", lambda: LLMFlujo())
    datos = tmp_path / "medidas.csv"
    _df().to_csv(datos, index=False)

    final = dp.run_datapulse(str(datos))

    assert final["intentos"] == dp.OH_MAX_INTENTOS_SCORE
    assert [d["columna"] for d in final["discrepancias_negativos"]] == ["edad"]
    historial = " ".join(final["error_history"]).lower()
    assert "discrep" not in historial and "lista de control" not in historial

    informe = final["informe_final"]
    assert informe["decisiones_negativos"]["edad"]["fuente"] == "usuario"
    md = open(os.path.join(informe["ruta"], "informe_final.md"), encoding="utf-8").read()
    assert "## Decisiones sobre negativos" in md
    assert "| temperatura | validos | Las temperaturas bajo cero son físicamente posibles. | llm |" in md
    assert "| x7_val | incierto |" in md
    assert "La lista de control considera que 'edad' no admite negativos" in md

    # Sin commit (score bajo): el audit_log se construye igual para la publicación
    audit = json.loads(dp._build_files_to_commit(final, "ts")[0][1])
    assert audit["decisiones_negativos"]["x7_val"]["decision"] == "incierto"
    assert audit["discrepancias_negativos"][0]["columna"] == "edad"
    assert not glob.glob(str(tmp_path / "outputs" / "*" / "audit_log.json"))


# ── Punto 6: campo "negativos" ausente o contradictorio ─────────────────────

def test_sin_campo_con_etiqueta_es_invalidos(diagnostico, capsys):
    respuesta = json.loads(json.dumps(DIAG_LLM))
    respuesta["x7_val"] = {"tipo_inferido": "numerica_continua",
                           "problemas": ["valores_negativos_inesperados"]}
    out = diagnostico(respuesta)
    d = out["decisiones_negativos"]["x7_val"]
    assert (d["decision"], d["fuente"]) == ("invalidos", "llm")
    assert "Deducido de la etiqueta" in d["razon"]
    assert "set_nan_y_flag" in _accion(out, "x7_val")
    assert "se deduce 'invalidos'" in capsys.readouterr().out


def test_sin_campo_ni_senal_es_incierto(diagnostico, capsys):
    respuesta = json.loads(json.dumps(DIAG_LLM))
    respuesta["x7_val"] = {"tipo_inferido": "numerica_continua", "problemas": ["nulos_bajos"]}
    out = diagnostico(respuesta)
    assert out["decisiones_negativos"]["x7_val"]["decision"] == "incierto"
    assert "ni ninguna señal" in capsys.readouterr().out


def test_campo_y_etiqueta_contradictorios_manda_el_campo(diagnostico, capsys):
    respuesta = json.loads(json.dumps(DIAG_LLM))
    respuesta["edad"]["problemas"] = ["negativos_inciertos"]        # etiqueta: incierto
    respuesta["edad"]["negativos"] = "invalidos"                     # campo: invalidos
    out = diagnostico(respuesta)
    assert out["decisiones_negativos"]["edad"]["decision"] == "invalidos"
    assert out["diagnostico"]["edad"]["problemas"] == ["valores_negativos_inesperados"]
    salida = capsys.readouterr().out
    assert "contradice la etiqueta 'negativos_inciertos'; manda el campo" in salida
    # Y la contradicción de temperatura (validos + valores_negativos_inesperados) también avisa
    assert "'temperatura' el campo 'negativos' ('validos') contradice" in salida
