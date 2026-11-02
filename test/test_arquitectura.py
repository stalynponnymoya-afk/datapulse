"""
Tests de contrato de la arquitectura:

- El grafo LangGraph tiene exactamente los 7 nodos originales y el router
  exactamente 3 ramas (retry | commit | end_no_commit).
- El vocabulario del diagnóstico sale del Grafo de Conocimiento y las
  acciones del grafo llegan al plan sin cambios, sea cual sea su contenido
  (se usa un grafo mínimo inventado).
"""

import json
import os
import sys
from types import SimpleNamespace

import networkx as nx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402


NODOS = {"ingestar", "diagnosticar", "consultar_grafo", "planificar", "ejecutar",
         "validar_score", "commit"}
SECUENCIA = [("__start__", "ingestar"), ("ingestar", "diagnosticar"),
             ("diagnosticar", "consultar_grafo"), ("consultar_grafo", "planificar"),
             ("planificar", "ejecutar"), ("ejecutar", "validar_score"), ("commit", "__end__")]
RAMAS_ROUTER = {"retry": "ejecutar", "commit": "commit", "end_no_commit": "__end__"}


def test_grafo_langgraph_tiene_exactamente_7_nodos():
    g = dp.build_datapulse_graph().get_graph()
    assert {n for n in g.nodes if not n.startswith("__")} == NODOS
    simples = {(e.source, e.target) for e in g.edges if not e.conditional}
    assert simples == set(SECUENCIA)


def test_router_tiene_exactamente_3_ramas():
    g = dp.build_datapulse_graph().get_graph()
    condicionales = [e for e in g.edges if e.conditional]
    assert all(e.source == "validar_score" for e in condicionales)
    ramas = {(e.data or e.target): e.target for e in condicionales}
    assert ramas == RAMAS_ROUTER
    # commit y end_no_commit terminan en END; retry vuelve a ejecutar
    assert ("commit", "__end__") in {(e.source, e.target) for e in g.edges}


@pytest.mark.parametrize("estado", [
    {}, {"openhands_status": "finished", "score_despues": 95, "intentos": 1},
    {"openhands_status": "finished", "score_despues": 10, "intentos": 1},
    {"openhands_status": "finished", "score_despues": 10, "intentos": 9},
    {"openhands_status": "rechazado"},
    {"openhands_status": "sin_validar", "processed_esperado": True, "intentos": 1},
    {"openhands_status": "sin_validar", "processed_esperado": False, "intentos": 1},
] + [{"openhands_status": s, "intentos": n}
     for s in ("error", "timeout", "stuck", "stopped") for n in (1, 2)])
def test_router_solo_devuelve_esas_3_ramas(estado):
    assert dp.router_validar_score(estado) in RAMAS_ROUTER


def _grafo_minimo():
    kg = nx.MultiDiGraph()
    kg.add_edge("Tipo_A", "Problema_X", accion="accion_x", confianza=0.9,
                parametros={"p": 1}, justificacion_es="regla X del usuario")
    kg.add_edge("Tipo_A", "Problema_Y", accion="accion_y_debil", confianza=0.5, parametros={})
    kg.add_edge("Tipo_A", "Problema_Y", accion="accion_y_fuerte", confianza=0.95, parametros={})
    kg.add_edge("Esquema_General", "Problema_Global", accion="accion_global", confianza=0.99,
                parametros={})
    return kg


@pytest.fixture
def grafo_usuario(monkeypatch):
    """Reemplaza el grafo y los mapas por los de un usuario hipotético."""
    kg = _grafo_minimo()
    monkeypatch.setattr(dp, "kg", kg)
    monkeypatch.setattr(dp, "TIPO_MAP", {"tipo_a": "Tipo_A"})
    monkeypatch.setattr(dp, "PROBLEMA_MAP", {"x": "Problema_X"})
    return kg


class LLMGuion:
    """LLM falso: responde según el tipo de prompt y guarda lo que recibió."""

    def __init__(self, diagnostico, orden):
        self.diagnostico, self.orden, self.prompts = diagnostico, orden, []

    def invoke(self, mensajes):
        prompt = mensajes[0].content
        self.prompts.append(prompt)
        if "planificador técnico" in prompt:
            return SimpleNamespace(content=json.dumps(
                {"orden": self.orden, "brief_tecnico": "Brief del LLM."}))
        return SimpleNamespace(content=json.dumps(self.diagnostico))


def _pipeline(monkeypatch, diagnostico, orden):
    llm = LLMGuion(diagnostico, orden)
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    meta = {"filas": 3, "columnas_info": {c: {} for c in diagnostico}}
    diag = dp.node_diagnosticar({"metadata": meta})["diagnostico"]
    grafo = dp.node_consultar_grafo({"diagnostico": diag, "metadata": meta})["acciones_grafo"]
    plan = dp.node_planificar({"acciones_grafo": grafo})
    return llm, grafo, plan


def test_vocabulario_sale_del_grafo(grafo_usuario):
    vocab = dp.vocabulario_grafo()
    assert vocab["tipos"] == {"Tipo_A": ["tipo_a"]}
    assert vocab["problemas"] == {
        "Problema_Global": ["problema_global"],
        "Problema_X": ["x", "problema_x"],
        "Problema_Y": ["problema_y"],          # sin alias: se ofrece su nombre
    }
    prompt = dp.construir_prompt_diagnostico({})
    for etiqueta in ("tipo_a", "x", "problema_y", "problema_global"):
        assert etiqueta in prompt
    # Lo que no está en el grafo del usuario no se ofrece como etiqueta
    lista = prompt.split("- problemas:")[1].split("Si detectas un problema real")[0]
    assert "nulos_disfrazados" not in lista and "Valores_Centinela" not in lista


def test_plan_contiene_exactamente_las_acciones_del_grafo(grafo_usuario, monkeypatch):
    diagnostico = {
        "col1": {"tipo_inferido": "tipo_a", "problemas": ["x", "problema_y", "sin_regla_inventada"]},
        "col2": {"tipo_inferido": "tipo_a", "problemas": ["problema_global"]},
    }
    # El LLM propone un orden e intenta colar una acción que no existe
    llm, grafo, plan = _pipeline(monkeypatch, diagnostico, ["a3", "a99", "a1"])

    acciones = [(a["columna"], a["accion"], a["parametros"], a["fuente_decision"])
                for a in plan["plan_acciones"]]
    assert acciones == [
        ("col2", "accion_global", {}, "knowledge_graph"),     # a3, primero según el LLM
        ("col1", "accion_x", {"p": 1}, "knowledge_graph"),    # a1
        ("col1", "accion_y_fuerte", {}, "knowledge_graph"),   # a2 no ordenada: al final
    ]
    # El problema sin regla se informa, no se limpia
    assert plan["sin_regla"] == [{"columna": "col1", "problema": "sin_regla_inventada"}]
    brief = plan["brief_tecnico"]
    assert brief.startswith("Brief del LLM.")
    assert "ACCIONES DEL GRAFO" in brief and "accion_y_fuerte" in brief
    assert "PROBLEMAS SIN REGLA" in brief and "sin_regla_inventada" in brief
    assert "accion_y_debil" not in brief      # gana la regla de mayor confianza


def test_editar_el_grafo_cambia_el_plan(grafo_usuario, monkeypatch):
    diagnostico = {"col1": {"tipo_inferido": "tipo_a", "problemas": ["x"]}}
    _, _, antes = _pipeline(monkeypatch, diagnostico, ["a1"])
    assert antes["plan_acciones"][0]["accion"] == "accion_x"

    # El usuario edita su regla y agrega un tipo nuevo sin tocar los mapas
    grafo_usuario["Tipo_A"]["Problema_X"][0]["accion"] = "accion_x_editada"
    grafo_usuario.add_edge("Tipo_Nuevo", "Problema_X", accion="accion_del_tipo_nuevo",
                           confianza=0.8, parametros={})
    diagnostico = {"col1": {"tipo_inferido": "tipo_a", "problemas": ["x"]},
                   "col2": {"tipo_inferido": "Tipo_Nuevo", "problemas": ["Problema_X"]}}
    _, _, despues = _pipeline(monkeypatch, diagnostico, ["a1", "a2"])
    assert [a["accion"] for a in despues["plan_acciones"]] == [
        "accion_x_editada", "accion_del_tipo_nuevo"]
    assert "tipo_nuevo" in dp.construir_prompt_diagnostico({})


def test_planificador_sin_respuesta_valida_usa_el_grafo(grafo_usuario, monkeypatch):
    class LLMRoto:
        def invoke(self, mensajes):
            if "planificador técnico" in mensajes[0].content:
                return SimpleNamespace(content="no es json")
            return SimpleNamespace(content=json.dumps(
                {"col1": {"tipo_inferido": "tipo_a", "problemas": ["x"]}}))

    monkeypatch.setattr(dp, "get_llm", lambda: LLMRoto())
    meta = {"columnas_info": {"col1": {}}}
    diag = dp.node_diagnosticar({"metadata": meta})["diagnostico"]
    grafo = dp.node_consultar_grafo({"diagnostico": diag})["acciones_grafo"]
    plan = dp.node_planificar({"acciones_grafo": grafo})
    assert [a["accion"] for a in plan["plan_acciones"]] == ["accion_x"]
    assert "ACCIONES DEL GRAFO" in plan["brief_tecnico"]


def test_sin_acciones_no_llama_al_llm(grafo_usuario, monkeypatch):
    def _no_llamar():
        raise AssertionError("no debería llamarse al LLM")

    monkeypatch.setattr(dp, "get_llm", _no_llamar)
    plan = dp.node_planificar({"acciones_grafo": {
        "col1": {"acciones": [], "problemas_fallback": ["algo_raro", "ninguno"]}}})
    assert plan["plan_acciones"] == []
    assert plan["sin_regla"] == [{"columna": "col1", "problema": "algo_raro"}]


def test_grafo_real_todas_las_reglas_alcanzables():
    """Con el vocabulario generado, cada regla del grafo real es alcanzable."""
    vocab = dp.vocabulario_grafo()
    for origen, destino in dp.kg.edges():
        etiqueta_p = vocab["problemas"][destino][0]
        if origen == dp.NODO_ESQUEMA_GENERAL:
            assert dp._map_tipo_problema("texto_libre", etiqueta_p)[1] == destino
            continue
        etiqueta_t = vocab["tipos"][origen][0]
        assert dp._map_tipo_problema(etiqueta_t, etiqueta_p) == (origen, destino)
