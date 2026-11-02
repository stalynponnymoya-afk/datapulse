"""
Tests de los dominios opcionales del grafo (punto 8): DOMINIOS_ACTIVOS vacía por
defecto, activación por dominio, compatibilidad de agregar_reglas_dominio y la
plantilla documentada.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402


@pytest.fixture
def dominios_restaurados():
    """Restaura el grafo global y DOMINIOS_ACTIVOS al terminar cada test."""
    previo = (list(dp.DOMINIOS_ACTIVOS), dp.kg, dp.KNOWLEDGE_GRAPH, dp._DOMINIOS_APLICADOS)
    yield
    (dp.DOMINIOS_ACTIVOS, dp.kg, dp.KNOWLEDGE_GRAPH, dp._DOMINIOS_APLICADOS) = previo


def _aristas(grafo):
    return {(u, v, d["accion"]) for u, v, d in grafo.edges(data=True)}


def _nodos_de(funcion):
    """Nodos y aristas que una función de dominio agrega sobre el grafo base."""
    base = dp.construir_grafo_conocimiento()
    nodos_base, aristas_base = set(base.nodes()), _aristas(base)
    funcion(base)
    return set(base.nodes()) - nodos_base, _aristas(base) - aristas_base


def test_por_defecto_ningun_dominio():
    assert dp.DOMINIOS_ACTIVOS == []
    assert not any("Clinica" in n or "Ocupacional" in n or "Economica" in n
                   or n.startswith("Variable_Financiera_") for n in dp.kg.nodes())
    vocab = dp.vocabulario_grafo()
    etiquetas = {e for grupo in vocab.values() for lista in grupo.values() for e in lista}
    assert "fecha_clinica" not in etiquetas
    assert "signo_vital" not in etiquetas
    # Un alias de un dominio inactivo no resuelve a un nodo inexistente
    assert dp._map_tipo_problema("fecha_clinica", "fecha_clinica_inconsistente") == (None, None)


def test_registro_de_dominios():
    assert set(dp.DOMINIOS) == {"salud", "finanzas", "economia", "ocupacional"}
    assert dp.DOMINIOS["salud"] is dp.agregar_reglas_salud
    # La plantilla está documentada pero no registrada
    assert dp.agregar_reglas_plantilla not in dp.DOMINIOS.values()
    assert "DOMINIOS[" in dp.agregar_reglas_plantilla.__doc__


def test_cada_dominio_agrega_solo_sus_nodos():
    representativos = {
        "salud": "Variable_Clinica_Fecha", "finanzas": "Variable_Financiera_Tasa",
        "economia": "Variable_Economica_Indicador", "ocupacional": "Exposicion_Limite_Excedido",
    }
    vistos = {nombre: _nodos_de(dp.DOMINIOS[nombre]) for nombre in representativos}
    for nombre, (nodos, aristas) in vistos.items():
        assert aristas, nombre
        assert representativos[nombre] in nodos, nombre
        otros = set(representativos.values()) - {representativos[nombre]}
        assert not nodos & otros, nombre
    # Los nodos nuevos de un dominio no los agrega ningún otro
    for a in vistos:
        for b in vistos:
            if a < b:
                assert not vistos[a][0] & vistos[b][0], (a, b)


def test_agregar_reglas_dominio_compatible_agrega_todos():
    """Sin argumentos agrega todos los dominios registrados, como antes."""
    todos = dp.agregar_reglas_dominio(dp.construir_grafo_conocimiento())
    por_partes = dp.construir_grafo_conocimiento()
    for funcion in dp.DOMINIOS.values():
        funcion(por_partes)
    assert _aristas(todos) == _aristas(por_partes)
    assert todos.number_of_edges() > dp.construir_grafo_conocimiento().number_of_edges()


def test_lista_vacia_no_agrega_nada():
    base = dp.construir_grafo_conocimiento()
    antes = _aristas(base)
    dp.agregar_reglas_dominio(base, [])
    assert _aristas(base) == antes


def test_dominio_desconocido_falla_con_mensaje():
    with pytest.raises(ValueError, match="Dominios desconocidos.*retail_inexistente"):
        dp.agregar_reglas_dominio(dp.construir_grafo_conocimiento(), ["retail_inexistente"])


def test_activar_dominio_en_caliente(dominios_restaurados):
    dp.DOMINIOS_ACTIVOS = ["salud"]
    grafo = dp.aplicar_dominios_activos()
    assert grafo is dp.kg is dp.KNOWLEDGE_GRAPH
    assert "Variable_Clinica_Fecha" in dp.kg
    assert not any("Ocupacional" in n for n in dp.kg.nodes())
    vocab = dp.vocabulario_grafo()
    assert "fecha_clinica" in vocab["tipos"]["Variable_Clinica_Fecha"]
    assert dp._map_tipo_problema("signo_vital", "rango_clinico_invalido") == (
        "Variable_Clinica_Signo_Vital", "Rango_Clinico_Invalido")
    # Sin cambios en DOMINIOS_ACTIVOS no se reconstruye (se conservan ediciones en caliente)
    dp.kg.add_edge("Variable_Clinica_Fecha", "Nulos_Bajos", accion="probar", confianza=0.5,
                   parametros={}, justificacion_es="x", justificacion_en="x")
    assert dp.aplicar_dominios_activos() is grafo
    # Volver a la lista vacía deja el grafo base
    dp.DOMINIOS_ACTIVOS = []
    dp.aplicar_dominios_activos()
    assert _aristas(dp.kg) == _aristas(dp.construir_grafo_conocimiento())


def test_dominio_activo_cambia_la_regla_aplicada(dominios_restaurados):
    estado = {"diagnostico": {"pas": {"tipo_inferido": "signo_vital",
                                      "problemas": ["rango_clinico_invalido"],
                                      "problema_principal": "rango_clinico_invalido"}},
              "metadata": {"columnas_info": {"pas": {}}}}
    sin_dominio = dp.node_consultar_grafo(estado)["acciones_grafo"]["pas"]
    assert sin_dominio["requiere_llm_fallback"]
    dp.DOMINIOS_ACTIVOS = ["salud"]
    dp.aplicar_dominios_activos()
    con_dominio = dp.node_consultar_grafo(estado)["acciones_grafo"]["pas"]
    assert not con_dominio["requiere_llm_fallback"]
    assert con_dominio["acciones"][0]["accion"]
