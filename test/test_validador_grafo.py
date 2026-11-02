"""
Tests del validador del grafo de conocimiento (punto 10). Sustituyen a los
tests de conteo exacto de aristas: editar el grafo (añadir, quitar o cambiar
reglas con el formato correcto) no debe romper ningún test.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402

REGLA_OK = {"accion": "accion_de_prueba", "confianza": 0.8, "parametros": {},
            "justificacion_es": "Por qué.", "justificacion_en": "Why."}


def _grafo_con(**cambios):
    """Grafo base con una regla de prueba; cambios=None quita el atributo."""
    kg = dp.construir_grafo_conocimiento()
    datos = {**REGLA_OK, **cambios}
    datos = {k: v for k, v in datos.items() if v is not None}
    kg.add_edge("Variable_Numerica_Continua", "Problema_De_Prueba", **datos)
    return kg


def _error(kg):
    with pytest.raises(dp.GrafoInvalidoError) as exc:
        dp.validar_grafo(kg)
    return str(exc.value)


def test_grafo_por_defecto_es_valido():
    assert dp.validar_grafo(dp.kg) is dp.kg
    assert dp.errores_grafo(dp.agregar_reglas_dominio(dp.construir_grafo_conocimiento())) == []


def test_cada_dominio_y_la_plantilla_son_validos():
    for nombre, funcion in dp.DOMINIOS.items():
        assert dp.errores_grafo(funcion(dp.construir_grafo_conocimiento())) == [], nombre
    assert dp.errores_grafo(dp.agregar_reglas_plantilla(dp.construir_grafo_conocimiento())) == []


def test_editar_el_grafo_con_formato_correcto_no_rompe():
    kg = _grafo_con(modo="bi")
    kg.remove_edge("Variable_Numerica_Continua", "Nulos_Bajos")   # quitar una regla existente
    assert dp.validar_grafo(kg) is kg


@pytest.mark.parametrize("cambios,atributo", [
    ({"accion": None}, "'accion' debe ser un texto no vacío"),
    ({"accion": "  "}, "'accion' debe ser un texto no vacío"),
    ({"confianza": None}, "'confianza' debe ser un número entre 0 y 1"),
    ({"confianza": 1.5}, "'confianza' debe ser un número entre 0 y 1 (valor: 1.5)"),
    ({"confianza": -0.1}, "'confianza' debe ser un número entre 0 y 1"),
    ({"confianza": "0.8"}, "'confianza' debe ser un número entre 0 y 1 (valor: '0.8')"),
    ({"confianza": True}, "'confianza' debe ser un número entre 0 y 1"),
    ({"parametros": None}, "'parametros' debe ser un dict"),
    ({"parametros": ["a"]}, "'parametros' debe ser un dict"),
    ({"justificacion_es": ""}, "'justificacion_es' debe ser un texto no vacío"),
    ({"justificacion_es": None}, "'justificacion_es' debe ser un texto no vacío"),
    ({"justificacion_en": None}, "'justificacion_en' debe ser un texto no vacío"),
    ({"modo": "dashboard"}, "'modo' debe ser \"bi\" o \"ml\""),
    ({"modo": None}, None),
])
def test_atributo_invalido_indica_arista_y_atributo(cambios, atributo):
    kg = _grafo_con(**cambios)
    if atributo is None:
        assert dp.validar_grafo(kg) is kg      # sin modo es válido (vale en ambos modos)
        return
    mensaje = _error(kg)
    assert "Variable_Numerica_Continua → Problema_De_Prueba" in mensaje
    assert atributo in mensaje


def test_nodo_sin_aristas():
    kg = dp.construir_grafo_conocimiento()
    kg.add_node("Problema_Huerfano", node_type="problema")
    mensaje = _error(kg)
    assert "nodo 'Problema_Huerfano': no tiene ninguna arista" in mensaje


def test_reporta_todos_los_errores_juntos():
    kg = _grafo_con(confianza=2, justificacion_en="")
    kg.add_node("Otro_Huerfano")
    mensaje = _error(kg)
    assert "(3 error(es))" in mensaje
    assert "'confianza'" in mensaje and "'justificacion_en'" in mensaje and "Otro_Huerfano" in mensaje


def test_dominio_mal_formado_falla_al_agregarlo(monkeypatch):
    def agregar_reglas_roto(kg):
        kg.add_edge("Variable_Rota", "Problema_Roto", accion="algo", confianza=0.9,
                    parametros={}, justificacion_es="x")          # falta justificacion_en
        return kg

    monkeypatch.setitem(dp.DOMINIOS, "roto", agregar_reglas_roto)
    with pytest.raises(dp.GrafoInvalidoError, match="Variable_Rota → Problema_Roto.*justificacion_en"):
        dp.agregar_reglas_dominio(dp.construir_grafo_conocimiento(), ["roto"])


def test_run_datapulse_valida_ediciones_en_caliente(tmp_path, monkeypatch):
    """Una regla mal añadida a dp.kg en Colab falla antes de llamar al LLM."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "get_llm", lambda: pytest.fail("no debe llamar al LLM"))
    monkeypatch.setattr(dp, "kg", _grafo_con(confianza=95))
    datos = tmp_path / "datos.csv"
    datos.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    with pytest.raises(dp.GrafoInvalidoError, match="'confianza'.*95"):
        dp.run_datapulse(str(datos))
