"""
Tests de la normalización del diagnóstico del LLM (respuestas mal formadas).
"""

import json
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402

COLUMNAS = ["Row", "Edad", "Ciudad"]
DIAG = {"tipo_inferido": "numerica_discreta", "problemas": ["nulos_bajos"],
        "problema_principal": "nulos_bajos"}


def test_respuesta_correcta_sin_cambios():
    crudo = {c: dict(DIAG) for c in COLUMNAS}
    assert dp._normalizar_diagnostico(crudo, COLUMNAS) == crudo


def test_clave_de_resumen_se_ignora(capsys):
    """El caso de Colab: columnas + una clave de texto con un resumen."""
    crudo = {c: dict(DIAG) for c in COLUMNAS}
    crudo["resumen_general"] = "Dataset de ventas con problemas de formato"
    res = dp._normalizar_diagnostico(crudo, COLUMNAS)
    assert set(res) == set(COLUMNAS)
    assert "resumen_general" in capsys.readouterr().out


@pytest.mark.parametrize("envoltorio", ["diagnostico", "columnas", "otra_cosa"])
def test_respuesta_envuelta(envoltorio):
    crudo = {envoltorio: {c: dict(DIAG) for c in COLUMNAS}, "notas": "x"}
    assert set(dp._normalizar_diagnostico(crudo, COLUMNAS)) == set(COLUMNAS)


def test_respuesta_en_lista():
    crudo = [{"columna": c, **DIAG} for c in COLUMNAS] + ["texto suelto"]
    res = dp._normalizar_diagnostico(crudo, COLUMNAS)
    assert set(res) == set(COLUMNAS)
    assert res["Edad"]["tipo_inferido"] == "numerica_discreta"


def test_columna_con_texto_y_columnas_faltantes(capsys):
    crudo = {"Edad": dict(DIAG), "Row": "sin problemas", "Inventada": dict(DIAG)}
    res = dp._normalizar_diagnostico(crudo, COLUMNAS)
    assert list(res) == ["Edad"]
    salida = capsys.readouterr().out
    assert "Inventada" in salida and "Row" in salida and "Ciudad" in salida


def test_sin_ninguna_columna_valida():
    with pytest.raises(ValueError, match="ninguna columna"):
        dp._normalizar_diagnostico({"resumen": "nada útil"}, COLUMNAS)
    with pytest.raises(ValueError):
        dp._normalizar_diagnostico("texto", COLUMNAS)


def test_columnas_no_string():
    crudo = {"0": dict(DIAG), "1.5": dict(DIAG)}
    assert set(dp._normalizar_diagnostico(crudo, [0, 1.5])) == {"0", "1.5"}


def test_consultar_grafo_no_se_rompe_con_entradas_mal_formadas(capsys):
    out = dp.node_consultar_grafo({"diagnostico": {
        "Edad": {"tipo_inferido": "numerica_continua", "problemas": ["nulos_bajos"]},
        "resumen_general": "texto",
    }})
    assert list(out["acciones_grafo"]) == ["Edad"]
    assert "resumen_general" in capsys.readouterr().out


def test_node_diagnosticar_normaliza_la_respuesta_del_llm(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    respuesta = {c: dict(DIAG) for c in COLUMNAS}
    respuesta["resumen_general"] = "texto"
    llm = SimpleNamespace(invoke=lambda m: SimpleNamespace(content=json.dumps(respuesta)))
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    meta = {"filas": 3, "columnas_info": {c: {} for c in COLUMNAS}}
    diag = dp.node_diagnosticar({"metadata": meta})["diagnostico"]
    assert set(diag) == set(COLUMNAS)
    # El grafo lo consume sin errores
    dp.node_consultar_grafo({"diagnostico": diag, "metadata": meta})


def test_prompt_pide_solo_claves_de_columna():
    prompt = dp.construir_prompt_diagnostico({})
    assert "No añadas ninguna otra clave" in prompt
