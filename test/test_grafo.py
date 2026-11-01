"""
Tests de coherencia entre el grafo de conocimiento y los prompts (Bloque G).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402


def _consultar(tipo, problema, skewness=None, col="col"):
    estado = {
        "diagnostico": {col: {"tipo_inferido": tipo, "problemas": [problema],
                              "problema_principal": problema}},
        "metadata": {"columnas_info": {col: {"skewness": skewness}}},
    }
    return dp.node_consultar_grafo(estado)["acciones_grafo"][col]


def test_nulos_disfrazados_reemplaza_centinela_por_nan():
    assert dp.PROBLEMA_MAP["nulos_disfrazados"] == "Valores_Centinela"
    for tipo in ("numerica_continua", "numerica_discreta", "financiero"):
        res = _consultar(tipo, "nulos_disfrazados")
        assert not res["requiere_llm_fallback"], tipo
        accion = res["acciones"][0]
        assert accion["accion"] == "reemplazar_centinela_por_nan"
        assert accion["confianza"] == 0.92
        assert accion["parametros"] == {"centinelas": [-999, -9999, 999, 9999, -1]}


def test_aristas_centinela_en_el_grafo():
    origenes = {u for u, v in dp.kg.in_edges("Valores_Centinela")}
    assert origenes == {
        "Variable_Numerica_Continua", "Variable_Numerica_Discreta", "Variable_Financiera",
    }
    # Ya no apunta al KNN imputer de Nulos_Criticos
    res = _consultar("numerica_continua", "nulos_disfrazados")
    assert "knn" not in res["acciones"][0]["accion"].lower()


def test_negativos_financieros_flag_y_nan():
    res = _consultar("financiero", "valores_negativos_inesperados")
    accion = res["acciones"][0]
    assert accion["accion"] == "flag_y_nan"
    assert "clip_min" not in accion["parametros"]


def test_sesgada_con_nulos_bajos_usa_mediana():
    # La etiqueta del LLM dice "simétrica", pero la metadata muestra sesgo
    for etiqueta in ("numerica_continua_sesgada", "numerica_continua_simetrica"):
        res = _consultar(etiqueta, "nulos_bajos", skewness=2.7)
        accion = res["acciones"][0]
        assert accion["accion"] == "fillna_median", etiqueta
        assert accion["parametros"]["strategy"] == "median"
        assert "skewness" in accion["justificacion_es"]
        assert accion["ajuste"] == "skewness"

    # Sesgo negativo también cuenta
    assert _consultar("numerica_continua", "nulos_bajos", skewness=-1.5)["acciones"][0][
        "accion"] == "fillna_median"


def test_simetrica_con_nulos_bajos_mantiene_media():
    for skew in (0.3, -1.0, None, "no-numero"):
        res = _consultar("numerica_continua_sesgada", "nulos_bajos", skewness=skew)
        assert res["acciones"][0]["accion"] == "fillna_mean", skew


def test_sin_metadata_no_rompe():
    out = dp.node_consultar_grafo({"diagnostico": {
        "x": {"tipo_inferido": "numerica_continua", "problemas": ["nulos_bajos"]}}})
    assert out["acciones_grafo"]["x"]["acciones"][0]["accion"] == "fillna_mean"


def test_conteo_de_reglas():
    kg = dp.construir_grafo_conocimiento()
    base = kg.number_of_edges()
    kg = dp.agregar_reglas_dominio(kg)
    assert (base, kg.number_of_edges() - base) == (109, 26)
    confianzas = [d["confianza"] for _, _, d in dp.kg.edges(data=True)]
    assert min(confianzas) == 0.78 and max(confianzas) == 1.0
