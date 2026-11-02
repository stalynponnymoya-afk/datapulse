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
        "Variable_Categorica_Nominal", "Variable_Categorica_Ordinal", "Texto_General",
        "Variable_Temporal_Timestamp",
    }
    # Ya no apunta al KNN imputer de Nulos_Criticos
    res = _consultar("numerica_continua", "nulos_disfrazados")
    assert "knn" not in res["acciones"][0]["accion"].lower()


def test_negativos_financieros_flag_y_nan():
    res = _consultar("financiero", "valores_negativos_inesperados")
    accion = res["acciones"][0]
    assert accion["accion"] == "flag_y_nan"
    assert "clip_min" not in accion["parametros"]


def test_la_accion_del_grafo_no_se_modifica_en_codigo():
    """Sin reglas fuera del grafo: la skewness no cambia la acción elegida."""
    for skew in (2.7, -1.5, 0.3, None):
        res = _consultar("numerica_continua_sesgada", "nulos_bajos", skewness=skew)
        accion = res["acciones"][0]
        datos = max(dp.kg["Variable_Numerica_Continua"]["Nulos_Bajos"].values(),
                    key=lambda d: d["confianza"])
        assert accion["accion"] == datos["accion"]
        assert accion["parametros"] == datos.get("parametros", {})
        assert "ajuste" not in accion


def test_conteo_de_reglas():
    kg = dp.construir_grafo_conocimiento()
    base = kg.number_of_edges()
    kg = dp.agregar_reglas_dominio(kg)
    assert (base, kg.number_of_edges() - base) == (113, 26)
    assert dp.kg.number_of_edges() == 139
    confianzas = [d["confianza"] for _, _, d in dp.kg.edges(data=True)]
    assert min(confianzas) == 0.78 and max(confianzas) == 1.0


def test_centinelas_en_texto_y_categorias_replace_to_na():
    lista_cadenas_vacias = dp.kg["Texto_General"]["Cadenas_Vacias_Representan_Nulos"][0][
        "parametros"]["to_replace"]
    assert lista_cadenas_vacias == dp.CADENAS_NULAS
    for tipo in ("categorica_nominal", "categorica_ordinal", "texto_libre"):
        res = _consultar(tipo, "nulos_disfrazados")
        assert not res["requiere_llm_fallback"], tipo
        accion = res["acciones"][0]
        assert accion["accion"] == "replace_to_na"
        # Reutiliza la lista de Cadenas_Vacias_Representan_Nulos
        assert accion["parametros"]["to_replace"] == dp.CADENAS_NULAS
        assert accion["parametros"]["value"] is None


def test_fechas_centinela_vacias_por_defecto_y_sin_imputar(monkeypatch):
    monkeypatch.delenv("DATAPULSE_FECHAS_CENTINELA", raising=False)
    res = _consultar("temporal_fecha_puntual", "nulos_disfrazados")
    assert not res["requiere_llm_fallback"]
    accion = res["acciones"][0]
    assert accion["accion"] == "marcar_fecha_centinela"
    assert accion["parametros"]["fechas_centinela"] == []
    assert accion["parametros"]["imputar"] is False
    assert "fillna" not in accion["accion"] and "interpol" not in accion["accion"]
    # La arista del grafo tampoco imputa
    datos = dp.kg["Variable_Temporal_Timestamp"]["Valores_Centinela"][0]
    assert datos["parametros"]["imputar"] is False


def test_fechas_centinela_configurables(monkeypatch, capsys):
    monkeypatch.setenv(
        "DATAPULSE_FECHAS_CENTINELA", "1900-01-01, 9999-12-31,no-es-fecha,,1970-01-01"
    )
    accion = _consultar("datetime", "nulos_disfrazados")["acciones"][0]
    assert accion["parametros"]["fechas_centinela"] == ["1900-01-01", "9999-12-31", "1970-01-01"]
    assert accion["parametros"]["imputar"] is False
    assert "no-es-fecha" in capsys.readouterr().out


def test_prompts_sin_reglas_de_limpieza():
    """Las reglas de limpieza solo viven en el grafo; los prompts no deciden acciones."""
    plan = dp.PLANIFICAR_PROMPT.lower()
    for decision in ("winsoriz", "por nan", "recortar", "drop_duplicates", "fillna"):
        assert decision not in plan, decision
    assert "no cambies ni añadas acciones" in plan
    # El diagnóstico sí describe cómo reconocer centinelas (es diagnóstico, no limpieza)
    diag = dp.construir_prompt_diagnostico({})
    assert '"N/A"' in diag and "1900-01-01" in diag
