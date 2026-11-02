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


def test_negativos_financieros_set_nan_y_flag():
    """Punto 5: la financiera deja flag_y_nan (no cubría montos positivos absurdos)."""
    res = _consultar("financiero", "valores_negativos_inesperados")
    accion = res["acciones"][0]
    assert accion["accion"] == "set_nan_y_flag"
    assert accion["parametros"]["aplica_a"] == ["negativos_inesperados", "fuera_de_rango_logico"]
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
    assert (base, kg.number_of_edges() - base) == (116, 26)
    assert dp.kg.number_of_edges() == 142
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


def test_prompts_no_contradicen_el_grafo_en_fechas():
    assert "NUNCA se reemplazan ni se imputan" in dp.PLANIFICAR_PROMPT
    prompt = dp.construir_prompt_diagnostico({})
    assert '"N/A"' in prompt and "1900-01-01" in prompt


def test_prompt_del_planificador_restaura_reglas_c_d_e_y_respaldo():
    plan = dp.PLANIFICAR_PROMPT
    assert "REEMPLAZAR ese valor por NaN" in plan                    # C: centinelas → NaN
    assert "FLAG + reemplazar por NaN" in plan                       # D: negativos → flag + NaN
    assert "1. drop_duplicates (primero siempre)" in plan            # E: prioridad
    assert "Máx. confianza: 0.70" in plan and '"llm_fallback"' in plan  # B: respaldo del LLM
    assert "NO se modifican" in plan


def test_rango_invalido_set_nan_y_flag_sin_recorte():
    """Rango_Invalido: nunca se recorta (recortar inventa un valor)."""
    reglas = [(u, d) for u, v, d in dp.kg.edges(data=True) if v == "Rango_Invalido"]
    assert reglas
    assert all(d["accion"] != "clip_to_valid_range" for _, d in reglas)

    esperados = {"aplica_a": ["negativos_inesperados", "fuera_de_rango_logico"],
                 "flag_suffix": "_rango_invalido"}
    por_origen = {u: d for u, d in reglas}
    for origen, confianza in (("Variable_Numerica_Continua", 0.83),
                              ("Variable_Numerica_Discreta", 0.85),
                              ("Variable_Financiera", 0.85)):
        d = por_origen[origen]
        assert d["accion"] == "set_nan_y_flag"
        assert d["parametros"] == esperados
        assert d["confianza"] == confianza
        assert "NaN" in d["justificacion_es"] and "inventa" in d["justificacion_es"]
    # Ninguna regla de Rango_Invalido conserva flag_y_nan (no cubría montos positivos absurdos)
    assert all(d["accion"] == "set_nan_y_flag" for _, d in reglas)
    assert dp.kg.number_of_edges() == 142


def test_rango_invalido_cubre_negativos_y_fuera_de_rango():
    """Las dos etiquetas que llegan a Rango_Invalido producen la misma acción."""
    for tipo in ("numerica_continua", "numerica_discreta", "financiero"):
        for etiqueta in ("valores_negativos_inesperados", "rango_invalido"):
            accion = _consultar(tipo, etiqueta)["acciones"][0]
            assert accion["accion"] == "set_nan_y_flag", (tipo, etiqueta)
            assert set(accion["parametros"]["aplica_a"]) == {
                "negativos_inesperados", "fuera_de_rango_logico"}


# ── Punto 5: la decisión de negativos limita las acciones ───────────────────

def _consultar_con_decision(tipo, problemas, decision, col="col"):
    diag = {col: {"tipo_inferido": tipo, "problemas": problemas, "negativos": decision}}
    return dp.node_consultar_grafo({"diagnostico": diag})["acciones_grafo"][col]["acciones"]


def test_edad_incierto_con_rango_invalido_solo_fuera_de_rango():
    acciones = _consultar_con_decision("numerica_discreta",
                                       ["negativos_inciertos", "rango_invalido"], "incierto")
    rango = next(a for a in acciones if a["problema"] == "rango_invalido")
    assert rango["accion"] == "set_nan_y_flag"
    assert rango["parametros"]["aplica_a"] == ["fuera_de_rango_logico"]
    assert "los negativos de esta columna son 'incierto'" in rango["justificacion_es"]
    assert rango["ajuste"] == "negativos"
    inciertos = next(a for a in acciones if a["problema"] == "negativos_inciertos")
    assert inciertos["accion"] == "flag_sin_modificar"


def test_monto_financiero_validos_con_rango_invalido_solo_fuera_de_rango():
    acciones = _consultar_con_decision("financiero", ["rango_invalido"], "validos")
    assert acciones[0]["accion"] == "set_nan_y_flag"
    assert acciones[0]["parametros"]["aplica_a"] == ["fuera_de_rango_logico"]
    assert "'validos'" in acciones[0]["justificacion_es"]


def test_invalidos_mantiene_negativos_y_fuera_de_rango():
    acciones = _consultar_con_decision("numerica_discreta", ["valores_negativos_inesperados"],
                                       "invalidos")
    assert acciones[0]["parametros"]["aplica_a"] == ["negativos_inesperados",
                                                      "fuera_de_rango_logico"]
    assert "ajuste" not in acciones[0]


def test_centinelas_negativos_no_se_tocan_si_los_negativos_son_validos():
    acciones = _consultar_con_decision("numerica_continua", ["nulos_disfrazados"], "validos")
    assert acciones[0]["accion"] == "reemplazar_centinela_por_nan"
    assert acciones[0]["parametros"]["centinelas"] == [999, 9999]
    # Con "invalidos" la lista queda completa
    acciones = _consultar_con_decision("numerica_continua", ["nulos_disfrazados"], "invalidos")
    assert acciones[0]["parametros"]["centinelas"] == [-999, -9999, 999, 9999, -1]
    # El grafo no se modifica
    datos = dp.kg["Variable_Numerica_Continua"]["Valores_Centinela"][0]
    assert datos["parametros"]["centinelas"] == [-999, -9999, 999, 9999, -1]


def test_brief_prohibe_modificar_negativos_no_invalidos():
    texto = dp._seccion_obligatoria([], [], {
        "temperatura": {"decision": "validos"}, "x7": {"decision": "incierto"},
        "edad": {"decision": "invalidos"}})
    assert "NEGATIVOS QUE NO SE MODIFICAN" in texto
    assert '"temperatura": "validos"' in texto and '"x7": "incierto"' in texto
    assert '"edad"' not in texto
