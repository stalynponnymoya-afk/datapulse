"""
Tests del modo de salida (punto 9): atributo opcional modo="bi"|"ml" en las
reglas, MODO_SALIDA (por defecto "bi"), problemas "omitido_por_modo" que no van
al fallback del LLM y planificador que no propone transformaciones de modelos
en modo "bi". Sin red: el LLM siempre se simula.
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402

# Acciones que el punto 9 marca explícitamente como propias de modelos
ACCIONES_ML_NOMBRADAS = {
    "pd_get_dummies", "sklearn_one_hot_encode", "sklearn_ordinal_encode", "ordinal_encode",
    "label_encode", "evaluar_drop_o_hash", "pd_map", "evaluar_encoding_por_cardinalidad",
    "mantener_como_int", "sklearn_min_max_scale", "sklearn_standard_scale",
    "sklearn_robust_scale", "sklearn_max_abs_scale", "sklearn_quantile_transform_uniform",
    "sklearn_quantile_transform_normal", "scipy_boxcox", "sklearn_power_transform_boxcox",
    "sklearn_power_transform_yeo_johnson", "sklearn_pca", "scipy_winsorize",
}


def _grafo_completo():
    return dp.agregar_reglas_dominio(dp.construir_grafo_conocimiento())


def _consultar(tipo, problema, monkeypatch=None, modo=None, col="col"):
    if modo is not None:
        monkeypatch.setattr(dp, "MODO_SALIDA", modo)
    estado = {"diagnostico": {col: {"tipo_inferido": tipo, "problemas": [problema],
                                    "problema_principal": problema}},
              "metadata": {"columnas_info": {col: {}}}}
    return dp.node_consultar_grafo(estado)


# ── Atributo modo en el grafo ───────────────────────────────────────────────

def test_modo_por_defecto_es_bi():
    assert dp.MODO_SALIDA == "bi"
    assert dp.modo_salida_vigente() == "bi"


def test_modo_invalido_falla_con_mensaje(monkeypatch):
    monkeypatch.setattr(dp, "MODO_SALIDA", "dashboard")
    with pytest.raises(ValueError, match="MODO_SALIDA debe ser 'bi' o 'ml'"):
        dp.modo_salida_vigente()


def test_modo_solo_toma_bi_o_ml():
    for _, _, d in _grafo_completo().edges(data=True):
        assert d.get("modo") in (None, "bi", "ml"), d["accion"]


def test_acciones_nombradas_son_ml():
    for u, v, d in _grafo_completo().edges(data=True):
        if d["accion"] in ACCIONES_ML_NOMBRADAS:
            assert d.get("modo") == "ml", (u, v, d["accion"])


def test_toda_accion_de_modelos_esta_marcada_ml():
    """Cualquier regla que codifique, escale, transforme o recorte debe llevar modo="ml"."""
    for u, v, d in _grafo_completo().edges(data=True):
        if dp.es_accion_ml(d["accion"], d.get("parametros")):
            assert d.get("modo") == "ml", (u, v, d["accion"])


def test_reglas_bi_de_outlier_severo():
    kg = dp.construir_grafo_conocimiento()
    for tipo in ("Variable_Numerica_Continua", "Variable_Numerica_Discreta"):
        bi = [d for d in kg[tipo]["Outlier_Severo"].values() if d.get("modo") == "bi"]
        assert len(bi) == 1, tipo
        assert bi[0]["accion"] == "flag_outlier_no_remove"
        assert bi[0]["parametros"] == {"strategy": "flag_no_remove", "flag_suffix": "_outlier"}
    financiera = list(kg["Variable_Financiera"]["Outlier_Severo"].values())
    assert financiera[0]["parametros"] == bi[0]["parametros"]


# ── consultar_grafo filtra por modo ─────────────────────────────────────────

@pytest.mark.parametrize("tipo", ["numerica_continua", "numerica_discreta"])
def test_outlier_severo_segun_modo(tipo, monkeypatch):
    bi = _consultar(tipo, "outlier_severo", monkeypatch, "bi")["acciones_grafo"]["col"]
    assert bi["acciones"][0]["accion"] == "flag_outlier_no_remove"
    ml = _consultar(tipo, "outlier_severo", monkeypatch, "ml")["acciones_grafo"]["col"]
    assert ml["acciones"][0]["accion"] == "scipy_winsorize"


@pytest.mark.parametrize("tipo,problema,accion_ml", [
    ("categorica_nominal", "cardinalidad_baja", "pd_get_dummies"),
    ("numerica_continua", "escalas_diferentes", "sklearn_standard_scale"),
    ("numerica_continua", "distribucion_sesgada_positiva", "sklearn_power_transform_boxcox"),
    ("categorica_nominal", "ninguno", "evaluar_encoding_por_cardinalidad"),
    ("categorica_ordinal", "ninguno", "ordinal_encode"),
    ("binaria", "ninguno", "mantener_como_int"),
])
def test_omitido_por_modo_no_va_al_fallback(tipo, problema, accion_ml, monkeypatch):
    out = _consultar(tipo, problema, monkeypatch, "bi")
    col = out["acciones_grafo"]["col"]
    assert col["acciones"] == []
    assert col["requiere_llm_fallback"] is False and col["problemas_fallback"] == []
    assert out["modo_salida"] == "bi"
    (omitido,) = out["omitidos_por_modo"]
    assert omitido["estado"] == "omitido_por_modo" and omitido["columna"] == "col"
    assert accion_ml in omitido["acciones_omitidas"]
    # En modo ml la misma regla sí se aplica
    ml = _consultar(tipo, problema, monkeypatch, "ml")
    assert ml["acciones_grafo"]["col"]["acciones"][0]["accion"] == accion_ml
    assert ml["omitidos_por_modo"] == []


def test_regla_solo_bi_se_omite_en_ml(monkeypatch):
    """Simetría: una regla modo="bi" no se aplica en modo ml."""
    kg = dp.construir_grafo_conocimiento()
    kg.add_edge("Variable_Numerica_Continua", "Problema_Solo_BI", accion="marcar_para_reporte",
                modo="bi", confianza=0.8, parametros={}, justificacion_es="x", justificacion_en="x")
    monkeypatch.setattr(dp, "kg", kg)
    out = _consultar("numerica_continua", "problema_solo_bi", monkeypatch, "ml")
    assert out["omitidos_por_modo"][0]["acciones_omitidas"] == ["marcar_para_reporte"]
    assert out["acciones_grafo"]["col"]["requiere_llm_fallback"] is False


# ── Planificador ────────────────────────────────────────────────────────────

class LLMPlan:
    """Planificador simulado: propone np_log1p para el problema sin regla."""

    def __init__(self):
        self.prompts = []

    def invoke(self, mensajes):
        self.prompts.append(mensajes[0].content)
        return type("R", (), {"content": json.dumps({
            "orden": ["a1", "f1"],
            "acciones_fallback": [{"id": "f1", "columna": "monto", "problema": "cola_larga",
                                   "accion": "np_log1p", "confianza": 0.9, "parametros": {},
                                   "justificacion": "reduce la asimetría"}],
            "brief_tecnico": "Aplicar el plan.",
        })})()


def _estado_plan(monkeypatch, modo):
    monkeypatch.setattr(dp, "MODO_SALIDA", modo)
    diag = {"monto": {"tipo_inferido": "financiero", "problemas": ["nulos_bajos", "cola_larga"],
                      "problema_principal": "nulos_bajos"},
            "segmento": {"tipo_inferido": "categorica_nominal", "problemas": ["cardinalidad_baja"],
                         "problema_principal": "cardinalidad_baja"}}
    estado = {"diagnostico": diag, "metadata": {"columnas_info": {}}}
    estado.update(dp.node_consultar_grafo(estado))
    return estado


def test_planificador_bi_descarta_transformaciones(monkeypatch):
    llm = LLMPlan()
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    estado = _estado_plan(monkeypatch, "bi")
    out = dp.node_planificar(estado)

    assert 'MODO DE SALIDA: "bi"' in llm.prompts[0] and "PROHIBIDO" in llm.prompts[0]
    assert "np_log1p" not in [a["accion"] for a in out["plan_acciones"]]
    assert out["sin_regla"] == []
    por_problema = {(o["columna"], o["problema"]): o for o in out["omitidos_por_modo"]}
    assert por_problema[("monto", "cola_larga")]["acciones_omitidas"] == ["np_log1p"]
    assert por_problema[("monto", "cola_larga")]["origen"] == "llm_fallback"
    assert "pd_get_dummies" in por_problema[("segmento", "cardinalidad_baja")]["acciones_omitidas"]
    assert "## MODO DE SALIDA: bi (obligatorio)" in out["brief_tecnico"]
    assert "## PROBLEMAS OMITIDOS POR MODO DE SALIDA" in out["brief_tecnico"]


def test_planificador_ml_acepta_transformaciones(monkeypatch):
    llm = LLMPlan()
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    out = dp.node_planificar(_estado_plan(monkeypatch, "ml"))

    assert 'MODO DE SALIDA: "ml"' in llm.prompts[0]
    acciones = {a["accion"]: a for a in out["plan_acciones"]}
    assert acciones["np_log1p"]["fuente_decision"] == "llm_fallback"
    assert acciones["np_log1p"]["confianza"] == dp.MAX_CONFIANZA_FALLBACK
    assert "pd_get_dummies" in acciones
    assert out["omitidos_por_modo"] == []
    assert "## MODO DE SALIDA: ml (obligatorio)" in out["brief_tecnico"]


@pytest.mark.parametrize("accion,params", [
    ("np_log1p", None), ("log_transform", None), ("aplicar_logaritmo", None),
    ("standard_scaler", None), ("escalar_minmax", None), ("winsorizar_p99", None),
    ("clip_valores", None), ("one_hot", None), ("yeo_johnson", None), ("sklearn_pca", None),
    ("transformar", {"method": "box-cox"}), ("codificar_categorias", None),
    ("hash_encoding", None),
])
def test_es_accion_ml_detecta(accion, params):
    assert dp.es_accion_ml(accion, params)


@pytest.mark.parametrize("accion", [
    "fix_encoding", "normalize_text_basico", "validar_rango_logico", "flag_outlier_no_remove",
    "reemplazar_centinela_por_nan", "fillna_median", "validar_anio_fiscal", "hash_o_anonimizar",
    "catalogar_productos", "str_lower",
])
def test_es_accion_ml_no_marca_limpieza(accion):
    assert not dp.es_accion_ml(accion)


# ── Trazabilidad ────────────────────────────────────────────────────────────

def test_informe_y_audit_registran_omitidos(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    omitidos = [{"columna": "segmento", "problema": "cardinalidad_baja", "modo": "bi",
                 "estado": "omitido_por_modo",
                 "acciones_omitidas": ["pd_get_dummies", "sklearn_one_hot_encode"]}]
    estado = {"openhands_status": "finished", "intentos": 1, "score_antes": 50,
              "score_despues": 80, "residuales": [], "sin_regla": [], "error_history": [],
              "github_url": "local://x", "modo_salida": "bi", "omitidos_por_modo": omitidos,
              "filename": "x.csv", "plan_acciones": []}
    out = dp.generar_informe_final(estado)
    assert out["omitidos_por_modo"] == omitidos and out["modo_salida"] == "bi"
    assert "omitidos por el modo de salida 'bi'" in out["resumen"]
    md = open(os.path.join(out["ruta"], "informe_final.md"), encoding="utf-8").read()
    assert "## Omitidos por modo de salida" in md
    assert "| segmento | cardinalidad_baja | pd_get_dummies, sklearn_one_hot_encode |" in md

    archivos = dict(dp._build_files_to_commit(estado, "ts"))
    audit = json.loads(archivos["outputs/ts/audit_log.json"])
    assert audit["modo_salida"] == "bi" and audit["omitidos_por_modo"] == omitidos


def test_estado_solo_anade_claves():
    claves = dp.DataPulseState.__annotations__
    assert {"modo_salida", "omitidos_por_modo"} <= set(claves)
    assert {"acciones_grafo", "plan_acciones", "sin_regla", "brief_tecnico"} <= set(claves)
