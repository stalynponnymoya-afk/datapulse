"""
Tests del dominio demo de retail (punto 12): está registrado pero inactivo por
defecto, sus valores de negocio son valores de ejemplo en los parámetros de cada
arista, activarlo cambia el plan y cambiar un parámetro cambia lo que recibe el
ejecutor. Sin red: el LLM siempre se simula.
"""

import json
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402

# Diagnóstico de la tienda con los problemas del dominio retail
DIAGNOSTICO = {
    "Satisfaccion": {"tipo_inferido": "categorica_ordinal",
                     "problemas": ["satisfaccion_con_etiquetas_texto"]},
    "Metodo_Pago": {"tipo_inferido": "categorica_nominal", "problemas": ["metodo_pago_ambiguo"]},
    "Cantidad_Articulos": {"tipo_inferido": "numerica_discreta",
                           "problemas": ["numeros_en_palabras"]},
    "Total_Compra": {"tipo_inferido": "financiero", "problemas": ["total_compra_inconsistente"]},
}
ACCIONES_RETAIL = {"mapear_etiquetas_satisfaccion_a_numero", "reemplazar_categoria_ambigua",
                   "convertir_numeros_en_palabras", "recalcular_total_y_marcar"}


@pytest.fixture
def dominios_restaurados():
    """Restaura el grafo global y DOMINIOS_ACTIVOS al terminar cada test."""
    previo = (list(dp.DOMINIOS_ACTIVOS), dp.kg, dp.KNOWLEDGE_GRAPH, dp._DOMINIOS_APLICADOS)
    yield
    (dp.DOMINIOS_ACTIVOS, dp.kg, dp.KNOWLEDGE_GRAPH, dp._DOMINIOS_APLICADOS) = previo


def _activar_retail():
    dp.DOMINIOS_ACTIVOS = ["retail"]
    dp.aplicar_dominios_activos()


class LLMFalso:
    """Planificador y ejecutor local simulados; guarda los prompts que recibe."""

    def __init__(self):
        self.prompts = []

    def invoke(self, mensajes):
        prompt = mensajes[0].content
        self.prompts.append(prompt)
        if "planificador técnico" in prompt:
            return SimpleNamespace(content=json.dumps(
                {"orden": [], "acciones_fallback": [], "brief_tecnico": "Aplicar el plan."}))
        # Ejecutor local: ya se capturó el prompt; se corta antes de ejecutar código
        raise RuntimeError("fin de la prueba: no se ejecuta código")

    def prompt_ejecutor(self):
        return next(p for p in self.prompts if "ingeniero de datos senior" in p)


def _plan(monkeypatch, llm):
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    estado = {"diagnostico": json.loads(json.dumps(DIAGNOSTICO)),
              "metadata": {"filas": 515, "columnas": 4, "columnas_info": {}}}
    estado.update(dp.node_consultar_grafo(estado))
    estado.update(dp.node_planificar(estado))
    return estado


def _aristas_retail(grafo):
    return [(u, v, d) for u, v, d in grafo.edges(data=True) if d["accion"] in ACCIONES_RETAIL]


# ── Registro y valores de ejemplo ───────────────────────────────────────────

def test_retail_registrado_e_inactivo_por_defecto():
    assert dp.DOMINIOS["retail"] is dp.agregar_reglas_retail
    assert "retail" not in dp.DOMINIOS_ACTIVOS
    assert _aristas_retail(dp.kg) == []
    etiquetas = {e for grupo in dp.vocabulario_grafo().values()
                 for lista in grupo.values() for e in lista}
    assert "numeros_en_palabras" not in etiquetas


def test_valores_de_ejemplo_como_parametros_explicitos():
    grafo = dp.agregar_reglas_retail(dp.construir_grafo_conocimiento())
    assert dp.errores_grafo(grafo) == []
    por_accion = {}
    for u, v, d in _aristas_retail(grafo):
        por_accion.setdefault(d["accion"], []).append(d)
        assert "valor de ejemplo: ajústalo a tu negocio" in d["justificacion_es"].lower()
        assert "example value: adjust it to your business" in d["justificacion_en"].lower()
        assert "modo" not in d                       # se aplican en bi y en ml
    assert {d["parametros"]["mapeo"]["excelente"] for d in
            por_accion["mapear_etiquetas_satisfaccion_a_numero"]} == {5}
    assert all(d["parametros"]["mapeo"] == {"excelente": 5, "muy bueno": 4}
               for d in por_accion["mapear_etiquetas_satisfaccion_a_numero"])
    assert por_accion["reemplazar_categoria_ambigua"][0]["parametros"]["reemplazos"] == {
        "TARJETA": "Tarjeta sin especificar"}
    assert all(d["parametros"]["palabras"] == {"tres": 3, "veinte": 20, "treinta": 30}
               for d in por_accion["convertir_numeros_en_palabras"])
    total = por_accion["recalcular_total_y_marcar"][0]["parametros"]
    assert total["columna_total"] == "Total_Compra"
    assert total["factores"] == ["Cantidad_Articulos", "Precio_Unitario"]
    assert total["operacion"] == "multiplicar" and total["flag_suffix"] == "_recalculado"
    assert total["marcar_solo_filas_que_cambian"] is True


def test_cada_arista_tiene_su_propia_copia_de_los_parametros():
    """Editar en caliente una arista no cambia otra por compartir el mismo dict."""
    grafo = dp.agregar_reglas_retail(dp.construir_grafo_conocimiento())
    palabras = [d["parametros"]["palabras"] for _, _, d in _aristas_retail(grafo)
                if d["accion"] == "convertir_numeros_en_palabras"]
    assert len({id(p) for p in palabras}) == len(palabras) > 1


# ── Activar retail cambia el plan ───────────────────────────────────────────

def test_activar_retail_cambia_el_plan(monkeypatch, dominios_restaurados):
    sin_retail = _plan(monkeypatch, LLMFalso())
    assert not ACCIONES_RETAIL & {a["accion"] for a in sin_retail["plan_acciones"]}
    assert {(p["columna"], p["problema"]) for p in sin_retail["sin_regla"]} == {
        (col, d["problemas"][0]) for col, d in DIAGNOSTICO.items()}

    _activar_retail()
    con_retail = _plan(monkeypatch, LLMFalso())
    por_columna = {a["columna"]: a for a in con_retail["plan_acciones"]}
    assert {a["accion"] for a in por_columna.values()} == ACCIONES_RETAIL
    assert all(a["fuente_decision"] == "knowledge_graph" for a in por_columna.values())
    assert por_columna["Metodo_Pago"]["parametros"]["reemplazos"] == {
        "TARJETA": "Tarjeta sin especificar"}
    assert con_retail["sin_regla"] == []
    # El vocabulario del diagnóstico ahora ofrece los problemas de retail
    prompt = dp.construir_prompt_diagnostico({})
    for etiqueta in ("satisfaccion_con_etiquetas_texto", "metodo_pago_ambiguo",
                     "numeros_en_palabras", "total_compra_inconsistente"):
        assert etiqueta in prompt


# ── Cambiar un parámetro cambia lo que recibe el ejecutor ───────────────────

def _prompt_del_ejecutor(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "OPENHANDS_API_KEY", "")          # modo local: lo escribe el LLM
    llm = LLMFalso()
    estado = _plan(monkeypatch, llm)
    datos = tmp_path / "tienda.csv"
    datos.write_text("Satisfaccion\n5\n", encoding="utf-8")
    estado.update({"filename": "tienda.csv", "filename_path": str(datos), "file_ext": "csv",
                   "intentos": 0, "error_history": []})
    salida = dp.node_ejecutar(estado)
    assert salida["openhands_status"] == "error"             # cortado por el LLM simulado
    return llm.prompt_ejecutor()


def test_cambiar_un_parametro_cambia_lo_que_recibe_el_ejecutor(monkeypatch, tmp_path,
                                                               dominios_restaurados):
    _activar_retail()
    antes = _prompt_del_ejecutor(monkeypatch, tmp_path)
    assert '"muy bueno": 4' in antes and '"TARJETA": "Tarjeta sin especificar"' in antes
    assert '"cuarenta"' not in antes

    # Edición en caliente, como explica el README: por nombre de acción
    for _, _, datos in dp.kg.edges(data=True):
        if datos["accion"] == "mapear_etiquetas_satisfaccion_a_numero":
            datos["parametros"]["mapeo"]["muy bueno"] = 4.5
        if datos["accion"] == "reemplazar_categoria_ambigua":
            datos["parametros"]["reemplazos"]["TARJETA"] = "Tarjeta (otra)"
        if datos["accion"] == "convertir_numeros_en_palabras":
            datos["parametros"]["palabras"]["cuarenta"] = 40
    dp.validar_grafo(dp.kg)

    despues = _prompt_del_ejecutor(monkeypatch, tmp_path)
    assert '"muy bueno": 4.5' in despues and '"muy bueno": 4,' not in despues
    assert '"TARJETA": "Tarjeta (otra)"' in despues and "Tarjeta sin especificar" not in despues
    assert '"cuarenta": 40' in despues

    # Reconstruir el dominio vuelve a los valores de ejemplo de agregar_reglas_retail
    dp.aplicar_dominios_activos(forzar=True)
    assert '"muy bueno": 4' in _prompt_del_ejecutor(monkeypatch, tmp_path)


def test_editar_el_dominio_cambia_lo_que_recibe_el_ejecutor(monkeypatch, tmp_path,
                                                             dominios_restaurados):
    """Edición permanente: una versión del dominio con otros valores de ejemplo."""
    original = dp.agregar_reglas_retail

    def retail_editado(kg):
        original(kg)
        for _, _, datos in kg.edges(data=True):
            if datos["accion"] == "recalcular_total_y_marcar":
                datos["parametros"]["factores"] = ["Unidades", "Precio_Con_IVA"]
        return kg

    monkeypatch.setitem(dp.DOMINIOS, "retail", retail_editado)
    _activar_retail()
    prompt = _prompt_del_ejecutor(monkeypatch, tmp_path)
    # Precio_Unitario solo aparecía en 'factores' (no es columna del diagnóstico)
    assert '"Precio_Con_IVA"' in prompt and '"Precio_Unitario"' not in prompt
