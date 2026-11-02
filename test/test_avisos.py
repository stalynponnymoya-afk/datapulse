"""
Tests del punto 7: avisos del diagnóstico en el estado, audit_log e informe;
respuesta cruda en outputs/ y reintento de las columnas que faltaron.
"""

import json
import os
import sys
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402

DIAG = {"tipo_inferido": "numerica_discreta", "problemas": ["ninguno"]}


class LLMSecuencia:
    """Devuelve las respuestas en orden y guarda los prompts."""

    def __init__(self, *respuestas):
        self.respuestas, self.prompts = list(respuestas), []

    def invoke(self, mensajes):
        self.prompts.append(mensajes[0].content)
        return SimpleNamespace(content=json.dumps(self.respuestas.pop(0)))


def _meta(columnas):
    return dp.serialize_metadata(pd.DataFrame({c: [1, 2, 3] for c in columnas}), "x.csv")


def test_reintento_completa_las_columnas_que_faltaron(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    llm = LLMSecuencia({"a": DIAG, "resumen_general": "texto"}, {"b": DIAG, "c": DIAG})
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    out = dp.node_diagnosticar({"metadata": _meta(["a", "b", "c"])})

    assert set(out["diagnostico"]) == {"a", "b", "c"}
    assert len(llm.prompts) == 2
    assert 'diagnostica SOLO estas columnas, que faltaron en la respuesta anterior: ["b", "c"]' \
        in llm.prompts[1]
    # El reintento solo lleva la metadata de las columnas que faltaron
    meta_reintento = llm.prompts[1].split("Metadata:\n", 1)[1]
    assert '"b"' in meta_reintento and '"a": {' not in meta_reintento
    avisos = out["avisos_diagnostico"]
    assert any("resumen_general" in a for a in avisos)
    assert any("columnas sin diagnóstico del LLM: ['b', 'c']" in a for a in avisos)
    assert any("el reintento completó" in a for a in avisos)
    # Respuesta cruda guardada en outputs/
    cruda = open(out["respuesta_cruda_diagnostico"], encoding="utf-8").read()
    assert out["respuesta_cruda_diagnostico"].startswith(str(tmp_path / "outputs"))
    assert "resumen_general" in cruda and "reintento de columnas faltantes" in cruda


def test_columnas_que_siguen_faltando_quedan_sin_tocar(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    llm = LLMSecuencia({"a": DIAG}, {"otra_cosa": DIAG})
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    out = dp.node_diagnosticar({"metadata": _meta(["a", "b"])})
    assert list(out["diagnostico"]) == ["a"]
    assert any("siguen sin diagnóstico (quedan sin tocar): ['b']" in a
               for a in out["avisos_diagnostico"])
    grafo = dp.node_consultar_grafo({"diagnostico": out["diagnostico"]})["acciones_grafo"]
    assert "b" not in grafo


def test_sin_avisos_no_hay_reintento_ni_archivo(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    llm = LLMSecuencia({"a": DIAG, "b": DIAG})
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    out = dp.node_diagnosticar({"metadata": _meta(["a", "b"])})
    assert out["avisos_diagnostico"] == [] and out["respuesta_cruda_diagnostico"] is None
    assert len(llm.prompts) == 1
    assert not (tmp_path / "outputs").exists()


def test_avisos_de_negativos_tambien_se_guardan(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    df = pd.DataFrame({"x7": [1, -2, 3, 4]})
    llm = LLMSecuencia({"x7": {"tipo_inferido": "numerica_continua", "problemas": []}})
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    out = dp.node_diagnosticar({"metadata": dp.serialize_metadata(df, "x.csv")})
    assert any("'x7' tiene negativos" in a for a in out["avisos_diagnostico"])


def test_avisos_en_audit_log_e_informe(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "OPENHANDS_API_KEY", "")
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "")

    class LLM:
        def invoke(self, mensajes):
            p = mensajes[0].content
            if "planificador técnico" in p:
                return SimpleNamespace(content=json.dumps(
                    {"orden": [], "acciones_fallback": [], "brief_tecnico": "B"}))
            if "Escribe el script Python COMPLETO" in p:
                return SimpleNamespace(content=("import sys, shutil\n"
                                                "shutil.copy(sys.argv[1], 'processed_data.csv')\n"))
            return SimpleNamespace(content=json.dumps({"a": DIAG, "nota": "texto suelto"}))

    monkeypatch.setattr(dp, "get_llm", lambda: LLM())
    datos = tmp_path / "d.csv"
    pd.DataFrame({"a": [3, 7, 9, 12]}).to_csv(datos, index=False)
    final = dp.run_datapulse(str(datos))

    assert any("nota" in a for a in final["avisos_diagnostico"])
    audit = json.loads(dp._build_files_to_commit(final, "ts")[0][1])
    assert audit["avisos_diagnostico"] == final["avisos_diagnostico"]
    assert audit["respuesta_cruda_diagnostico"] == final["respuesta_cruda_diagnostico"]
    md = open(os.path.join(final["informe_final"]["ruta"], "informe_final.md"),
              encoding="utf-8").read()
    assert "## Avisos del diagnóstico" in md and "nota" in md
    assert "Respuesta cruda del LLM" in md
