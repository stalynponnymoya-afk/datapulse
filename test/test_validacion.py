"""
Tests de la validación independiente del score (Bloque C).

Nada llama a Groq, OpenHands ni GitHub: los LLM y subprocess van mockeados.
"""

import os
import sys
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402
from src.quality_checker import generate_report  # noqa: E402


def _csv(tmp_path, nombre, df):
    ruta = tmp_path / nombre
    df.to_csv(ruta, index=False)
    return str(ruta)


def _sucio():
    return pd.DataFrame({
        "edad": [25, -999, None, 40, 40, None, -1, 33],
        "precio": [10.0, -5.0, None, None, None, 3.0, 2.0, 1.0],
        "ciudad": [" Quito", "quito", "QUITO", "Lima ", "Lima", "lima", None, "Cuenca"],
    })


def _limpio():
    return pd.DataFrame({
        "edad": [25, 31, 29, 40, 38, 27, 35, 33],
        "precio": [10.0, 5.0, 7.5, 8.0, 6.0, 3.0, 2.0, 1.0],
        "ciudad": ["quito", "lima", "quito", "lima", "cuenca", "lima", "quito", "cuenca"],
    })


def test_validacion_usa_processed_real_y_no_el_score_reportado(tmp_path):
    original = _csv(tmp_path, "orig.csv", _sucio())
    processed = _csv(tmp_path, "processed_data.csv", _sucio())
    state = {
        "filename": "orig.csv",
        "filename_path": original,
        "openhands_status": "finished",
        "ruta_processed": processed,
        "score_reportado": 99,  # el script miente
    }
    out = dp.node_validar_score(state)
    esperado = dp.generate_report_df(_sucio())["quality_score"]
    assert out["score_despues"] == esperado
    assert out["score_despues"] < dp.MIN_SCORE_PASS
    assert out["score_antes"] == esperado
    # El score reportado nunca se copia al score que decide
    assert "score_reportado" not in out

    # Score bajo: error_history con un resumen accionable para el reintento
    assert len(out["error_history"]) == 1
    msg = out["error_history"][0]
    assert f"score {esperado}/100" in msg
    assert "negativos en edad" in msg and "texto inconsistente en ciudad" in msg

    # El router no hace commit pese al 99 reportado
    estado_router = {**state, **out, "intentos": 1}
    assert dp.router_validar_score(estado_router) == "retry"


def test_validacion_aprueba_dataset_limpio(tmp_path):
    original = _csv(tmp_path, "orig.csv", _sucio())
    processed = _csv(tmp_path, "processed_data.csv", _limpio())
    state = {
        "filename": "orig.csv",
        "filename_path": original,
        "openhands_status": "finished",
        "ruta_processed": processed,
        "score_reportado": None,
    }
    out = dp.node_validar_score(state)
    assert out["score_despues"] >= dp.MIN_SCORE_PASS
    assert out["score_antes"] < out["score_despues"]
    assert "error_history" not in out
    assert dp.router_validar_score({**state, **out, "intentos": 1}) == "commit"


def test_score_antes_se_calcula_una_sola_vez(tmp_path, monkeypatch):
    processed = _csv(tmp_path, "processed_data.csv", _limpio())
    llamadas = []
    original_load = dp.load_dataset

    def _load(ruta):
        llamadas.append(ruta)
        return original_load(ruta)

    monkeypatch.setattr(dp, "load_dataset", _load)
    out = dp.node_validar_score({
        "filename_path": "/no/existe.csv",
        "score_antes": 40,
        "openhands_status": "finished",
        "ruta_processed": processed,
    })
    assert "score_antes" not in out
    assert llamadas == [processed]


def test_sin_validar_se_reintenta_si_processed_debia_existir(tmp_path):
    """processed_data.csv debía existir y no apareció → sin_validar + reintento."""
    original = _csv(tmp_path, "orig.csv", _sucio())
    for ruta in (None, str(tmp_path / "no_existe.csv")):
        state = {
            "filename": "orig.csv",
            "filename_path": original,
            "openhands_status": "finished",
            "ruta_processed": ruta,
            "processed_esperado": True,
            "score_reportado": 95,
        }
        out = dp.node_validar_score(state)
        assert out["openhands_status"] == "sin_validar"
        assert out["score_despues"] == 0
        assert "debía existir y no apareció" in out["error_history"][0]

        decisiones = [
            dp.router_validar_score({**state, **out, "intentos": n})
            for n in range(1, dp.OH_MAX_INTENTOS_SCORE + 1)
        ]
        assert decisiones == ["retry"] * (dp.OH_MAX_INTENTOS_SCORE - 1) + ["end_no_commit"]


def test_sin_validar_no_se_reintenta_si_no_se_podia_leer(tmp_path):
    """Resultados ilegibles por diseño → sin_validar, fin inmediato, sin commit."""
    original = _csv(tmp_path, "orig.csv", _sucio())
    state = {
        "filename": "orig.csv",
        "filename_path": original,
        "openhands_status": "finished",
        "ruta_processed": None,
        "processed_esperado": False,
        "score_reportado": 95,
    }
    out = dp.node_validar_score(state)
    assert out["openhands_status"] == "sin_validar"
    assert "no se pudieron leer" in out["error_history"][0]
    assert dp.router_validar_score({**state, **out, "intentos": 1}) == "end_no_commit"


def test_sin_validar_aunque_el_score_previo_fuera_alto():
    # Aunque quede un score alto de otro intento en el estado, sin_validar no hace commit
    assert dp.router_validar_score(
        {"openhands_status": "sin_validar", "score_despues": 95, "intentos": 99}
    ) == "end_no_commit"


def test_processed_invalido_es_score_cero_y_se_reintenta(tmp_path):
    """processed_data.csv apareció pero está vacío: se validó y la limpieza falló."""
    vacio = tmp_path / "processed_data.csv"
    vacio.write_text("", encoding="utf-8")
    state = {
        "score_antes": 50,
        "openhands_status": "finished",
        "ruta_processed": str(vacio),
        "processed_esperado": True,
    }
    out = dp.node_validar_score(state)
    assert "openhands_status" not in out  # sigue "finished", no es sin_validar
    assert out["score_despues"] == 0
    assert "no es un dataset válido" in out["error_history"][0]
    assert dp.router_validar_score({**state, **out, "intentos": 1}) == "retry"


def test_error_de_ejecucion_no_valida(tmp_path):
    out = dp.node_validar_score({
        "score_antes": 50,
        "openhands_status": "error",
        "ruta_processed": None,
    })
    assert out == {"score_despues": 0}


def test_min_score_pass_configurable(monkeypatch):
    monkeypatch.setenv("DATAPULSE_MIN_SCORE_PASS", "85")
    assert dp._entero_env("DATAPULSE_MIN_SCORE_PASS", 70) == 85
    monkeypatch.setenv("DATAPULSE_MIN_SCORE_PASS", "abc")
    assert dp._entero_env("DATAPULSE_MIN_SCORE_PASS", 70) == 70
    monkeypatch.delenv("DATAPULSE_MIN_SCORE_PASS")
    assert dp._entero_env("DATAPULSE_MIN_SCORE_PASS", 70) == 70


def test_leer_score_reportado():
    assert dp._leer_score_reportado('{"score": 80}') == 80
    assert dp._leer_score_reportado('{"score_after": 61.7}') == 61
    assert dp._leer_score_reportado('{"score": true}') is None
    assert dp._leer_score_reportado("[1, 2]") is None
    assert dp._leer_score_reportado("no es json") is None


class _LLMFalso:
    """LLM de mentira: guarda los prompts y devuelve un script fijo."""

    def __init__(self, codigo):
        self.codigo = codigo
        self.prompts = []

    def invoke(self, mensajes):
        self.prompts.append(mensajes[0].content)
        return SimpleNamespace(content=self.codigo)


def _estado_local(tmp_path, errores=None):
    datos = _csv(tmp_path, "entrada.csv", _sucio())
    return {
        "filename": "entrada.csv",
        "filename_path": datos,
        "file_ext": "csv",
        "metadata": {"filas": 8, "columnas": 3},
        "brief_tecnico": "Limpia el dataset.",
        "intentos": 1,
        "error_history": errores or [],
    }


def test_pipeline_local_incluye_errores_previos_en_el_prompt(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    llm = _LLMFalso("print('hola')")
    monkeypatch.setattr(dp, "get_groq_llm", lambda: llm)
    monkeypatch.setattr(
        dp.subprocess, "run",
        lambda *a, **k: SimpleNamespace(returncode=0, stdout="", stderr=""),
    )
    errores = ["error viejo", "KeyError: 'precio' en la línea 12",
               "Validación independiente: score 40/100"]
    dp._run_local_groq_pipeline(_estado_local(tmp_path, errores))
    prompt = llm.prompts[0]
    assert "ERRORES INTENTOS PREVIOS" in prompt
    assert "KeyError: 'precio'" in prompt and "score 40/100" in prompt
    assert "error viejo" not in prompt  # solo los últimos 2

    llm.prompts.clear()
    dp._run_local_groq_pipeline(_estado_local(tmp_path))
    assert "ERRORES INTENTOS PREVIOS" not in llm.prompts[0]


def test_pipeline_local_end_to_end_con_validacion(tmp_path, monkeypatch):
    """Script real (generado por un LLM falso) → processed_data.csv → score independiente."""
    monkeypatch.chdir(tmp_path)
    codigo = (
        "import sys, json\n"
        "import pandas as pd\n"
        "df = pd.read_csv(sys.argv[1])\n"
        "df = df.dropna()\n"
        "df = df[df['edad'] >= 0]\n"
        "df = df[df['precio'] >= 0]\n"
        "df['ciudad'] = df['ciudad'].str.strip().str.lower()\n"
        "df.to_csv('processed_data.csv', index=False)\n"
        "json.dump({'score': 100}, open('quality_report.json', 'w'))\n"
        "print('\"score\":', 100)\n"
    )
    monkeypatch.setattr(dp, "get_groq_llm", lambda: _LLMFalso(codigo))
    estado = _estado_local(tmp_path)
    res = dp._run_local_groq_pipeline(estado)
    assert res["openhands_status"] == "finished", res
    assert res["score_reportado"] == 100
    assert res["ruta_processed"] and os.path.exists(res["ruta_processed"])
    assert res["processed_esperado"] is True
    assert res["directorio_salida"] == os.path.dirname(res["ruta_processed"])

    val = dp.node_validar_score({**estado, **res})
    assert val["score_despues"] == generate_report(res["ruta_processed"])["quality_score"]


def test_pipeline_local_script_que_falla(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "get_groq_llm", lambda: _LLMFalso("raise SystemExit('boom')"))
    res = dp._run_local_groq_pipeline(_estado_local(tmp_path))
    assert res["openhands_status"] == "error"
    assert res["ruta_processed"] is None
    assert res["processed_esperado"] is False
    assert "boom" in res["error_history"][0]


def test_pipeline_local_sin_processed_se_reintenta(tmp_path, monkeypatch):
    """Modo local: el script terminó pero no escribió processed_data.csv."""
    monkeypatch.chdir(tmp_path)
    codigo = "import json\njson.dump({'score': 99}, open('quality_report.json', 'w'))\n"
    monkeypatch.setattr(dp, "get_groq_llm", lambda: _LLMFalso(codigo))
    estado = _estado_local(tmp_path)
    res = dp._run_local_groq_pipeline(estado)
    assert res["openhands_status"] == "finished"
    assert res["ruta_processed"] is None and res["processed_esperado"] is True
    val = dp.node_validar_score({**estado, **res})
    assert val["openhands_status"] == "sin_validar"
    assert dp.router_validar_score({**estado, **res, **val}) == "retry"


def test_pipeline_local_groq_no_responde(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    class _LLMRoto:
        def invoke(self, _):
            raise RuntimeError("rate limit")

    monkeypatch.setattr(dp, "get_groq_llm", lambda: _LLMRoto())
    res = dp._run_local_groq_pipeline(_estado_local(tmp_path))
    assert res["openhands_status"] == "error"
    assert "rate limit" in res["error_history"][0]
