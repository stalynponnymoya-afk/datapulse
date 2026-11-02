"""
Tests del proveedor LLM (Claude Sonnet 5.5 / Groq). Sin red ni claves reales.
"""

import json
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402

CLAVE_FALSA = "sk-ant-clave-de-prueba"


# ── Cliente de Anthropic falso ──────────────────────────────────────────────

class _StreamFalso:
    def __init__(self, mensaje):
        self.mensaje = mensaje

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get_final_message(self):
        return self.mensaje


class ClienteFalso:
    """Imita client.beta.messages.stream(...) y guarda los argumentos."""

    def __init__(self, texto="{}", stop_reason="end_turn", modelo="claude-sonnet-5-5",
                 categoria=None):
        self.llamadas = []
        contenido = [
            SimpleNamespace(type="thinking", thinking=""),   # display omitido
            SimpleNamespace(type="text", text=texto),
        ]
        self.mensaje = SimpleNamespace(
            stop_reason=stop_reason, content=contenido, model=modelo,
            stop_details=SimpleNamespace(category=categoria) if categoria else None,
        )
        self.beta = SimpleNamespace(messages=SimpleNamespace(stream=self._stream))

    def _stream(self, **kwargs):
        self.llamadas.append(kwargs)
        return _StreamFalso(self.mensaje)


def _mensajes(*textos):
    return [SimpleNamespace(content=t) for t in textos]


# ── Adaptador ───────────────────────────────────────────────────────────────

def test_parametros_de_la_peticion():
    cliente = ClienteFalso(texto='{"ok": 1}')
    llm = dp.ClaudeLLM(cliente, "claude-sonnet-5-5", "medium")
    res = llm.invoke(_mensajes("parte 1", "parte 2"))
    assert res.content == '{"ok": 1}'  # solo texto, sin el bloque de pensamiento

    kw = cliente.llamadas[0]
    assert kw["model"] == "claude-sonnet-5-5"
    assert kw["max_tokens"] == 64000
    assert kw["output_config"] == {"effort": "medium"}
    assert kw["betas"] == ["server-side-fallback-2026-07-01"]
    assert kw["fallbacks"] == "default"
    assert kw["messages"] == [{"role": "user", "content": "parte 1\n\nparte 2"}]
    # Sonnet 5.5 rechaza sampling y thinking desactivado: no se envían
    for prohibido in ("temperature", "top_p", "top_k", "thinking"):
        assert prohibido not in kw


def test_rechazo_lanza_error_claro():
    llm = dp.ClaudeLLM(ClienteFalso(stop_reason="refusal", categoria="cyber"),
                       "claude-sonnet-5-5", "medium")
    with pytest.raises(RuntimeError, match="declinó.*cyber"):
        llm.invoke(_mensajes("x"))


def test_limite_de_tokens_avisa_y_devuelve_el_texto(capsys):
    llm = dp.ClaudeLLM(ClienteFalso(texto='{"a": {"b": 1}, "c": {"d"',
                                    stop_reason="max_tokens"),
                       "claude-sonnet-5-5", "medium")
    res = llm.invoke(_mensajes("x"))
    assert "límite" in capsys.readouterr().out
    # El extractor rescata las columnas completas del JSON truncado
    assert dp.extract_json_from_markdown(res.content) == {"a": {"b": 1}}


def test_fallback_del_servidor_se_informa(capsys):
    llm = dp.ClaudeLLM(ClienteFalso(modelo="claude-sonnet-5"), "claude-sonnet-5-5", "medium")
    llm.invoke(_mensajes("x"))
    assert "Respondió claude-sonnet-5 (fallback" in capsys.readouterr().out


def test_peticion_real_del_sdk_sin_red():
    """
    SDK anthropic real con un transporte HTTP simulado: verifica lo que viajaría
    por la red (cabeceras y cuerpo) y que el streaming se interpreta bien.
    """
    import anthropic
    import httpx2

    capturado = {}
    eventos = [
        ("message_start", {"type": "message_start", "message": {
            "id": "msg_1", "type": "message", "role": "assistant",
            "model": "claude-sonnet-5-5", "content": [], "stop_reason": None,
            "stop_sequence": None, "usage": {"input_tokens": 10, "output_tokens": 1}}}),
        ("content_block_start", {"type": "content_block_start", "index": 0,
                                 "content_block": {"type": "text", "text": ""}}),
        ("content_block_delta", {"type": "content_block_delta", "index": 0,
                                 "delta": {"type": "text_delta", "text": '{"col": '}}),
        ("content_block_delta", {"type": "content_block_delta", "index": 0,
                                 "delta": {"type": "text_delta", "text": '"ok"}'}}),
        ("content_block_stop", {"type": "content_block_stop", "index": 0}),
        ("message_delta", {"type": "message_delta",
                           "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                           "usage": {"output_tokens": 5}}),
        ("message_stop", {"type": "message_stop"}),
    ]
    sse = "".join(f"event: {e}\ndata: {json.dumps(d)}\n\n" for e, d in eventos)

    def handler(request):
        capturado["url"] = str(request.url)
        capturado["headers"] = dict(request.headers)
        capturado["body"] = json.loads(request.content)
        return httpx2.Response(200, headers={"content-type": "text/event-stream"},
                               content=sse.encode())

    cliente = anthropic.Anthropic(
        api_key=CLAVE_FALSA, max_retries=0,
        http_client=anthropic.DefaultHttpxClient(transport=httpx2.MockTransport(handler)),
    )
    res = dp.ClaudeLLM(cliente, "claude-sonnet-5-5", "medium").invoke(_mensajes("hola"))
    assert res.content == '{"col": "ok"}'

    body, headers = capturado["body"], capturado["headers"]
    assert capturado["url"] == "https://api.anthropic.com/v1/messages"
    assert body["model"] == "claude-sonnet-5-5"
    assert body["stream"] is True
    assert body["fallbacks"] == "default"
    assert body["output_config"] == {"effort": "medium"}
    assert "temperature" not in body and "thinking" not in body
    assert "server-side-fallback-2026-07-01" in headers["anthropic-beta"]
    # La clave solo viaja en la cabecera de autenticación (según la versión del SDK)
    assert CLAVE_FALSA in headers.get("x-api-key", "") + headers.get("authorization", "")
    assert CLAVE_FALSA not in json.dumps(body)


# ── Selección de proveedor y claves ─────────────────────────────────────────

@pytest.mark.parametrize("pedido, hay_clave, esperado", [
    ("", True, "claude"),
    ("", False, "groq"),
    ("claude", False, "claude"),
    ("anthropic", False, "claude"),
    ("groq", True, "groq"),
    ("GROQ", True, "groq"),
    ("otro", True, "claude"),
])
def test_resolver_proveedor(monkeypatch, pedido, hay_clave, esperado):
    monkeypatch.setitem(sys.modules, "google.colab", None)
    monkeypatch.setenv("DATAPULSE_LLM", pedido)
    monkeypatch.setattr(dp, "ANTHROPIC_API_KEY", CLAVE_FALSA if hay_clave else "")
    assert dp._resolver_proveedor() == esperado


@pytest.mark.parametrize("valor, esperado", [("", "medium"), ("high", "high"),
                                             ("XHIGH", "xhigh"), ("rapido", "medium")])
def test_resolver_effort(monkeypatch, valor, esperado):
    monkeypatch.setitem(sys.modules, "google.colab", None)
    monkeypatch.setenv("DATAPULSE_CLAUDE_EFFORT", valor)
    assert dp._resolver_effort() == esperado


def test_get_claude_llm_sin_clave(monkeypatch):
    monkeypatch.setattr(dp, "ANTHROPIC_API_KEY", "")
    with pytest.raises(RuntimeError, match="Falta ANTHROPIC_API_KEY"):
        dp.get_claude_llm()


def test_get_claude_llm_pasa_la_clave_al_cliente_y_no_al_entorno(monkeypatch):
    import anthropic

    creados = []

    class AnthropicFalso:
        def __init__(self, api_key=None, **kw):
            creados.append(api_key)

    monkeypatch.setattr(anthropic, "Anthropic", AnthropicFalso)
    monkeypatch.setattr(dp, "ANTHROPIC_API_KEY", CLAVE_FALSA)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    llm = dp.get_claude_llm()
    assert creados == [CLAVE_FALSA]
    assert (llm.model, llm.effort) == (dp.CLAUDE_MODEL, dp.CLAUDE_EFFORT)
    assert dp.CLAUDE_MODEL == "claude-sonnet-5-5"
    assert "ANTHROPIC_API_KEY" not in os.environ


def test_get_llm_elige_proveedor(monkeypatch):
    monkeypatch.setattr(dp, "get_claude_llm", lambda: "claude")
    monkeypatch.setattr(dp, "get_groq_llm", lambda: "groq")
    monkeypatch.setattr(dp, "LLM_PROVEEDOR", "claude")
    assert dp.get_llm() == "claude"
    monkeypatch.setattr(dp, "LLM_PROVEEDOR", "groq")
    assert dp.get_llm() == "groq"


# ── Integración con los nodos ───────────────────────────────────────────────

def test_diagnostico_y_brief_con_claude(monkeypatch):
    """Claude diagnostica y planifica: el brief que recibe OpenHands sale de Claude."""
    diagnostico = {"edad": {"tipo_inferido": "numerica_discreta",
                            "problemas": ["nulos_disfrazados"],
                            "problema_principal": "nulos_disfrazados"}}
    plan = {"orden": ["a1"], "brief_tecnico": "Reemplazar -999 por NaN en edad."}
    clientes = iter([ClienteFalso(texto=json.dumps(diagnostico)),
                     ClienteFalso(texto=json.dumps(plan))])
    monkeypatch.setattr(dp, "LLM_PROVEEDOR", "claude")
    monkeypatch.setattr(dp, "get_claude_llm",
                        lambda: dp.ClaudeLLM(next(clientes), "claude-sonnet-5-5", "medium"))

    diag = dp.node_diagnosticar({"metadata": {"filas": 3, "columnas_info": {}}})["diagnostico"]
    assert diag == diagnostico
    grafo = dp.node_consultar_grafo({"diagnostico": diag})["acciones_grafo"]
    assert grafo["edad"]["acciones"][0]["accion"] == "reemplazar_centinela_por_nan"
    salida = dp.node_planificar({"acciones_grafo": grafo})
    assert salida["brief_tecnico"].startswith(plan["brief_tecnico"])
    assert "reemplazar_centinela_por_nan" in salida["brief_tecnico"]
    assert [a["accion"] for a in salida["plan_acciones"]] == ["reemplazar_centinela_por_nan"]


def test_generacion_local_con_claude_sin_clave_no_rompe(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "LLM_PROVEEDOR", "claude")
    monkeypatch.setattr(dp, "ANTHROPIC_API_KEY", "")
    datos = tmp_path / "d.csv"
    datos.write_text("a\n1\n", encoding="utf-8")
    res = dp._run_local_groq_pipeline({
        "filename": "d.csv", "filename_path": str(datos), "file_ext": "csv",
        "metadata": {}, "brief_tecnico": "", "intentos": 0, "error_history": [],
    })
    assert res["openhands_status"] == "error"
    assert "Falta ANTHROPIC_API_KEY" in res["error_history"][0]
