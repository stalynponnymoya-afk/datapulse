"""
Tests de la lectura de configuración y secretos (Colab → entorno → defecto).

Se simula ``google.colab.userdata``: nunca se usan claves reales.
"""

import importlib.util
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402

RUTA_MODULO = os.path.join(os.path.dirname(__file__), "..", "datapulse_prod.py")
NOMBRES = ("GROQ_API_KEY", "GITHUB_TOKEN", "OPENHANDS_API_KEY",
           "GITHUB_USERNAME", "GITHUB_REPO", "GROQ_MODEL")


class SecretNotFoundError(Exception):
    pass


class NotebookAccessError(Exception):
    pass


def _colab_falso(monkeypatch, secretos, sin_acceso=()):
    """Instala un google.colab.userdata falso con los secretos dados."""

    def get(nombre):
        if nombre in sin_acceso:
            raise NotebookAccessError(nombre)
        if nombre not in secretos:
            raise SecretNotFoundError(nombre)
        return secretos[nombre]

    colab = SimpleNamespace(userdata=SimpleNamespace(get=get))
    monkeypatch.setitem(sys.modules, "google", SimpleNamespace(colab=colab))
    monkeypatch.setitem(sys.modules, "google.colab", colab)


@pytest.fixture
def entorno_limpio(monkeypatch):
    for nombre in NOMBRES:
        monkeypatch.delenv(nombre, raising=False)
    return monkeypatch


def _cargar_modulo():
    """Ejecuta datapulse_prod.py desde cero (como al importarlo en Colab)."""
    spec = importlib.util.spec_from_file_location("dp_config", RUTA_MODULO)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_un_secreto_que_falta_no_anula_los_demas(entorno_limpio, capsys):
    """Antes: faltaba OPENHANDS_API_KEY → las tres claves quedaban vacías."""
    _colab_falso(entorno_limpio, {"GROQ_API_KEY": "gsk_falsa"})
    mod = _cargar_modulo()
    assert mod.GROQ_API_KEY == "gsk_falsa"
    assert mod.OPENHANDS_API_KEY == "" and mod.GITHUB_TOKEN == ""
    assert os.environ["GROQ_API_KEY"] == "gsk_falsa"  # lo usa langchain-groq

    salida = capsys.readouterr().out
    assert "GROQ_API_KEY: secreto de Colab" in salida
    assert "OPENHANDS_API_KEY: no definido" in salida
    assert "modo local" in salida and "carpeta local outputs/" in salida
    assert "gsk_falsa" not in salida  # nunca se imprime una clave


def test_secretos_completos_y_repo_desde_colab(entorno_limpio, capsys):
    _colab_falso(entorno_limpio, {
        "GROQ_API_KEY": "gsk_falsa", "GITHUB_TOKEN": "ghp_falso",
        "OPENHANDS_API_KEY": "oh_falsa", "GITHUB_USERNAME": "usuario",
        "GITHUB_REPO": "datapulse",
    })
    mod = _cargar_modulo()
    assert (mod.GITHUB_USERNAME, mod.GITHUB_REPO) == ("usuario", "datapulse")
    assert mod._falta_repo_github() is None
    salida = capsys.readouterr().out
    assert "Ejecución: OpenHands" in salida
    assert "GitHub (usuario/datapulse)" in salida
    assert "ghp_falso" not in salida and "oh_falsa" not in salida


def test_entorno_como_respaldo_y_sin_acceso(entorno_limpio):
    # Sin acceso del notebook al secreto (o con !python) → variable de entorno
    _colab_falso(entorno_limpio, {"GITHUB_REPO": "repo-colab"}, sin_acceso={"GROQ_API_KEY"})
    entorno_limpio.setenv("GROQ_API_KEY", "gsk_entorno")
    entorno_limpio.setenv("GITHUB_USERNAME", "  usuario-entorno  ")
    mod = _cargar_modulo()
    assert mod.GROQ_API_KEY == "gsk_entorno"
    assert mod.ORIGEN_CONFIG["GROQ_API_KEY"] == "variable de entorno"
    assert mod.GITHUB_USERNAME == "usuario-entorno"
    assert mod.GITHUB_REPO == "repo-colab"


def test_secreto_de_colab_tiene_prioridad(entorno_limpio):
    _colab_falso(entorno_limpio, {"GROQ_MODEL": "modelo-colab"})
    entorno_limpio.setenv("GROQ_MODEL", "modelo-entorno")
    assert dp._leer_config("GROQ_MODEL", "defecto") == "modelo-colab"
    assert dp.ORIGEN_CONFIG["GROQ_MODEL"] == "secreto de Colab"


def test_fuera_de_colab_y_valores_por_defecto(entorno_limpio):
    entorno_limpio.setitem(sys.modules, "google.colab", None)  # ImportError
    assert dp._leer_config("GROQ_MODEL", "openai/gpt-oss-120b") == "openai/gpt-oss-120b"
    assert dp.ORIGEN_CONFIG["GROQ_MODEL"] == "valor por defecto"
    assert dp._leer_config("GITHUB_TOKEN") == ""
    assert dp.ORIGEN_CONFIG["GITHUB_TOKEN"] == "no definido"
    entorno_limpio.setenv("GITHUB_TOKEN", "ghp_entorno")
    assert dp._leer_config("GITHUB_TOKEN") == "ghp_entorno"


def test_aviso_si_falta_groq(entorno_limpio, monkeypatch):
    monkeypatch.setattr(dp, "GROQ_API_KEY", "")
    assert "Falta GROQ_API_KEY" in dp.resumen_configuracion()
    monkeypatch.setattr(dp, "GROQ_API_KEY", "x")
    assert "Falta GROQ_API_KEY" not in dp.resumen_configuracion()
