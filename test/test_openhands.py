"""
Tests del camino OpenHands y de las rutas (Bloques E y F), sin red.

Se sustituyen ``requests``, ``time.sleep`` y PyGithub por dobles en memoria:
nunca se contacta OpenHands ni GitHub.
"""

import base64
import os
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402
from src.quality_checker import generate_report  # noqa: E402

BASE = "https://app.all-hands.dev/api/v1"


# ── Dobles de GitHub ─────────────────────────────────────────────────────────

class _Contenido:
    def __init__(self, data: bytes, grande: bool = False):
        self.sha = f"sha-{len(data)}"
        self.size = len(data)
        self._data = data
        self._grande = grande

    @property
    def decoded_content(self):
        if self._grande:
            # La API de contents no devuelve el cuerpo de archivos > 1 MB
            raise AssertionError("unsupported encoding: none")
        return self._data


class RepoFalso:
    def __init__(self, private=True, salidas=None, grande=()):
        self.private = private
        self.full_name = "usuario/repo"
        self.refs = []
        self.creados = []    # (path, branch)
        self.borrados = []
        # Archivos que el "agente" dejó commiteados en cualquier rama de trabajo
        self.salidas = salidas or {}
        self.grande = set(grande)

    def get_branch(self, nombre):
        return SimpleNamespace(commit=SimpleNamespace(sha=f"sha-{nombre}"))

    def create_git_ref(self, ref, sha):
        self.refs.append((ref, sha))

    def get_contents(self, path, ref=None):
        if ref and ref.startswith("data/datapulse-") and path in self.salidas:
            return _Contenido(self.salidas[path], grande=path in self.grande)
        raise Exception("404 Not Found")

    def get_git_blob(self, sha):
        for path, data in self.salidas.items():
            if f"sha-{len(data)}" == sha:
                return SimpleNamespace(content=base64.b64encode(data).decode())
        raise Exception("blob no encontrado")

    def create_file(self, path, message, content, branch):
        self.creados.append((path, branch))

    def delete_file(self, path, message, sha, branch):
        self.borrados.append(path)


class GithubFalso:
    repo = None
    pedidos = []

    def __init__(self, *args, **kwargs):
        pass

    def get_repo(self, nombre):
        GithubFalso.pedidos.append(nombre)
        return GithubFalso.repo


# ── Doble de la API de OpenHands ─────────────────────────────────────────────

class OpenHandsFalso:
    def __init__(self, estado_final="finished", eventos=None):
        self.estado_final = estado_final
        self.eventos = eventos if eventos is not None else []
        self.mensajes = []
        self.conversaciones = []

    def post(self, url, headers=None, json=None, timeout=None):
        if url.endswith("/app-conversations"):
            self.conversaciones.append(json)
            return _Resp({"id": "task-1234567890abcdef"})
        if url.endswith("/send-message"):
            self.mensajes.append(json["content"][0]["text"])
            return _Resp({})
        raise AssertionError(f"POST inesperado {url}")

    def get(self, url, headers=None, timeout=None):
        if "/start-tasks" in url:
            return _Resp([{"status": "READY", "app_conversation_id": "conv-1234567890abcdef"}])
        if "/events/search" in url:
            return _Resp({"items": self.eventos, "next_page_id": None})
        if "/app-conversations?ids=" in url:
            return _Resp([{"execution_status": self.estado_final}])
        raise AssertionError(f"GET inesperado {url}")


class _Resp:
    def __init__(self, data, status=200):
        self._data = data
        self.status_code = status

    def json(self):
        return self._data

    def raise_for_status(self):
        pass


EVENTO_SCRIPT = {
    "action": {
        "kind": "FileEditorAction",
        "path": "/workspace/repo/datapulse_pipeline.py",
        "command": "create",
        "file_text": "import sys\nprint('limpio')\n",
    }
}
EVENTO_FIN = {"action": {"kind": "FinishAction", "message": "Listo"}}


def _limpio_csv() -> bytes:
    df = pd.DataFrame({
        "edad": [25, 31, 29, 40, 38, 27, 35, 33],
        "ciudad": ["quito", "lima", "quito", "lima", "cuenca", "lima", "quito", "cuenca"],
    })
    return df.to_csv(index=False).encode()


@pytest.fixture
def entorno(tmp_path, monkeypatch):
    """Configura credenciales falsas y dobles de red; devuelve un estado base."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "OPENHANDS_API_KEY", "oh-falsa")
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "gh-falso")
    monkeypatch.setattr(dp, "GITHUB_USERNAME", "usuario")
    monkeypatch.setattr(dp, "GITHUB_REPO", "repo")
    monkeypatch.setattr(dp, "Github", GithubFalso)
    monkeypatch.setattr(dp.time, "sleep", lambda *_: None)
    monkeypatch.setenv("DATAPULSE_PERMITIR_SUBIDA_DATOS", "1")
    GithubFalso.pedidos = []

    datos = tmp_path / "ventas.csv"
    datos.write_text("edad,ciudad\n25, Quito\n-1,lima\n", encoding="utf-8")
    estado = {
        "filename": "ventas.csv",
        "filename_path": str(datos),
        "file_ext": "csv",
        "metadata": {"filas": 2, "columnas": 2},
        "brief_tecnico": "Limpia ventas.",
        "intentos": 0,
        "error_history": [],
        "github_source_branch": None,
        "github_source_path": None,
    }

    def configurar(repo, oh):
        GithubFalso.repo = repo
        monkeypatch.setattr(dp.requests, "post", oh.post)
        monkeypatch.setattr(dp.requests, "get", oh.get)

    return estado, configurar


# ── Bloque C/E: sin fallback de 75 y lectura de la rama ─────────────────────

def test_openhands_con_solo_un_py_queda_sin_validar(entorno):
    """Antes: OpenHands 'finished' + cualquier .py → score 75 y commit."""
    estado, configurar = entorno
    repo = RepoFalso(private=True, salidas={})
    oh = OpenHandsFalso(eventos=[EVENTO_SCRIPT, EVENTO_FIN])
    configurar(repo, oh)

    res = dp.node_ejecutar(estado)
    assert res["openhands_status"] == "finished"
    assert res["score_reportado"] is None
    assert res["ruta_processed"] is None
    assert "score_despues" not in res  # ya no hay score inventado
    assert "datapulse_pipeline.py" in res["codigo"] or "limpio" in res["codigo"]

    val = dp.node_validar_score({**estado, **res})
    assert val["openhands_status"] == "sin_validar"
    assert val["score_despues"] == 0
    decision = dp.router_validar_score({**estado, **res, **val})
    assert decision != "commit"


def test_openhands_lee_processed_de_la_rama_y_valida(entorno):
    estado, configurar = entorno
    salidas = {
        "processed_data.csv": _limpio_csv(),
        "quality_report.json": b'{"score": 97}',
        "cleaning_log.json": b"[]",
        "datapulse_pipeline.py": b"import sys\n",
    }
    repo = RepoFalso(private=True, salidas=salidas, grande={"processed_data.csv"})
    oh = OpenHandsFalso(eventos=[EVENTO_FIN])
    configurar(repo, oh)

    res = dp.node_ejecutar(estado)
    assert res["score_reportado"] == 97
    assert res["ruta_processed"] and os.path.exists(res["ruta_processed"])
    with open(res["ruta_processed"], "rb") as f:
        assert f.read() == salidas["processed_data.csv"]  # leído vía blob (>1 MB)
    assert res["codigo"] == "import sys\n"

    val = dp.node_validar_score({**estado, **res})
    assert val["score_despues"] == generate_report(res["ruta_processed"])["quality_score"]
    assert dp.router_validar_score({**estado, **res, **val}) == "commit"


def test_brief_usa_la_ruta_real_y_pide_commit_en_la_rama(entorno):
    estado, configurar = entorno
    estado = {
        **estado,
        "github_source_branch": "dev",
        "github_source_path": "datasets/2024/ventas.csv",
        "error_history": ["fallo previo X"],
    }
    repo = RepoFalso(private=True)
    oh = OpenHandsFalso(eventos=[EVENTO_FIN])
    configurar(repo, oh)

    dp.node_ejecutar(estado)
    brief = oh.mensajes[0]
    assert "Ruta en el repo: datasets/2024/ventas.csv" in brief
    assert "data/ventas.csv" not in brief
    assert "sys.argv[1]" in brief
    assert "fallo previo X" in brief
    # Rama de trabajo nueva creada desde la rama de origen, nunca la de origen
    rama = oh.conversaciones[0]["selected_branch"]
    assert rama.startswith("data/datapulse-")
    assert repo.refs == [(f"refs/heads/{rama}", "sha-dev")]
    assert f"rama `{rama}`" in brief and "processed_data.csv" in brief
    # Archivo ya en GitHub: no se sube nada
    assert repo.creados == []


def test_openhands_estado_terminal_de_error(entorno):
    estado, configurar = entorno
    repo = RepoFalso(private=True)
    configurar(repo, OpenHandsFalso(estado_final="stuck", eventos=[]))
    res = dp.node_ejecutar(estado)
    assert res["openhands_status"] == "stuck"
    assert dp.router_validar_score({**res, "intentos": 1}) == "retry"
    assert dp.router_validar_score({**res, "intentos": 2}) == "end_no_commit"


def test_openhands_excepcion_de_red(entorno, monkeypatch):
    estado, configurar = entorno
    configurar(RepoFalso(private=True), OpenHandsFalso())

    def _caida(*a, **k):
        raise ConnectionError("sin red")

    monkeypatch.setattr(dp.requests, "post", _caida)
    res = dp.node_ejecutar(estado)
    assert res["openhands_status"] == "error"
    assert "ConnectionError" in res["error_history"][0]


# ── Bloque E: modo local con sys.argv y descarga en Colab ───────────────────

def test_subprocess_recibe_la_ruta_como_argumento(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    datos = tmp_path / "d.csv"
    datos.write_text("a\n1\n", encoding="utf-8")
    capturado = {}

    def _run(cmd, **kwargs):
        capturado["cmd"] = cmd
        capturado["kwargs"] = kwargs
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    llm = SimpleNamespace(
        invoke=lambda m: (capturado.setdefault("prompt", m[0].content),
                          SimpleNamespace(content="print(1)"))[1]
    )
    monkeypatch.setattr(dp, "get_groq_llm", lambda: llm)
    monkeypatch.setattr(dp.subprocess, "run", _run)
    dp._run_local_groq_pipeline({
        "filename": "d.csv", "filename_path": str(datos), "file_ext": "csv",
        "metadata": {}, "brief_tecnico": "", "intentos": 0, "error_history": [],
    })
    assert capturado["cmd"][1:] == ["datapulse_pipeline.py", str(datos.resolve())]
    # El prompt no contiene la ruta absoluta: el script la lee de sys.argv[1]
    assert str(datos.resolve()) not in capturado["prompt"]
    assert "sys.argv[1]" in capturado["prompt"]


def test_descarga_colab_usa_la_ruta_real(monkeypatch, tmp_path, capsys):
    descargas = []

    def _download(ruta):
        if not os.path.exists(ruta):
            raise FileNotFoundError(ruta)
        descargas.append(ruta)

    colab = SimpleNamespace(files=SimpleNamespace(download=_download))
    monkeypatch.setitem(sys.modules, "google", SimpleNamespace(colab=colab))
    monkeypatch.setitem(sys.modules, "google.colab", colab)

    real = tmp_path / "outputs" / "x-local" / "processed_data.csv"
    real.parent.mkdir(parents=True)
    real.write_text("a\n1\n")
    dp._descargar_en_colab({"ruta_processed": str(real)})
    assert descargas == [str(real)]

    # Ruta inexistente: se captura FileNotFoundError
    dp._descargar_en_colab({"ruta_processed": str(tmp_path / "no.csv")})
    assert "No se encontró" in capsys.readouterr().out

    # Sin processed: no intenta descargar
    dp._descargar_en_colab({"ruta_processed": None})
    assert descargas == [str(real)]


def test_descarga_fuera_de_colab_no_hace_nada(monkeypatch):
    monkeypatch.setitem(sys.modules, "google.colab", None)
    dp._descargar_en_colab({"ruta_processed": "/x.csv"})
