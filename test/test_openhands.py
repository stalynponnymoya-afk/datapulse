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


# ── Bloque F: seguridad y privacidad ────────────────────────────────────────

def test_subprocess_recibe_env_sin_claves(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for clave in ("GROQ_API_KEY", "GITHUB_TOKEN", "OPENHANDS_API_KEY", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(clave, "secreto-" + clave)
    datos = tmp_path / "d.csv"
    datos.write_text("a\n1\n", encoding="utf-8")
    capturado = {}

    def _run(cmd, **kwargs):
        capturado.update(kwargs)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(dp, "get_groq_llm",
                        lambda: SimpleNamespace(invoke=lambda m: SimpleNamespace(content="x=1")))
    monkeypatch.setattr(dp.subprocess, "run", _run)
    dp._run_local_groq_pipeline({
        "filename": "d.csv", "filename_path": str(datos), "file_ext": "csv",
        "metadata": {}, "brief_tecnico": "", "intentos": 0, "error_history": [],
    })
    env = capturado["env"]
    assert set(env) == {"PATH", "HOME", "LANG", "PYTHONIOENCODING"}
    assert env["PYTHONIOENCODING"] == "utf-8"
    assert not any("secreto" in v for v in env.values())


def test_script_generado_no_ve_las_claves(tmp_path, monkeypatch):
    """Ejecución real del subproceso: el script no puede leer GROQ_API_KEY."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GROQ_API_KEY", "secreto")
    datos = tmp_path / "d.csv"
    datos.write_text("a\n1\n", encoding="utf-8")
    codigo = (
        "import os, sys\n"
        "import pandas as pd\n"
        "assert 'GROQ_API_KEY' not in os.environ\n"
        "pd.read_csv(sys.argv[1]).to_csv('processed_data.csv', index=False)\n"
    )
    monkeypatch.setattr(dp, "get_groq_llm",
                        lambda: SimpleNamespace(invoke=lambda m: SimpleNamespace(content=codigo)))
    res = dp._run_local_groq_pipeline({
        "filename": "d.csv", "filename_path": str(datos), "file_ext": "csv",
        "metadata": {}, "brief_tecnico": "", "intentos": 0, "error_history": [],
    })
    assert res["openhands_status"] == "finished", res
    assert res["ruta_processed"]


def test_rechazo_subida_a_repo_publico(entorno):
    estado, configurar = entorno
    repo = RepoFalso(private=False)
    oh = OpenHandsFalso()
    configurar(repo, oh)

    res = dp.node_ejecutar(estado)
    assert res["openhands_status"] == "rechazado"
    msg = res["error_history"][0]
    assert "no es privado" in msg and "modo local" in msg
    # No se creó rama, no se subió nada y OpenHands no arrancó
    assert repo.refs == [] and repo.creados == []
    assert oh.conversaciones == []
    assert dp.router_validar_score({**res}) == "end_no_commit"


def test_rechazo_subida_sin_variable_de_permiso(entorno, monkeypatch):
    estado, configurar = entorno
    monkeypatch.delenv("DATAPULSE_PERMITIR_SUBIDA_DATOS")
    repo = RepoFalso(private=True)
    configurar(repo, OpenHandsFalso())
    res = dp.node_ejecutar(estado)
    assert res["openhands_status"] == "rechazado"
    assert "DATAPULSE_PERMITIR_SUBIDA_DATOS" in res["error_history"][0]
    assert repo.creados == []


def test_subida_permitida_en_repo_privado(entorno):
    estado, configurar = entorno
    repo = RepoFalso(private=True)
    oh = OpenHandsFalso(eventos=[EVENTO_FIN])
    configurar(repo, oh)
    dp.node_ejecutar(estado)
    assert [p for p, _ in repo.creados] == ["data/ventas.csv"]
    assert "processed_data.csv" in oh.mensajes[0]


def test_github_file_en_repo_publico_no_sube_datos(entorno):
    """Un archivo ya en GitHub no cuenta como subida, pero no se piden ni leen datos."""
    estado, configurar = entorno
    estado = {**estado, "github_source_branch": "main", "github_source_path": "data/x.csv"}
    salidas = {"processed_data.csv": _limpio_csv(), "quality_report.json": b'{"score": 90}'}
    repo = RepoFalso(private=False, salidas=salidas)
    oh = OpenHandsFalso(eventos=[EVENTO_FIN])
    configurar(repo, oh)

    res = dp.node_ejecutar(estado)
    assert res["openhands_status"] == "finished"
    assert repo.creados == []
    brief = oh.mensajes[0]
    assert "NO hagas commit de archivos de datos" in brief
    assert "processed_data.csv" not in brief.split("Haz commit y push")[1].split("\n")[1]
    # Solo se leyó el reporte, no processed_data.csv → sin_validar
    assert res["ruta_processed"] is None
    assert res["score_reportado"] == 90
    assert dp.node_validar_score({**estado, **res})["openhands_status"] == "sin_validar"


def test_sin_usuario_o_repo_openhands_falla_claro(entorno, monkeypatch):
    estado, configurar = entorno
    configurar(RepoFalso(), OpenHandsFalso())
    monkeypatch.setattr(dp, "GITHUB_USERNAME", "")
    res = dp.node_ejecutar(estado)
    assert res["openhands_status"] == "rechazado"
    assert "GITHUB_USERNAME" in res["error_history"][0]
    assert GithubFalso.pedidos == []

    with pytest.raises(ValueError, match="GITHUB_USERNAME"):
        dp._download_github_file("data/x.csv", "main", "/tmp/no_se_usa.csv")


def test_usuario_y_repo_sin_valor_por_defecto(monkeypatch):
    import importlib.util

    monkeypatch.delenv("GITHUB_USERNAME", raising=False)
    monkeypatch.delenv("GITHUB_REPO", raising=False)
    ruta = os.path.join(os.path.dirname(__file__), "..", "datapulse_prod.py")
    spec = importlib.util.spec_from_file_location("dp_sin_env", ruta)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod.GITHUB_USERNAME == "" and mod.GITHUB_REPO == ""
    assert "GITHUB_USERNAME" in mod._falta_repo_github()


def _estado_commit():
    return {
        "filename": "ventas.csv",
        "score_despues": 88,
        "plan_acciones": [{"fuente_decision": "knowledge_graph"}],
        "archivos_generados": {
            "datapulse_pipeline.py": "print(1)",
            "quality_report.json": '{"score": 88}',
            "cleaning_log.json": "[]",
            "processed_data.csv": "a\n1\n",
            "/workspace/extra/clientes.json": '[{"a": 1}]',
            "/workspace/extra/hoja.xlsx": "xx",
        },
    }


def test_commit_en_repo_publico_sube_solo_script_y_reportes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "gh-falso")
    monkeypatch.setattr(dp, "GITHUB_USERNAME", "usuario")
    monkeypatch.setattr(dp, "GITHUB_REPO", "repo")
    monkeypatch.setattr(dp, "Github", GithubFalso)
    monkeypatch.setenv("DATAPULSE_PERMITIR_SUBIDA_DATOS", "1")
    repo = RepoFalso(private=False)
    GithubFalso.repo = repo

    out = dp.node_commit(_estado_commit())
    subidos = sorted(os.path.basename(p) for p, _ in repo.creados)
    assert subidos == ["audit_log.json", "cleaning_log.json", "datapulse_pipeline.py",
                       "quality_report.json"]
    assert out["github_url"].startswith("https://github.com/usuario/repo/tree/")
    # Los datos no se pierden: quedan en local
    locales = os.listdir(next((tmp_path / "outputs").iterdir()))
    assert {"processed_data.csv", "clientes.json", "hoja.xlsx"} <= set(locales)


def test_commit_en_repo_privado_con_permiso_sube_datos(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "gh-falso")
    monkeypatch.setattr(dp, "GITHUB_USERNAME", "usuario")
    monkeypatch.setattr(dp, "GITHUB_REPO", "repo")
    monkeypatch.setattr(dp, "Github", GithubFalso)
    monkeypatch.setenv("DATAPULSE_PERMITIR_SUBIDA_DATOS", "1")
    repo = RepoFalso(private=True)
    GithubFalso.repo = repo
    dp.node_commit(_estado_commit())
    subidos = {os.path.basename(p) for p, _ in repo.creados}
    assert {"processed_data.csv", "clientes.json", "hoja.xlsx"} <= subidos


def test_commit_sin_usuario_guarda_en_local(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "gh-falso")
    monkeypatch.setattr(dp, "GITHUB_REPO", "")
    GithubFalso.pedidos = []
    monkeypatch.setattr(dp, "Github", GithubFalso)
    out = dp.node_commit(_estado_commit())
    assert out["github_url"].startswith("local://outputs/")
    assert GithubFalso.pedidos == []


def test_commit_sin_token_guarda_en_local(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "")
    out = dp.node_commit(_estado_commit())
    assert out["github_url"].startswith("local://outputs/")


def test_es_archivo_datos():
    assert dp._es_archivo_datos("processed_data.csv")
    assert dp._es_archivo_datos("/w/x.parquet")
    assert dp._es_archivo_datos("datos.json")
    assert not dp._es_archivo_datos("outputs/ts/audit_log.json")
    assert not dp._es_archivo_datos("quality_report.json")
    assert not dp._es_archivo_datos("datapulse_pipeline.py")
