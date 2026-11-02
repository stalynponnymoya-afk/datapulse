"""
Validación de la ARQUITECTURA (no de la calidad de la limpieza) con bases de
datos de otros giros, el grafo de conocimiento del usuario y sus propias APIs.

Cada caso entra por run_datapulse, sin red, con un LLM simulado genérico que
solo usa las etiquetas que el prompt le ofrece (el vocabulario sale del grafo
vigente), como haría un LLM real. El mismo simulador sirve para cualquier
dataset y cualquier grafo: no hay código específico de ninguna base.

Para cualquier dataset se comprueba que:
- los 7 nodos corren en su orden y el router solo usa retry, commit y end_no_commit;
- cada columna recibe diagnóstico y cada problema queda trazado: acción del
  grafo, respaldo del LLM, sin regla u omitido por modo;
- las acciones del grafo llegan intactas al plan y al ejecutor;
- las salidas conservan sus nombres y siempre hay informe final;
- el estado solo usa claves declaradas en DataPulseState;
- las llamadas al LLM son las documentadas en el README.
"""

import csv
import importlib.util
import json
import os
import re
import socket
import sys
from collections import Counter
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402

RUTA_MODULO = os.path.join(os.path.dirname(__file__), "..", "datapulse_prod.py")
NODOS = ("ingestar", "diagnosticar", "consultar_grafo", "planificar", "ejecutar",
         "validar_score", "commit")
SALIDAS_COMMIT = {"audit_log.json", "datapulse_pipeline.py", "quality_report.json",
                  "cleaning_log.json"}
PROBLEMA_RESUELTO_POR_LLM = "problema_del_giro_sin_regla"
PROBLEMA_SIN_RESOLVER = "otro_problema_sin_resolver"

# Palabras que no identifican nada por sí solas al comparar columnas con nodos
GENERICAS = {"variable", "valores", "valor", "formato", "invalido", "inconsistente", "fuera",
             "rango", "total", "nulos", "problema", "numerica", "continua", "discreta",
             "categorica", "nominal", "ordinal", "texto", "general", "temporal", "tipo",
             "tipos", "con", "sin", "los", "las", "del", "por", "esquema", "datos"}


@pytest.fixture(autouse=True)
def sin_red(monkeypatch):
    """Cualquier intento de abrir una conexión de red hace fallar el test."""
    def bloquear(*args, **kwargs):
        raise AssertionError("la validación no debe usar la red")

    monkeypatch.setattr(socket.socket, "connect", bloquear)
    monkeypatch.setattr(socket, "create_connection", bloquear)


# ── Bases de datos de otros giros (se generan aquí; no se suben al repo) ────

def _hospital(d):
    n = 60
    df = pd.DataFrame({
        "id_paciente": [f"P{i:04d}" for i in range(n)],
        "fecha_ingreso": [f"2024-{i % 12 + 1:02d}-{i % 28 + 1:02d}T08:30:00+01:00" for i in range(n)],
        "presion_sistolica": [None if i % 9 == 0 else 110 + (i * 7) % 60 for i in range(n)],
        "edad": [-3 if i == 5 else 18 + (i * 13) % 70 for i in range(n)],
        "diagnostico": ["Hipertensión", "diabetes ", "ASMA", "Ninguno"] * 15,
    })
    ruta = d / "hospital.csv"
    df.to_csv(ruta, index=False)
    return ruta


def _sensores(d):
    n = 96
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-06-01", periods=n, freq="h", tz="America/Lima"),
        "estacion": ["Norte", "Sur", "Centro"] * 32,
        "temperatura_c": [-5 + (i * 3) % 30 for i in range(n)],
        "humedad_pct": [None if i % 17 == 0 else 40 + i % 50 for i in range(n)],
        "lectura_raw": [-999.0 if i % 23 == 0 else round((i * 1.7) % 100, 2) for i in range(n)],
    })
    ruta = d / "sensores.parquet"
    df.to_parquet(ruta, index=False)
    return ruta


def _rrhh(d):
    n = 40
    df = pd.DataFrame({
        "Nombre Empleado": [f"Persona Numero{i}" for i in range(n)],
        "Departamento": ["Ventas", "TI", "rrhh", "Finanzas "] * 10,
        "Salario": [1200.5 + (i * 37) % 900 for i in range(n)],
        "Fecha Alta": [f"{i % 28 + 1:02d}/{i % 12 + 1:02d}/20{10 + i % 14}" for i in range(n)],
        "Activo": ["sí", "no", "Si", "NO"] * 10,
    })
    ruta = d / "rrhh.xlsx"
    df.to_excel(ruta, index=False)
    return ruta


def _logistica(d):
    n = 50
    df = pd.DataFrame({
        "pedido": [f"PED-{1000 + i}" for i in range(n)],
        "peso_kg": [-1.0 if i == 3 else round(0.5 + (i * 2.3) % 40, 2) for i in range(n)],
        "distancia_km": [None if i % 11 == 0 else (i * 13) % 800 for i in range(n)],
        "estado": ["entregado", "en ruta", "Entregado", "devuelto", None] * 10,
        "lat": [-12.0 - (i % 10) / 10 for i in range(n)],
        "lon": [-77.0 - (i % 7) / 10 for i in range(n)],
    })
    ruta = d / "logistica.json"
    df.to_json(ruta, orient="records", force_ascii=False)
    return ruta


def _finanzas(d):
    n = 45
    df = pd.DataFrame({
        "cuenta": [f"CTA{i % 9:03d}" for i in range(n)],
        "saldo": [round(-250 + (i * 91) % 1500, 2) for i in range(n)],
        "moneda": ["USD", "PEN", "usd", "EUR", None] * 9,
        "fecha_movimiento": [f"2025-{i % 12 + 1:02d}-{i % 27 + 1:02d}" for i in range(n)],
    })
    ruta = d / "finanzas.csv"
    df.to_csv(ruta, sep=";", index=False)
    return ruta


def _educacion(d):
    n = 30
    df = pd.DataFrame({
        "Estudiante": [f"Alumno {i}" for i in range(n)],
        "Calificación": [None if i % 8 == 0 else round(5 + (i * 0.37) % 15, 1) for i in range(n)],
        "Año": [2020 + i % 5 for i in range(n)],
        "Observación": [None] * n,
    })
    df = pd.concat([df, df.head(4)], ignore_index=True)          # filas duplicadas
    ruta = d / "educacion.tsv"
    df.to_csv(ruta, sep="\t", index=False)
    return ruta


def _minimo(d):
    ruta = d / "minimo.csv"
    ruta.write_text("x,y\n1,a\n2,b\n3,c\n", encoding="utf-8")
    return ruta


def _ancho(d):
    n = 25
    datos = {}
    for j in range(30):
        if j % 3 == 0:
            datos[f"medida_{j}"] = [None if (i + j) % 10 == 0 else (i * (j + 1)) % 97 for i in range(n)]
        elif j % 3 == 1:
            datos[f"categoria_{j}"] = [f"c{(i + j) % 4}" for i in range(n)]
        else:
            datos[f"importe_{j}"] = [round(((i + 1) * (j + 3)) % 500 * 1.1, 2) for i in range(n)]
    ruta = d / "ancho.csv"
    pd.DataFrame(datos).to_csv(ruta, index=False)
    return ruta


DATASETS = {"hospital": _hospital, "sensores_parquet": _sensores, "rrhh_xlsx": _rrhh,
            "logistica_json": _logistica, "finanzas_punto_y_coma": _finanzas,
            "educacion_tsv": _educacion, "minimo": _minimo, "ancho_30_columnas": _ancho}


# ── LLM simulado genérico ───────────────────────────────────────────────────

SCRIPT_GENERICO = r'''
import csv, json, sys
import pandas as pd

ruta = sys.argv[1]
ext = ruta.lower().rsplit(".", 1)[-1]
if ext in ("xlsx", "xls"):
    df = pd.read_excel(ruta)
elif ext in ("parquet", "pq"):
    df = pd.read_parquet(ruta)
elif ext == "json":
    df = pd.read_json(ruta)
else:
    muestra = open(ruta, encoding="utf-8-sig", errors="ignore").read(65536)
    try:
        sep = csv.Sniffer().sniff(muestra, delimiters=",;\t|").delimiter
    except csv.Error:
        sep = ","
    df = pd.read_csv(ruta, sep=sep, encoding="utf-8-sig")
n = len(df)
df = df.drop_duplicates().reset_index(drop=True)
for c in df.columns:
    if not pd.api.types.is_numeric_dtype(df[c]) and not pd.api.types.is_datetime64_any_dtype(df[c]):
        df[c] = df[c].astype("string").str.strip()
df.to_csv("processed_data.csv", index=False)
json.dump([{"accion": "drop_duplicates", "eliminadas": n - len(df)}], open("cleaning_log.json", "w"))
json.dump({"score_before": 0, "score_after": 0, "score": 0}, open("quality_report.json", "w"))
print('"score":', 0)
'''


def _tokens(texto):
    t = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", str(texto))
    return {x for x in re.split(r"[^a-záéíóúñü0-9]+", t.lower()) if len(x) >= 3} - GENERICAS


def _vocabulario(prompt):
    """Etiquetas que el prompt de diagnóstico ofrece, por nodo del grafo."""
    def bloque(inicio, fin):
        texto = prompt.split(inicio, 1)[1].split(fin, 1)[0]
        nodos = {}
        for linea in texto.splitlines():
            m = re.match(r"\s*· (.+)  \((\w+)\)\s*$", linea)
            if m:
                nodos[m.group(2)] = [e.strip() for e in m.group(1).split(",")]
        return nodos

    return (bloque("- tipo_inferido:", "- dtype_actual:"),
            bloque("- problemas:", "Si detectas un problema real"))


def _primera_que_contenga(nodos, fragmentos):
    for fragmento in fragmentos:
        for etiquetas in nodos.values():
            for etiqueta in etiquetas:
                if fragmento in etiqueta:
                    return etiqueta
    return None


class LLMGenerico:
    """Diagnostica, planifica y escribe el script usando solo lo que el prompt ofrece."""

    def __init__(self, omitir_columna=None, script=SCRIPT_GENERICO):
        self.llamadas = Counter()
        self.prompts = {"diagnostico": [], "plan": [], "ejecutor": []}
        self.omitir_columna = omitir_columna
        self.script = script

    def invoke(self, mensajes):
        prompt = mensajes[0].content
        if "planificador técnico" in prompt:
            tipo, respuesta = "plan", self._planificar(prompt)
        elif "Escribe el script Python COMPLETO" in prompt:
            tipo, respuesta = "ejecutor", self.script
        else:
            tipo, respuesta = "diagnostico", self._diagnosticar(prompt)
        self.llamadas[tipo] += 1
        self.prompts[tipo].append(prompt)
        return SimpleNamespace(content=respuesta)

    def _diagnosticar(self, prompt):
        tipos, problemas = _vocabulario(prompt)
        meta = json.loads(prompt.split("Metadata:\n", 1)[1].rsplit("\n\nResponde SOLO", 1)[0])
        reintento = "IMPORTANTE: diagnostica SOLO" in prompt
        diag = {}
        for i, (col, info) in enumerate(meta["columnas_info"].items()):
            if col == self.omitir_columna and not reintento:
                continue                       # el LLM "olvida" una columna
            propios = _tokens(col)
            tipo = next((etiquetas[0] for nodo, etiquetas in tipos.items()
                         if propios & _tokens(nodo)), None)
            preferidos = {"numerica": ("numerica_continua", "continua", "numer"),
                          "fecha": ("fecha", "temporal", "timestamp")}.get(
                info.get("lectura"), ("categorica_nominal", "nominal", "categ", "texto"))
            tipo = tipo or _primera_que_contenga(tipos, preferidos) or next(iter(tipos.values()))[0]
            lista = [etiquetas[0] for nodo, etiquetas in problemas.items() if propios & _tokens(nodo)]
            if info.get("null_count"):
                nulos = _primera_que_contenga(problemas, ("nulos_bajos", "nulos"))
                if nulos:
                    lista.append(nulos)
            if i == 0:
                lista.append(PROBLEMA_RESUELTO_POR_LLM)
            if i == 1:
                lista.append(PROBLEMA_SIN_RESOLVER)
            if not lista and "Ninguno" in problemas:
                lista = ["ninguno"]
            diag[col] = {"tipo_inferido": tipo, "problemas": lista or ["ninguno"],
                         "problema_principal": (lista or ["ninguno"])[0], "severidad": "media"}
            if info.get("negativos", 0) > 0:
                diag[col].update(negativos="incierto",
                                 razon_negativos="Sin contexto no se puede decidir.")
        return json.dumps(diag, ensure_ascii=False)

    @staticmethod
    def _planificar(prompt):
        acciones = json.loads(prompt.split("Acciones del grafo (no modificables):\n", 1)[1]
                              .split("\n\nProblemas sin regla en el grafo", 1)[0])
        sin_regla = json.loads(prompt.split("(problemas_fallback):\n", 1)[1]
                               .split("\n\nResponde SOLO", 1)[0])
        fallback = [{"id": f"f{i + 1}", "columna": p["columna"], "problema": p["problema"],
                     "accion": "revisar_y_documentar", "confianza": 0.95, "parametros": {},
                     "justificacion": "Respaldo del LLM."}
                    for i, p in enumerate(x for x in sin_regla
                                          if x["problema"] == PROBLEMA_RESUELTO_POR_LLM)]
        return json.dumps({"orden": [a["id"] for a in acciones] + [f["id"] for f in fallback],
                           "acciones_fallback": fallback, "brief_tecnico": "Aplicar el plan."})


# ── Instrumentación y verificaciones de arquitectura ────────────────────────

def _instrumentar(monkeypatch, modulo):
    """Registra el orden real de los nodos y las ramas que elige el router."""
    traza = []
    for nombre in NODOS:
        original = getattr(modulo, f"node_{nombre}")

        def envuelto(state, _original=original, _nombre=nombre):
            traza.append(_nombre)
            return _original(state)

        monkeypatch.setattr(modulo, f"node_{nombre}", envuelto)
    router = modulo.router_validar_score

    def router_envuelto(state):
        rama = router(state)
        traza.append(f"router:{rama}")
        return rama

    monkeypatch.setattr(modulo, "router_validar_score", router_envuelto)
    return traza


def _sin_servicios_reales(monkeypatch, modulo):
    """Modo local, sin GitHub: nunca se usan las claves del entorno."""
    for nombre in ("OPENHANDS_API_KEY", "GITHUB_TOKEN"):
        monkeypatch.setattr(modulo, nombre, "")


def _verificar_arquitectura(fs, traza, llm, modulo, omitida=None):
    # 1. Los 7 nodos en su orden; el router solo usa sus 3 ramas
    camino = " ".join(traza)
    assert re.fullmatch(r"ingestar diagnosticar consultar_grafo planificar "
                        r"(ejecutar validar_score router:retry )*"
                        r"ejecutar validar_score router:(commit commit|end_no_commit)", camino), camino

    # 2. Todas las columnas diagnosticadas y cada problema trazado
    columnas = list(fs["metadata"]["columnas_info"])
    assert sorted(fs["diagnostico"]) == sorted(columnas)
    en_plan = {(a["columna"], a["problema"]) for a in fs["plan_acciones"]}
    sin_regla = {(p["columna"], p["problema"]) for p in fs["sin_regla"]}
    omitidos = {(o["columna"], o["problema"]) for o in fs["omitidos_por_modo"]}
    for col, d in fs["diagnostico"].items():
        for problema in d["problemas"]:
            assert problema == "ninguno" or (col, problema) in en_plan | sin_regla | omitidos, (
                col, problema)
    assert (columnas[1], PROBLEMA_SIN_RESOLVER) in sin_regla

    # 3. Acciones del grafo intactas; respaldo del LLM limitado a 0.70
    del_grafo = {(col, a["problema"], a["accion"], json.dumps(a["parametros"], sort_keys=True))
                 for col, info in fs["acciones_grafo"].items() for a in info["acciones"]}
    for a in fs["plan_acciones"]:
        if a["fuente_decision"] == "knowledge_graph":
            clave = (a["columna"], a["problema"], a["accion"],
                     json.dumps(a["parametros"], sort_keys=True))
            assert clave in del_grafo, clave
        else:
            assert a["fuente_decision"] == "llm_fallback"
            assert a["confianza"] <= modulo.MAX_CONFIANZA_FALLBACK
    ejecutor = llm.prompts["ejecutor"][-1]
    for a in fs["plan_acciones"]:
        assert f'"accion": "{a["accion"]}"' in ejecutor

    # 4. Nombres de salida de siempre e informe final
    if (fs["github_url"] or "").startswith("local://"):
        carpeta = fs["github_url"].replace("local://", "")
        assert SALIDAS_COMMIT | {"processed_data.csv"} <= set(os.listdir(carpeta))
    informe = fs["informe_final"]
    assert informe and {"informe_final.md", "informe_final.json"} <= set(os.listdir(informe["ruta"]))

    # 5. Solo claves declaradas en el estado
    assert set(fs) - {"informe_final"} <= set(modulo.DataPulseState.__annotations__)

    # 6. Llamadas al LLM: diagnóstico 1 (+1 si faltaron columnas), plan 1, una por intento
    assert llm.llamadas["diagnostico"] == 1 + (1 if omitida else 0)
    assert llm.llamadas["plan"] == 1
    assert llm.llamadas["ejecutor"] == fs["intentos"]


def _correr(monkeypatch, tmp_path, ruta, llm, modulo=dp):
    traza = _instrumentar(monkeypatch, modulo)
    monkeypatch.chdir(tmp_path)
    fs = modulo.run_datapulse(str(ruta))
    return fs, traza


# ── 1. Cualquier base de datos ──────────────────────────────────────────────

@pytest.mark.parametrize("nombre", list(DATASETS))
def test_arquitectura_con_bases_de_otros_giros(nombre, tmp_path, monkeypatch):
    ruta = DATASETS[nombre](tmp_path)
    llm = LLMGenerico()
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    _sin_servicios_reales(monkeypatch, dp)
    fs, traza = _correr(monkeypatch, tmp_path, ruta, llm)
    _verificar_arquitectura(fs, traza, llm, dp)


def test_columna_olvidada_por_el_llm_se_reintenta(tmp_path, monkeypatch):
    ruta = _logistica(tmp_path)
    llm = LLMGenerico(omitir_columna="estado")
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    _sin_servicios_reales(monkeypatch, dp)
    fs, traza = _correr(monkeypatch, tmp_path, ruta, llm)
    _verificar_arquitectura(fs, traza, llm, dp, omitida="estado")
    assert any("estado" in a for a in fs["avisos_diagnostico"])


@pytest.mark.parametrize("modo", ["bi", "ml"])
def test_ambos_modos_de_salida(modo, tmp_path, monkeypatch):
    monkeypatch.setattr(dp, "MODO_SALIDA", modo)
    ruta = _ancho(tmp_path)
    llm = LLMGenerico()
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    _sin_servicios_reales(monkeypatch, dp)
    fs, traza = _correr(monkeypatch, tmp_path, ruta, llm)
    _verificar_arquitectura(fs, traza, llm, dp)
    assert fs["modo_salida"] == modo


@pytest.mark.parametrize("caso,estado,intentos", [
    ("script_que_falla", "error", dp.OH_MAX_INTENTOS_INFRA),
    ("score_inalcanzable", "finished", dp.OH_MAX_INTENTOS_SCORE),
])
def test_ramas_retry_y_end_no_commit(caso, estado, intentos, tmp_path, monkeypatch):
    """Los reintentos y el fin sin commit también recorren los 7 nodos en orden."""
    script = "raise SystemExit(1)" if caso == "script_que_falla" else SCRIPT_GENERICO
    if caso == "score_inalcanzable":
        monkeypatch.setattr(dp, "MIN_SCORE_PASS", 101)
    llm = LLMGenerico(script=script)
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    _sin_servicios_reales(monkeypatch, dp)
    fs, traza = _correr(monkeypatch, tmp_path, _hospital(tmp_path), llm)
    _verificar_arquitectura(fs, traza, llm, dp)
    assert fs["openhands_status"] == estado and fs["intentos"] == intentos
    assert traza.count("router:retry") == intentos - 1 and traza[-1] == "router:end_no_commit"
    assert fs["github_url"] is None and fs["informe_final"]["errores"]


# ── 2. El grafo de conocimiento del usuario ─────────────────────────────────

@pytest.fixture
def dominios_restaurados():
    previo = (list(dp.DOMINIOS_ACTIVOS), dp.kg, dp.KNOWLEDGE_GRAPH, dp._DOMINIOS_APLICADOS)
    yield
    (dp.DOMINIOS_ACTIVOS, dp.kg, dp.KNOWLEDGE_GRAPH, dp._DOMINIOS_APLICADOS) = previo


def agregar_reglas_logistica(kg):
    """Dominio propio de un usuario, escrito con la plantilla y sin alias."""
    dp._agregar_nodos_dominio(kg, ["Peso_Fuera_De_Rango"], ["Variable_Peso_Envio"])
    kg.add_edge("Variable_Peso_Envio", "Peso_Fuera_De_Rango",
                accion="marcar_peso_fuera_de_rango", confianza=0.9,
                parametros={"min_kg": 0, "max_kg": 1000, "flag_suffix": "_peso_invalido"},
                justificacion_es="Un envío no pesa menos de 0 ni más que la capacidad del camión.",
                justificacion_en="A shipment weighs neither below 0 nor above truck capacity.")
    kg.add_edge("Variable_Peso_Envio", "Nulos_Bajos",
                accion="fillna_peso_declarado", confianza=0.85,
                parametros={"columna_respaldo": "peso_declarado"},
                justificacion_es="Si falta el peso medido se usa el declarado.",
                justificacion_en="If the measured weight is missing the declared one is used.")
    return kg


def test_dominio_propio_del_usuario_llega_al_ejecutor(tmp_path, monkeypatch, dominios_restaurados):
    monkeypatch.setitem(dp.DOMINIOS, "logistica", agregar_reglas_logistica)
    dp.DOMINIOS_ACTIVOS = ["logistica"]
    dp.aplicar_dominios_activos()
    # Edición en caliente sobre la regla del usuario
    for _, _, datos in dp.kg.edges(data=True):
        if datos["accion"] == "marcar_peso_fuera_de_rango":
            datos["parametros"]["max_kg"] = 500

    ruta = _logistica(tmp_path)
    llm = LLMGenerico()
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    _sin_servicios_reales(monkeypatch, dp)
    fs, traza = _correr(monkeypatch, tmp_path, ruta, llm)
    _verificar_arquitectura(fs, traza, llm, dp)

    # Sus etiquetas (sin alias) se ofrecen al LLM y su regla entra al plan con sus parámetros
    assert "variable_peso_envio" in llm.prompts["diagnostico"][0]
    assert "peso_fuera_de_rango" in llm.prompts["diagnostico"][0]
    assert fs["diagnostico"]["peso_kg"]["tipo_inferido"] == "variable_peso_envio"
    regla = next(a for a in fs["plan_acciones"] if a["accion"] == "marcar_peso_fuera_de_rango")
    assert regla["fuente_decision"] == "knowledge_graph" and regla["parametros"]["max_kg"] == 500
    assert '"max_kg": 500' in llm.prompts["ejecutor"][-1]


def test_grafo_completamente_propio(tmp_path, monkeypatch):
    """Un usuario que reemplaza todo el grafo por el suyo (sin reglas base)."""
    import networkx as nx

    propio = nx.MultiDiGraph()
    propio.add_edge("Variable_Lectura", "Nulos_En_Lectura", accion="interpolar_lectura",
                    confianza=0.8, parametros={"metodo": "lineal"},
                    justificacion_es="Las lecturas son continuas en el tiempo.",
                    justificacion_en="Readings are continuous over time.")
    monkeypatch.setattr(dp, "kg", dp.validar_grafo(propio))

    ruta = _sensores(tmp_path)
    llm = LLMGenerico()
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    _sin_servicios_reales(monkeypatch, dp)
    fs, traza = _correr(monkeypatch, tmp_path, ruta, llm)
    _verificar_arquitectura(fs, traza, llm, dp)
    vocabulario = llm.prompts["diagnostico"][0]
    assert "variable_lectura" in vocabulario and "nulos_en_lectura" in vocabulario
    assert "nulos_bajos" not in vocabulario.split("- problemas:")[1].split("Si detectas")[0]


def test_grafo_mal_editado_se_detiene_antes_del_llm(tmp_path, monkeypatch):
    import networkx as nx

    roto = nx.MultiDiGraph()
    roto.add_edge("Variable_X", "Problema_X", accion="", confianza=0.8, parametros={},
                  justificacion_es="x", justificacion_en="x")
    monkeypatch.setattr(dp, "kg", roto)
    monkeypatch.setattr(dp, "get_llm", lambda: pytest.fail("no debe llamar al LLM"))
    monkeypatch.chdir(tmp_path)
    with pytest.raises(dp.GrafoInvalidoError, match="Variable_X → Problema_X.*'accion'"):
        dp.run_datapulse(str(_minimo(tmp_path)))


# ── 3. Las APIs del usuario (claves, proveedor, modelo, repo y cliente propio) ──

def _modulo_nuevo(monkeypatch, entorno, secretos_colab=None):
    """Importa datapulse_prod desde cero con la configuración del usuario."""
    for nombre in ("GROQ_API_KEY", "GITHUB_TOKEN", "OPENHANDS_API_KEY", "ANTHROPIC_API_KEY",
                   "GITHUB_USERNAME", "GITHUB_REPO", "GROQ_MODEL", "CLAUDE_MODEL",
                   "DATAPULSE_LLM", "DATAPULSE_CLAUDE_EFFORT"):
        monkeypatch.delenv(nombre, raising=False)
    for nombre, valor in entorno.items():
        monkeypatch.setenv(nombre, valor)
    if secretos_colab is not None:
        def get(nombre):
            if nombre not in secretos_colab:
                raise KeyError(nombre)
            return secretos_colab[nombre]

        colab = SimpleNamespace(userdata=SimpleNamespace(get=get))
        monkeypatch.setitem(sys.modules, "google", SimpleNamespace(colab=colab))
        monkeypatch.setitem(sys.modules, "google.colab", colab)
    spec = importlib.util.spec_from_file_location("dp_usuario", RUTA_MODULO)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


def _ningun_archivo_contiene(raiz, secreto):
    for carpeta, _, archivos in os.walk(raiz):
        for nombre in archivos:
            ruta = os.path.join(carpeta, nombre)
            with open(ruta, "rb") as f:
                assert secreto.encode() not in f.read(), ruta


def test_claude_con_la_clave_y_el_modelo_del_usuario(tmp_path, monkeypatch):
    import anthropic

    clave = "sk-ant-del-usuario-NO-REAL"
    llm = LLMGenerico()
    peticiones = []

    class AnthropicFalso:
        def __init__(self, api_key=None, **kwargs):
            assert api_key == clave
            self.beta = SimpleNamespace(messages=SimpleNamespace(stream=self._stream))

        def _stream(self, **kwargs):
            peticiones.append(kwargs["model"])
            texto = llm.invoke([SimpleNamespace(content=kwargs["messages"][0]["content"])]).content
            mensaje = SimpleNamespace(stop_reason="end_turn", model=kwargs["model"],
                                      content=[SimpleNamespace(type="text", text=texto)])

            class Flujo:
                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    return False

                def get_final_message(self):
                    return mensaje

            return Flujo()

    monkeypatch.setattr(anthropic, "Anthropic", AnthropicFalso)
    modulo = _modulo_nuevo(monkeypatch, {}, {"ANTHROPIC_API_KEY": clave,
                                            "CLAUDE_MODEL": "claude-modelo-del-usuario"})
    assert modulo.LLM_PROVEEDOR == "claude"
    _sin_servicios_reales(monkeypatch, modulo)
    fs, traza = _correr(monkeypatch, tmp_path, _finanzas(tmp_path), llm, modulo)
    _verificar_arquitectura(fs, traza, llm, modulo)
    assert set(peticiones) == {"claude-modelo-del-usuario"}
    assert clave not in os.environ.values()
    _ningun_archivo_contiene(tmp_path, clave)
    assert all(clave not in p for lista in llm.prompts.values() for p in lista)


def test_groq_con_la_clave_y_el_modelo_del_usuario(tmp_path, monkeypatch):
    clave = "gsk-del-usuario-NO-REAL"
    llm = LLMGenerico()
    creados = []

    class ChatGroqFalso:
        def __init__(self, **kwargs):
            creados.append(kwargs)

        def invoke(self, mensajes):
            return llm.invoke(mensajes)

    modulo = _modulo_nuevo(monkeypatch, {"DATAPULSE_LLM": "groq", "GROQ_API_KEY": clave,
                                         "GROQ_MODEL": "modelo-groq-del-usuario"})
    monkeypatch.setattr(modulo, "ChatGroq", ChatGroqFalso)
    assert modulo.LLM_PROVEEDOR == "groq"
    _sin_servicios_reales(monkeypatch, modulo)
    fs, traza = _correr(monkeypatch, tmp_path, _rrhh(tmp_path), llm, modulo)
    _verificar_arquitectura(fs, traza, llm, modulo)
    assert {c["model_name"] for c in creados} == {"modelo-groq-del-usuario"}
    assert {c["groq_api_key"] for c in creados} == {clave}
    _ningun_archivo_contiene(tmp_path, clave)


def test_cliente_llm_propio_de_cualquier_api(tmp_path, monkeypatch):
    """Contrato mínimo para conectar otra API: invoke(mensajes) → objeto con .content."""
    base = LLMGenerico()

    class MiClienteLLM:
        """Adaptador del usuario a su propia API de LLM (aquí simulada)."""

        def invoke(self, mensajes):
            texto = "\n\n".join(m.content for m in mensajes)
            return SimpleNamespace(content=base.invoke([SimpleNamespace(content=texto)]).content)

    monkeypatch.setattr(dp, "get_llm", lambda: MiClienteLLM())
    _sin_servicios_reales(monkeypatch, dp)
    fs, traza = _correr(monkeypatch, tmp_path, _educacion(tmp_path), base)
    _verificar_arquitectura(fs, traza, base, dp)


class RepoDelUsuario:
    def __init__(self, nombre):
        self.full_name, self.private = nombre, False
        self.ramas, self.creados = [], []

    def get_branch(self, nombre):
        return SimpleNamespace(commit=SimpleNamespace(sha=f"sha-{nombre}"))

    def create_git_ref(self, ref, sha):
        self.ramas.append(ref)

    def get_contents(self, path, ref=None):
        raise Exception("404 Not Found")

    def create_file(self, path, message, content, branch):
        self.creados.append((path, branch))


def test_commit_en_el_repo_propio_del_usuario(tmp_path, monkeypatch):
    pedidos = []
    repo = RepoDelUsuario("mi-usuario/mi-repo")

    class GithubFalso:
        def __init__(self, *args, **kwargs):
            pass

        def get_repo(self, nombre):
            pedidos.append(nombre)
            return repo

    llm = LLMGenerico()
    monkeypatch.setattr(dp, "get_llm", lambda: llm)
    monkeypatch.setattr(dp, "OPENHANDS_API_KEY", "")
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "ghp-del-usuario-NO-REAL")
    monkeypatch.setattr(dp, "GITHUB_USERNAME", "mi-usuario")
    monkeypatch.setattr(dp, "GITHUB_REPO", "mi-repo")
    monkeypatch.setattr(dp, "Github", GithubFalso)
    monkeypatch.setattr(dp, "MIN_SCORE_PASS", 0)       # basta con no empeorar
    fs, traza = _correr(monkeypatch, tmp_path, _minimo(tmp_path), llm)
    _verificar_arquitectura(fs, traza, llm, dp)

    assert traza[-1] == "commit" and set(pedidos) == {"mi-usuario/mi-repo"}
    assert fs["github_url"].startswith("https://github.com/mi-usuario/mi-repo/tree/feature/datapulse-")
    subidos = {os.path.basename(p) for p, _ in repo.creados}
    assert subidos == SALIDAS_COMMIT                    # repo público: los datos quedan en local
    _ningun_archivo_contiene(tmp_path, "ghp-del-usuario-NO-REAL")
