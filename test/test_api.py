"""
Tests de la API con TestClient (Bloque H). Sin red.
"""

import io
import os
import sys

import pandas as pd
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.main import app  # noqa: E402

client = TestClient(app)
DATA = os.path.join(os.path.dirname(__file__), "..", "data")


def _csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def test_health():
    assert client.get("/health").json() == {"status": "ok", "service": "DataPulse"}


def test_analyze_correcto():
    with open(os.path.join(DATA, "ventas_prueba.csv"), "rb") as f:
        r = client.post("/analyze", files={"file": ("ventas_prueba.csv", f, "text/csv")})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["file"] == "ventas_prueba.csv"
    assert body["shape"] == {"rows": 5, "columns": 5}
    assert 0 <= body["quality_score"] <= 100
    assert body["negative_values"]["edad"]["penalizado"] is True


def test_analyze_csv_excel_espanol():
    contenido = "nombre;edad\nJosé;30\nMuñoz;40\n".encode("cp1252")
    r = client.post("/analyze", files={"file": ("es.csv", contenido, "text/csv")})
    assert r.status_code == 200, r.text
    assert set(r.json()["data_types"]) == {"nombre", "edad"}


def test_analyze_extension_no_soportada():
    r = client.post("/analyze", files={"file": ("x.xml", b"<a/>", "text/xml")})
    assert r.status_code == 400


@pytest.mark.parametrize("contenido", [b"", b"a,b\n"])
def test_analyze_datos_invalidos_da_400(contenido):
    r = client.post("/analyze", files={"file": ("vacio.csv", contenido, "text/csv")})
    assert r.status_code == 400, r.text
    assert "Datos inválidos" in r.json()["detail"]


def test_analyze_error_inesperado_da_500(monkeypatch):
    import api.main as api_main

    def _boom(_):
        raise RuntimeError("fallo interno")

    monkeypatch.setattr(api_main, "generate_report", _boom)
    r = client.post("/analyze", files={"file": ("a.csv", b"a\n1\n", "text/csv")})
    assert r.status_code == 500


def test_analyze_archivo_demasiado_grande(monkeypatch):
    monkeypatch.setenv("DATAPULSE_API_MAX_MB", "0.001")  # ~1 KB
    grande = b"a,b\n" + b"1,2\n" * 2000
    r = client.post("/analyze", files={"file": ("g.csv", grande, "text/csv")})
    assert r.status_code == 413
    # Por debajo del límite sí se acepta
    r = client.post("/analyze", files={"file": ("p.csv", b"a,b\n1,2\n3,4\n", "text/csv")})
    assert r.status_code == 200


def test_compare_columna_nula_y_media_de_0_a_600():
    old = pd.DataFrame({
        "ventas": [0, 0, 0, 0],
        "precio": [10.0, 10.0, 10.0, 10.0],
        "vacia": [None, None, None, None],
        "estable": [5, 5, 5, 5],
        "texto": ["a", "b", "c", "d"],
    })
    new = pd.DataFrame({
        "ventas": [600, 600, 600, 600],
        "precio": [10.5, 10.5, 10.5, 10.5],
        "vacia": [1.0, 2.0, 3.0, 4.0],
        "estable": [5, 5, 5, 5],
        "texto": ["a", "b", "c", "d"],
    })
    r = client.post("/compare", files={
        "file_old": ("old.csv", _csv(old), "text/csv"),
        "file_new": ("new.csv", _csv(new), "text/csv"),
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["columnas_omitidas"] == ["vacia"]
    assert "vacia" not in body["drift_detail"]

    ventas = body["drift_detail"]["ventas"]
    assert ventas["change_pct"] is None
    assert ventas["abs_change"] == 600
    assert ventas["drifted"] is True

    assert body["drift_detail"]["precio"]["change_pct"] == 5.0
    assert body["drift_detail"]["precio"]["drifted"] is False
    assert body["drift_detail"]["estable"]["drifted"] is False
    assert "texto" not in body["drift_detail"]
    assert body["columns_compared"] == 3
    assert body["columns_drifted"] == 1
    assert body["verdict"] == "DRIFT DETECTADO"


def test_compare_media_0_sin_cambio_no_es_drift():
    df = pd.DataFrame({"x": [0, 0, 0], "f": [1.5, 2.5, 3.5]})
    r = client.post("/compare", files={
        "file_old": ("a.csv", _csv(df), "text/csv"),
        "file_new": ("b.csv", _csv(df), "text/csv"),
    })
    body = r.json()
    assert body["drift_detail"]["x"] == {
        "old_mean": 0.0, "new_mean": 0.0, "abs_change": 0.0, "change_pct": None, "drifted": False,
    }
    assert body["verdict"] == "SIN DRIFT"


def test_compare_formato_no_soportado_y_vacio():
    ok = ("a.csv", b"x\n1\n", "text/csv")
    r = client.post("/compare", files={"file_old": ok, "file_new": ("b.txt.exe", b"", "x")})
    assert r.status_code == 400
    r = client.post("/compare", files={"file_old": ok, "file_new": ("b.csv", b"", "text/csv")})
    assert r.status_code == 400


def test_compare_archivo_demasiado_grande(monkeypatch):
    monkeypatch.setenv("DATAPULSE_API_MAX_MB", "0.001")
    grande = b"a\n" + b"1\n" * 2000
    r = client.post("/compare", files={
        "file_old": ("a.csv", b"a\n1\n", "text/csv"),
        "file_new": ("b.csv", grande, "text/csv"),
    })
    assert r.status_code == 413


def test_compare_error_inesperado_da_500(monkeypatch):
    import api.main as api_main

    def _boom(*_):
        raise RuntimeError("x")

    monkeypatch.setattr(api_main, "_comparar_numericas", _boom)
    f = ("a.csv", b"a\n1\n", "text/csv")
    r = client.post("/compare", files={"file_old": f, "file_new": f})
    assert r.status_code == 500


def test_max_mb_invalido_usa_100(monkeypatch):
    import api.main as api_main

    monkeypatch.setenv("DATAPULSE_API_MAX_MB", "abc")
    assert api_main._max_bytes() == 100 * 1024 * 1024
