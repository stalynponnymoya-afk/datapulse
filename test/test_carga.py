"""
Tests del cargador unificado ``load_dataset`` (Bloque A).

No usan red: generan los archivos en un directorio temporal.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.quality_checker import load_dataset  # noqa: E402


def test_utf8_conserva_tildes_y_enie(tmp_path):
    ruta = tmp_path / "clientes.csv"
    ruta.write_text("nombre,ciudad\nJosé Muñoz,Logroño\nÁngela Díaz,Cádiz\n", encoding="utf-8")
    df = load_dataset(str(ruta))
    assert df["nombre"].tolist() == ["José Muñoz", "Ángela Díaz"]
    assert df["ciudad"].tolist() == ["Logroño", "Cádiz"]


def test_utf8_con_bom(tmp_path):
    ruta = tmp_path / "bom.csv"
    ruta.write_bytes("﻿nombre,edad\nPeña,30\n".encode("utf-8"))
    df = load_dataset(str(ruta))
    assert list(df.columns) == ["nombre", "edad"]
    assert df.loc[0, "nombre"] == "Peña"


def test_latin1_con_punto_y_coma(tmp_path):
    # CSV típico de Excel en español: cp1252/latin-1 y separador ';'
    ruta = tmp_path / "excel_es.csv"
    contenido = "nombre;importe;región\nJosé Muñoz;1,5;Andalucía\nNúñez;2,0;Aragón\n"
    ruta.write_bytes(contenido.encode("latin-1"))
    df = load_dataset(str(ruta))
    assert list(df.columns) == ["nombre", "importe", "región"]
    assert df["nombre"].tolist() == ["José Muñoz", "Núñez"]
    assert df.shape == (2, 3)


def test_tsv(tmp_path):
    ruta = tmp_path / "datos.tsv"
    ruta.write_text("a\tb\tc\n1\t2\t3\n4\t5\t6\n", encoding="utf-8")
    df = load_dataset(str(ruta))
    assert list(df.columns) == ["a", "b", "c"]
    assert df["c"].tolist() == [3, 6]


def test_pipe_en_txt(tmp_path):
    ruta = tmp_path / "datos.txt"
    ruta.write_text("a|b\n1|2\n3|4\n", encoding="utf-8")
    df = load_dataset(str(ruta))
    assert df.shape == (2, 2)


def test_coma_con_campos_entrecomillados(tmp_path):
    ruta = tmp_path / "comillas.csv"
    ruta.write_text('id,nota\n1,"hola; mundo"\n2,"a; b"\n', encoding="utf-8")
    df = load_dataset(str(ruta))
    assert list(df.columns) == ["id", "nota"]
    assert df.loc[0, "nota"] == "hola; mundo"


def test_extension_no_soportada(tmp_path):
    ruta = tmp_path / "datos.xml"
    ruta.write_text("<a/>", encoding="utf-8")
    with pytest.raises(ValueError, match="no soportado"):
        load_dataset(str(ruta))


def test_sin_extension(tmp_path):
    ruta = tmp_path / "datos"
    ruta.write_text("a,b\n1,2\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_dataset(str(ruta))


def test_json_parquet_y_xlsx(tmp_path):
    df = pd.DataFrame({"x": [1, 2, 3], "y": ["á", "é", "ñ"]})

    ruta_json = tmp_path / "d.json"
    df.to_json(ruta_json, orient="records", force_ascii=False)
    pd.testing.assert_frame_equal(load_dataset(str(ruta_json)), df)

    ruta_jsonl = tmp_path / "d_lines.json"
    df.to_json(ruta_jsonl, orient="records", lines=True, force_ascii=False)
    pd.testing.assert_frame_equal(load_dataset(str(ruta_jsonl)), df)

    ruta_parquet = tmp_path / "d.parquet"
    df.to_parquet(ruta_parquet)
    pd.testing.assert_frame_equal(load_dataset(str(ruta_parquet)), df)

    ruta_xlsx = tmp_path / "d.xlsx"
    df.to_excel(ruta_xlsx, index=False)
    pd.testing.assert_frame_equal(load_dataset(str(ruta_xlsx)), df)


def test_xls_sin_motor_da_error_claro(tmp_path, monkeypatch):
    def _sin_xlrd(*args, **kwargs):
        raise ImportError("Missing optional dependency 'xlrd'")

    monkeypatch.setattr(pd, "read_excel", _sin_xlrd)
    ruta = tmp_path / "viejo.xls"
    ruta.write_bytes(b"\xd0\xcf")
    with pytest.raises(ValueError, match="xlrd"):
        load_dataset(str(ruta))


def test_node_ingestar_lee_csv_excel_espanol(tmp_path):
    from datapulse_prod import node_ingestar

    ruta = tmp_path / "ventas.csv"
    ruta.write_bytes("cliente;monto\nMuñoz;10\nPeña;20\n".encode("cp1252"))
    out = node_ingestar(
        {"filename_path": str(ruta), "file_ext": "csv", "filename": "ventas.csv"}
    )
    info = out["metadata"]["columnas_info"]
    assert set(info) == {"cliente", "monto"}
    assert out["metadata"]["filas"] == 2


def test_quality_checker_no_importa_dependencias_pesadas():
    import subprocess

    raiz = os.path.join(os.path.dirname(__file__), "..")
    codigo = (
        "import sys; import src.quality_checker; "
        "pesados = [m for m in ('langgraph', 'langchain_groq', 'github', 'datapulse_prod') "
        "if m in sys.modules]; print(pesados); assert not pesados"
    )
    proc = subprocess.run(
        [sys.executable, "-c", codigo], cwd=raiz, capture_output=True, text=True
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
