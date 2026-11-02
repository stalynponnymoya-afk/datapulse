"""
Tests del enmascarado de muestras antes de enviarlas al LLM (Bloque F).
"""

import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402


def _df():
    return pd.DataFrame({
        "email": ["ana.perez@gmail.com", "jose@empresa.ec", "x@y.org", "ana.perez@gmail.com"],
        "cedula": ["1712345678", "0912345678", "1798765432", "1712345678"],
        "cedula_num": [1712345678, 912345678, 1798765432, 1712345678],
        "telefono": ["+593 0991234567", "tel 022345678", "n/d", "n/d"],
        "nombre": ["José Muñoz", "Ana Pérez", "Luis Ortega", "María Paz"],
        "categoria": ["A", "B", "A", "A"],
        "ciudad": ["Quito", "Quito", "Lima", "Quito"],
        "edad": [25, 31, 29, 40],
        "nota": [None, None, None, None],
    })


def _muestras(df):
    meta = dp.serialize_metadata(df, "x.csv")
    return {c: info["muestra"] for c, info in meta["columnas_info"].items()}


def test_enmascarado_por_defecto(monkeypatch):
    monkeypatch.delenv("DATAPULSE_ENMASCARAR", raising=False)
    m = _muestras(_df())
    texto = json.dumps(m, ensure_ascii=False)

    # Emails: u***@dominio (cardinalidad 75 % → además forma, sin el usuario)
    for original in ("ana.perez", "jose@", "1712345678", "José", "Muñoz", "Pérez"):
        assert original not in texto, original

    # Cédulas: misma longitud, todo 9 (columna de texto de alta cardinalidad → forma)
    assert m["cedula"][0] == "9999999999"
    # Cédula numérica: secuencia de dígitos → 9 de la misma longitud
    assert m["cedula_num"][:2] == ["9999999999", "999999999"]
    # Nombres (texto de alta cardinalidad) → patrón de forma
    assert m["nombre"][0] == "Aaaa Aaaaa"
    assert m["nombre"][1] == "Aaa Aaaaa"
    # Categorías de baja cardinalidad intactas
    assert m["categoria"] == ["A", "B", "A"]
    assert m["ciudad"] == ["Quito", "Quito", "Lima"]
    # Números cortos intactos
    assert m["edad"] == ["25", "31", "29"]
    assert m["nota"] == []


def test_email_en_columna_de_baja_cardinalidad():
    df = pd.DataFrame({"contacto": ["soporte@empresa.com"] * 5 + ["ventas@empresa.com"] * 5})
    m = _muestras(df)
    assert m["contacto"][0] == "s***@empresa.com"


def test_digitos_en_columna_de_baja_cardinalidad():
    df = pd.DataFrame({"ref": ["CTA 1234567890", "CTA 1234567890", "CTA 1234567890", "pend"]})
    m = _muestras(df)
    assert m["ref"][0] == "CTA 9999999999"


def test_enmascarado_desactivable(monkeypatch):
    monkeypatch.setenv("DATAPULSE_ENMASCARAR", "0")
    m = _muestras(_df())
    assert m["email"][0] == "ana.perez@gmail.com"
    assert m["nombre"][0] == "José Muñoz"


def test_funciones_auxiliares():
    assert dp._patron_forma("Ab-12 ñ") == "Aa-99 a"
    assert dp._enmascarar_valor("escribe a maria.lopez@uni.edu.ec o llama 0991234567") == (
        "escribe a m***@uni.edu.ec o llama 9999999999"
    )
    assert dp._enmascarar_valor("12345") == "12345"
