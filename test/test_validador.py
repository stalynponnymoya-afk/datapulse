"""
Tests del punto 2: fechas con zona horaria en el validador, estado
"error_validador" (sin commit ni reintento) y errores etiquetados por intento.
"""

import json
import os
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datapulse_prod as dp  # noqa: E402
from src import quality_checker as qc  # noqa: E402

CON_ZONA = ["2025-01-01 00:00:00+00:00", "2025-02-03 00:00:00+00:00", "2032-03-04 00:00:00+00:00"]


def test_fechas_con_zona_horaria_no_rompen_el_validador():
    """Antes: TypeError al comparar datetime64[us, UTC] con un Timestamp sin zona."""
    df = pd.DataFrame({"Fecha": CON_ZONA, "monto": [1.0, 2.0, 3.0]})
    res = qc.check_invalid_dates(df)
    assert res["Fecha"]["out_of_range_count"] == 1          # 2032 > hoy + 5 años
    assert qc.generate_report_df(df)["quality_score"] <= 100
    assert qc.fechas_imposibles(df["Fecha"]).tolist() == ["2032-03-04 00:00:00+00:00"]


def test_zonas_mezcladas_se_normalizan_a_utc():
    fechas = qc.parsear_fechas_flexible(pd.Series(
        ["2025-01-01", "2025-01-02T10:00:00Z", "2025-01-03T10:00:00-05:00"]))
    assert fechas.tolist() == [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02 10:00"),
                               pd.Timestamp("2025-01-03 15:00")]
    tz = pd.Series(pd.to_datetime(["2025-01-01", "1850-01-01"]).tz_localize("UTC"))
    assert qc.fechas_imposibles(tz).tolist() == [pd.Timestamp("1850-01-01")]
    assert qc.quitar_zona_horaria(tz).dt.tz is None


def test_metadata_con_fechas_con_zona(tmp_path):
    ruta = tmp_path / "d.csv"
    pd.DataFrame({"fecha": CON_ZONA * 3, "x": range(9)}).to_csv(ruta, index=False)
    meta = dp.node_ingestar({"filename_path": str(ruta), "file_ext": "csv",
                             "filename": "d.csv"})["metadata"]
    assert meta["columnas_info"]["fecha"]["lectura"] == "fecha"


def _estado(tmp_path, **extra):
    original = tmp_path / "orig.csv"
    processed = tmp_path / "processed_data.csv"
    pd.DataFrame({"a": [1, 2, 3, 4]}).to_csv(original, index=False)
    pd.DataFrame({"a": [1, 2, 3, 4]}).to_csv(processed, index=False)
    base = {"filename": "orig.csv", "filename_path": str(original), "openhands_status": "finished",
            "ruta_processed": str(processed), "intentos": 2}
    base.update(extra)
    return base


def test_excepcion_del_validador_es_error_validador(tmp_path, monkeypatch):
    real = dp.generate_report_df

    def _falla_en_processed(df, nombre="dataset", **kw):
        if nombre == "processed_data.csv":
            raise TypeError("Invalid comparison between dtype=datetime64[us, UTC] and Timestamp")
        return real(df, nombre, **kw)

    monkeypatch.setattr(dp, "generate_report_df", _falla_en_processed)
    out = dp.node_validar_score(_estado(tmp_path))
    assert out["openhands_status"] == "error_validador"
    assert out["score_despues"] == 0
    assert out["error_history"][0].startswith("[intento 2] El validador falló sobre processed_data.csv")
    # Sin reintento ni commit, con cualquier presupuesto
    for intentos in (1, 2, 3):
        assert dp.router_validar_score({**out, "intentos": intentos}) == "end_no_commit"


def test_excepcion_del_validador_sobre_el_original(tmp_path, monkeypatch):
    def _falla(*a, **k):
        raise RuntimeError("roto")

    monkeypatch.setattr(dp, "generate_report_df", _falla)
    out = dp.node_validar_score(_estado(tmp_path, intentos=1))
    assert out["openhands_status"] == "error_validador"
    assert "el dataset original" in out["error_history"][0]


def test_processed_vacio_sigue_siendo_score_cero_con_reintento(tmp_path):
    estado = _estado(tmp_path, score_antes=50)
    open(estado["ruta_processed"], "w").close()
    out = dp.node_validar_score(estado)
    assert "openhands_status" not in out and out["score_despues"] == 0
    assert out["error_history"][0].startswith("[intento 2] Validación independiente")
    assert dp.router_validar_score({**estado, **out}) == "retry"


def test_langgraph_error_validador_un_intento_e_informe(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(dp, "OPENHANDS_API_KEY", "")
    monkeypatch.setattr(dp, "GITHUB_TOKEN", "")
    real = dp.generate_report_df

    def _falla_en_processed(df, nombre="dataset", **kw):
        if nombre == "processed_data.csv":
            raise TypeError("comparación de fechas con zona")
        return real(df, nombre, **kw)

    monkeypatch.setattr(dp, "generate_report_df", _falla_en_processed)

    class LLM:
        def invoke(self, mensajes):
            p = mensajes[0].content
            if "planificador técnico" in p:
                return SimpleNamespace(content=json.dumps(
                    {"orden": [], "acciones_fallback": [], "brief_tecnico": "B"}))
            if "Escribe el script Python COMPLETO" in p:
                return SimpleNamespace(content=("import sys, shutil\n"
                                                "shutil.copy(sys.argv[1], 'processed_data.csv')\n"))
            return SimpleNamespace(content=json.dumps(
                {"a": {"tipo_inferido": "numerica_discreta", "problemas": ["ninguno"]}}))

    monkeypatch.setattr(dp, "get_llm", lambda: LLM())
    datos = tmp_path / "d.csv"
    datos.write_text("a\n1\n2\n3\n", encoding="utf-8")

    final = dp.run_datapulse(str(datos))
    assert final["openhands_status"] == "error_validador"
    assert final["intentos"] == 1 and final["github_url"] is None
    resumen = final["informe_final"]["resumen"]
    assert "El validador falló" in resumen
    assert "Último error (intento 1): El validador falló sobre processed_data.csv" in resumen


@pytest.mark.parametrize("estado, prefijo", [
    ({"intentos": 0}, "[intento 1] "),
    ({"intentos": 2}, "[intento 3] "),
])
def test_errores_de_ejecutar_llevan_el_intento(tmp_path, monkeypatch, estado, prefijo):
    monkeypatch.chdir(tmp_path)

    class LLMRoto:
        def invoke(self, _):
            raise RuntimeError("sin red")

    monkeypatch.setattr(dp, "get_llm", lambda: LLMRoto())
    datos = tmp_path / "d.csv"
    datos.write_text("a\n1\n", encoding="utf-8")
    res = dp._run_local_groq_pipeline({"filename": "d.csv", "filename_path": str(datos),
                                       "file_ext": "csv", "metadata": {}, "brief_tecnico": "",
                                       "error_history": [], **estado})
    assert res["error_history"][0].startswith(prefijo)
