"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                        DATAPULSE API v3.0                                     ║
║                   FastAPI REST Endpoints                                     ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  API REST para análisis de calidad de datos mediante HTTP.                  ║
║                                                                               ║
║  ENDPOINTS:                                                                  ║
║  • GET  /health          - Health check                                      ║
║  • POST /analyze          - Analiza archivo y genera reporte de calidad       ║
║  • POST /compare          - Detecta data drift entre dos archivos            ║
║                                                                               ║
║  INTEGRACIÓN:                                                                ║
║  Esta API se conecta con el módulo quality_checker.py                        ║
║  para realizar el análisis y puede integrarse con el pipeline principal      ║
║  de DataPulse para ejecutarse como servicio web.                             ║
╚══════════════════════════════════════════════════════════════════════════════╝

DataPulse API - FastAPI
Endpoints para analizar calidad de datos via HTTP.
"""

import os
import sys
import tempfile
from typing import List

import numpy as np
from fastapi import FastAPI, File, HTTPException, UploadFile

# Importar nuestro checker (está en src/)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.quality_checker import (  # noqa: E402
    EXTENSIONES_SOPORTADAS,
    generate_report,
    load_dataset,
)

# Tamaño máximo por archivo subido (MB), configurable por entorno
_MB = 1024 * 1024
UMBRAL_DRIFT_PCT = 10.0


def _max_bytes() -> int:
    """Límite de tamaño por archivo; DATAPULSE_API_MAX_MB (por defecto 100)."""
    try:
        mb = float(os.environ.get("DATAPULSE_API_MAX_MB", "100"))
    except ValueError:
        mb = 100.0
    return int(mb * _MB)


def _validar_extension(nombre: str) -> None:
    if not (nombre or "").lower().endswith(EXTENSIONES_SOPORTADAS):
        raise HTTPException(
            status_code=400,
            detail=f"Formato no soportado: {nombre}. Usa: {EXTENSIONES_SOPORTADAS}",
        )


async def _guardar_temporal(file: UploadFile) -> str:
    """
    Copia el archivo subido a un temporal leyendo por bloques y corta con 413
    si supera el tamaño máximo. Devuelve la ruta del temporal.
    """
    limite = _max_bytes()
    suffix = os.path.splitext(file.filename or "")[1]
    total = 0
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        ruta = tmp.name
        while True:
            bloque = await file.read(_MB)
            if not bloque:
                break
            total += len(bloque)
            if total > limite:
                tmp.close()
                os.unlink(ruta)
                raise HTTPException(
                    status_code=413,
                    detail=(
                        f"Archivo demasiado grande: {file.filename} supera "
                        f"{limite / _MB:g} MB (DATAPULSE_API_MAX_MB)."
                    ),
                )
            tmp.write(bloque)
    return ruta


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN DE LA APP
# ═══════════════════════════════════════════════════════════════════════════════

app = FastAPI(
    title="DataPulse API",
    description="""
## DataPulse API v3.0

API REST para análisis automático de calidad de datos.

### Características:
- **Análisis de calidad**: Detecta nulos, duplicados, outliers, inconsistencias
- **Comparación de datasets**: Detecta data drift entre versiones
- **Multi-formato**: Soporta CSV, Excel, JSON, Parquet
- **Score de calidad**: Genera puntuación 0-100 con veredicto

### Integración con Grafo de Conocimiento:
Los resultados del análisis pueden alimentar el Grafo de Conocimiento
de DataPulse para determinar las acciones de limpieza apropiadas.
    """,
    version="3.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)


# --- Endpoint 1: Health Check ---
@app.get("/health")
def health():
    """Verifica que el servicio está vivo."""
    return {"status": "ok", "service": "DataPulse"}


# --- Endpoint 2: Analizar un archivo ---
@app.post("/analyze")
async def analyze(file: UploadFile = File(...)):
    """
    Sube un CSV, Excel, JSON o Parquet y recibe el reporte de calidad (0-100).

    - 400: formato no soportado o datos inválidos (p. ej. dataset vacío).
    - 413: el archivo supera DATAPULSE_API_MAX_MB.
    """
    _validar_extension(file.filename)
    tmp_path = await _guardar_temporal(file)

    try:
        report = generate_report(tmp_path)
        report["file"] = file.filename  # Nombre original, no el temporal
        return report
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Datos inválidos: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        os.unlink(tmp_path)  # Borrar archivo temporal


def _comparar_numericas(df_old, df_new) -> dict:
    """
    Compara las medias de las columnas numéricas comunes.

    - Columnas 100 % nulas en alguno de los dos archivos: se omiten y se
      listan en ``columnas_omitidas``.
    - Media anterior 0: ``change_pct`` es None y hay drift si cambió
      (``abs_change`` != 0).
    """
    num_old = set(df_old.select_dtypes(include=[np.number]).columns)
    num_new = set(df_new.select_dtypes(include=[np.number]).columns)
    comunes = sorted(num_old & num_new, key=str)

    drift = {}
    omitidas: List[str] = []
    for col in comunes:
        if df_old[col].isna().all() or df_new[col].isna().all():
            omitidas.append(str(col))
            continue
        old_mean = float(df_old[col].mean())
        new_mean = float(df_new[col].mean())
        abs_change = new_mean - old_mean
        if old_mean != 0:
            change_pct = round(abs_change / abs(old_mean) * 100, 2)
            drifted = abs(change_pct) > UMBRAL_DRIFT_PCT
        else:
            change_pct = None
            drifted = abs_change != 0
        drift[str(col)] = {
            "old_mean": round(old_mean, 2),
            "new_mean": round(new_mean, 2),
            "abs_change": round(abs_change, 4),
            "change_pct": change_pct,
            "drifted": bool(drifted),
        }
    return {"drift": drift, "columnas_omitidas": omitidas}


# --- Endpoint 3: Comparar dos archivos (drift detection) ---
@app.post("/compare")
async def compare(
    file_old: UploadFile = File(...),
    file_new: UploadFile = File(...),
):
    """
    Sube dos archivos (viejo y nuevo) para detectar data drift.
    Compara las medias de las columnas numéricas comunes.

    - 400: formato no soportado o datos inválidos.
    - 413: algún archivo supera DATAPULSE_API_MAX_MB.
    """
    for f in (file_old, file_new):
        _validar_extension(f.filename)

    paths: List[str] = []
    try:
        for f in (file_old, file_new):
            paths.append(await _guardar_temporal(f))

        df_old = load_dataset(paths[0])
        df_new = load_dataset(paths[1])
        resultado = _comparar_numericas(df_old, df_new)
        drift = resultado["drift"]
        drifted_cols = [c for c, v in drift.items() if v["drifted"]]

        return {
            "file_old": file_old.filename,
            "file_new": file_new.filename,
            "columns_compared": len(drift),
            "columns_drifted": len(drifted_cols),
            "drift_detail": drift,
            "columnas_omitidas": resultado["columnas_omitidas"],
            "verdict": "DRIFT DETECTADO" if drifted_cols else "SIN DRIFT",
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(400, f"Datos inválidos: {e}")
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        for p in paths:
            os.unlink(p)
