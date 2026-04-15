"""
DataPulse API - FastAPI
Endpoints para analizar calidad de datos via HTTP.
"""
import os
import tempfile
from fastapi import FastAPI, UploadFile, File, HTTPException

# Importar nuestro checker (está en src/)
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.quality_checker import generate_report

# Crear la app
app = FastAPI(
    title="DataPulse API",
    description="Monitor automático de calidad de datos",
    version="1.0.0",
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
    Sube un CSV o Excel, recibe reporte de calidad con score 0-100.
    """
    # Validar extensión
    allowed = (".csv", ".xlsx", ".xls", ".json", ".parquet")
    if not file.filename.lower().endswith(allowed):
        raise HTTPException(
            status_code=400,
            detail=f"Formato no soportado. Usa: {allowed}"
        )

    # Guardar archivo temporal
    suffix = os.path.splitext(file.filename)[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    # Analizar
    try:
        report = generate_report(tmp_path)
        report["file"] = file.filename  # Nombre original, no el temporal
        return report
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        os.unlink(tmp_path)  # Borrar archivo temporal


# --- Endpoint 3: Comparar dos archivos (drift detection) ---
@app.post("/compare")
async def compare(
    file_old: UploadFile = File(...),
    file_new: UploadFile = File(...),
):
    """
    Sube dos archivos (viejo y nuevo) para detectar data drift.
    Compara distribuciones de columnas numéricas.
    """
    allowed = (".csv", ".xlsx", ".xls", ".json", ".parquet")
    for f in [file_old, file_new]:
        if not f.filename.lower().endswith(allowed):
            raise HTTPException(400, f"Formato no soportado: {f.filename}")

    # Guardar archivos temporales
    paths = []
    for f in [file_old, file_new]:
        suffix = os.path.splitext(f.filename)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            content = await f.read()
            tmp.write(content)
            paths.append(tmp.name)

    try:
        from src.quality_checker import load_dataset
        df_old = load_dataset(paths[0])
        df_new = load_dataset(paths[1])

        # Comparar columnas numéricas
        drift = {}
        common_cols = set(df_old.select_dtypes(include=[float, int]).columns) & \
                      set(df_new.select_dtypes(include=[float, int]).columns)

        for col in sorted(common_cols):
            old_mean = df_old[col].mean()
            new_mean = df_new[col].mean()
            if old_mean != 0:
                change_pct = round(((new_mean - old_mean) / abs(old_mean)) * 100, 2)
            else:
                change_pct = 0.0

            drift[col] = {
                "old_mean": round(old_mean, 2),
                "new_mean": round(new_mean, 2),
                "change_pct": change_pct,
                "drifted": bool(abs(change_pct) > 10),
            }

        drifted_cols = [c for c, v in drift.items() if v["drifted"]]

        return {
            "file_old": file_old.filename,
            "file_new": file_new.filename,
            "columns_compared": len(drift),
            "columns_drifted": len(drifted_cols),
            "drift_detail": drift,
            "verdict": "DRIFT DETECTADO" if drifted_cols else "SIN DRIFT",
        }
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        for p in paths:
            os.unlink(p)
