"""
DataPulse — pipeline de limpieza reproducible.

Ejecuta, en el orden de prioridad indicado, las transformaciones solicitadas
sobre data/ventas_prueba.csv y escribe:
  - processed_data.csv  (dataset limpio)
  - cleaning_log.json   (auditoría de decisiones)
  - quality_report.json (score_before / score_after / score)

Uso:
    python datapulse_pipeline.py
"""

import json
import os
import re
import sys

import numpy as np
import pandas as pd
from sklearn.impute import KNNImputer

DATA_PATH = "data/ventas_prueba.csv"
PROCESSED_PATH = "processed_data.csv"
REPORT_PATH = "quality_report.json"
LOG_PATH = "cleaning_log.json"

EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

_cleaning_log = []


def _log(columna, accion, justificacion):
    _cleaning_log.append(
        {"columna": columna, "accion": accion, "justificacion": justificacion}
    )


# ── 1. Duplicados ────────────────────────────────────────────────

def drop_duplicates(df, subset, keep="first"):
    """Elimina duplicados conservando la primera aparición."""
    antes = len(df)
    df = df.drop_duplicates(subset=subset, keep=keep, ignore_index=True)
    _log(
        subset,
        "drop_duplicates",
        f"Eliminados {antes - len(df)} duplicados en {subset} (keep='first').",
    )
    return df


# ── 2. Centinelas ────────────────────────────────────────────────

def replace_sentinel(series, sentinel=-999):
    """Reemplaza valores centinela por NaN."""
    n = int((series == sentinel).sum())
    series = series.replace(sentinel, np.nan)
    _log(
        series.name,
        "replace_sentinel",
        f"{n} valores centinela ({sentinel}) reemplazados por NaN.",
    )
    return series


# ── 3. Limpieza de strings / validación ──────────────────────────

def validate_email_format(series, exclude_from_model=True):
    """
    Valida el patrón de correo sin modificar el contenido.
    Devuelve (serie_original, info) y marca la columna como excluida del modelo.
    """
    validos = series.dropna().astype(str).str.match(EMAIL_PATTERN)
    n_invalid = int((~validos).sum())
    _log(
        series.name,
        "validate_email_format",
        f"Validación de patrón: {n_invalid} correos inválidos. "
        f"Contenido no modificado; columna excluida del entrenamiento "
        f"(exclude_from_model={exclude_from_model}).",
    )
    return series, {"exclude_from_model": exclude_from_model, "invalid": n_invalid}


# ── 4. Imputaciones ──────────────────────────────────────────────

def knn_impute(df, columns, n_neighbors=5, weights="distance"):
    """Imputa NaN numéricos con KNNImputer."""
    imputer = KNNImputer(n_neighbors=n_neighbors, weights=weights)
    df[columns] = imputer.fit_transform(df[columns])
    _log(
        ",".join(columns),
        "knn_impute",
        f"KNNImputer(n_neighbors={n_neighbors}, weights='{weights}') aplicado.",
    )
    return df


def simple_imputer_median(series):
    """Imputa NaN con la mediana de la propia columna."""
    median = series.median()
    series = series.fillna(median)
    _log(
        series.name,
        "simple_imputer_median",
        f"Nulos imputados con la mediana ({median}).",
    )
    return series


# ── 5. Outliers / rangos inválidos ───────────────────────────────

def flag_and_nan(series, flag_name):
    """Marca valores negativos en una columna flag y los convierte a NaN."""
    flag = (series < 0).astype(int)
    n = int(flag.sum())
    series = series.mask(series < 0, np.nan)
    _log(
        series.name,
        "flag_and_nan",
        f"{n} valores negativos marcados en '{flag_name}' y convertidos a NaN.",
    )
    return series, flag


def flag_and_clip(series, flag_name, clip_min=0):
    """Marca valores negativos en una columna flag y los recorta a clip_min."""
    flag = (series < 0).astype(int)
    n = int(flag.sum())
    series = series.clip(lower=clip_min)
    _log(
        series.name,
        "flag_and_clip",
        f"{n} valores negativos marcados en '{flag_name}' y recortados a {clip_min}.",
    )
    return series, flag


# ── 6. Categóricas ───────────────────────────────────────────────

def fillna_categorical(series, value="Unknown"):
    """Sustituye nulos por una categoría literal."""
    n = int(series.isna().sum())
    series = series.fillna(value)
    _log(
        series.name,
        "fillna_categorical",
        f"{n} nulos sustituidos por la categoría '{value}'.",
    )
    return series


# ── Score ────────────────────────────────────────────────────────

def _quality_score(path):
    """Score 0-100 usando el checker del repo; fallback mínimo si no está."""
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.quality_checker import generate_report

        return int(generate_report(path)["quality_score"])
    except Exception as exc:  # pragma: no cover - defensivo
        print(f"   ⚠️ No se pudo usar src.quality_checker: {exc}")
        df = pd.read_csv(path)
        null_ratio = float(df.isnull().mean().mean())
        return max(0, round(100 - null_ratio * 100 * 3))


def main():
    score_before = _quality_score(DATA_PATH)

    df = pd.read_csv(DATA_PATH)

    # Orden de prioridad: duplicados → centinelas → strings → tipos →
    # imputaciones → outliers → encoding/escalado (no requerido).

    # 1) Duplicados por id_cliente
    df = drop_duplicates(df, subset=["id_cliente"], keep="first")

    # 2) Centinelas en edad
    df["edad"] = replace_sentinel(df["edad"], sentinel=-999)

    # 3) Limpieza / validación de strings (email) — sin modificar contenido
    df["email"], _email_meta = validate_email_format(df["email"], exclude_from_model=True)

    # 4) Conversión de tipos (si fuera necesario)
    df["edad"] = pd.to_numeric(df["edad"], errors="coerce")
    df["monto_transaccion"] = pd.to_numeric(df["monto_transaccion"], errors="coerce")

    # 5) Imputaciones: replace_sentinel → knn_impute → simple_imputer_median
    df = knn_impute(df, columns=["edad"], n_neighbors=5, weights="distance")
    df["monto_transaccion"] = simple_imputer_median(df["monto_transaccion"])

    # 6) Outliers / rangos inválidos
    df["edad"], df["edad_negative_flag"] = flag_and_nan(df["edad"], "edad_negative_flag")
    df["monto_transaccion"], df["monto_transaccion_negative_flag"] = flag_and_clip(
        df["monto_transaccion"], "monto_transaccion_negative_flag", clip_min=0
    )

    # 7) Categórica: nulos → 'Unknown' (sin eliminar ni codificar)
    df["categoria"] = fillna_categorical(df["categoria"], value="Unknown")

    # 8) Encoding / escalado: no requerido en este caso.

    df.to_csv(PROCESSED_PATH, index=False)

    with open(LOG_PATH, "w", encoding="utf-8") as fh:
        json.dump(_cleaning_log, fh, indent=2, ensure_ascii=False)

    score_after = _quality_score(PROCESSED_PATH)
    score = score_after

    with open(REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(
            {"score_before": score_before, "score_after": score_after, "score": score},
            fh,
        )

    print(df.to_string())
    print(f"score_before: {score_before} | score_after: {score_after}")
    print('"score":', score_after)


if __name__ == "__main__":
    main()
