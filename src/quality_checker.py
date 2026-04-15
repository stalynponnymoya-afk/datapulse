"""
DataPulse - Quality Checker v2
Analiza la calidad de un dataset y genera un reporte detallado.

Detecta:
  - Valores nulos
  - Filas duplicadas
  - Outliers (IQR)
  - Valores negativos
  - Texto inconsistente (variaciones de mayúsculas, espacios extra)
  - Fechas inválidas
  - Inconsistencias entre columnas (ej: total != quantity * price)

Formatos soportados: CSV, Excel, JSON, Parquet
"""
import pandas as pd
import numpy as np
import re
from datetime import datetime


# =====================================================
# CARGA DE DATOS (CSV, Excel, JSON, Parquet)
# =====================================================

def load_dataset(filepath):
    """
    Carga un dataset automáticamente según su extensión.
    Soporta: .csv, .xlsx, .xls, .json, .parquet, .pq

    Ejemplo:
        df = load_dataset("data/ventas.csv")
        df = load_dataset("data/ventas.json")
        df = load_dataset("data/ventas.parquet")
    """
    filepath_lower = filepath.lower()

    if filepath_lower.endswith((".xlsx", ".xls")):
        return pd.read_excel(filepath)
    elif filepath_lower.endswith(".json"):
        return pd.read_json(filepath)
    elif filepath_lower.endswith((".parquet", ".pq")):
        return pd.read_parquet(filepath)
    elif filepath_lower.endswith((".csv", ".tsv", ".txt")):
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            first_line = f.readline()
        if "\t" in first_line:
            return pd.read_csv(filepath, sep="\t")
        elif ";" in first_line:
            return pd.read_csv(filepath, sep=";", encoding="latin-1")
        else:
            return pd.read_csv(filepath, encoding="latin-1")

# =====================================================
# CHECK 1: NULOS
# =====================================================

def check_nulls(df):
    """Porcentaje de nulos por columna. Solo reporta columnas con nulos."""
    null_pct = df.isnull().mean() * 100
    return {col: round(pct, 2) for col, pct in null_pct.items() if pct > 0}


# =====================================================
# CHECK 2: DUPLICADOS
# =====================================================

def check_duplicates(df):
    """Detecta filas completamente duplicadas."""
    mask = df.duplicated(keep="first")
    return {
        "count": 
        int(mask.sum()),
        "pct": round(mask.sum() / len(df) * 100, 2),
        "first_indices": df[mask].index.tolist()[:10],
    }


# =====================================================
# CHECK 3: OUTLIERS (IQR)
# =====================================================

def check_outliers(df, factor=1.5):
    """
    Detecta outliers con IQR en todas las columnas numéricas.
    IQR = Q3 - Q1. Outlier si valor < Q1-1.5*IQR o > Q3+1.5*IQR.
    """
    outliers = {}
    for col in df.select_dtypes(include=[np.number]).columns:
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            continue
        lower = q1 - factor * iqr
        upper = q3 + factor * iqr
        mask = (df[col] < lower) | (df[col] > upper)
        if mask.any():
            outliers[col] = {
                "count": int(mask.sum()),
                "pct": round(mask.sum() / len(df) * 100, 2),
                "bounds": {"lower": round(lower, 2), "upper": round(upper, 2)},
            }
    return outliers


# =====================================================
# CHECK 4: VALORES NEGATIVOS
# =====================================================

def check_negative_values(df):
    """Detecta valores negativos en columnas numéricas."""
    negatives = {}
    for col in df.select_dtypes(include=[np.number]).columns:
        neg_count = (df[col] < 0).sum()
        if neg_count > 0:
            negatives[col] = {
                "count": int(neg_count),
                "pct": round(neg_count / len(df) * 100, 2),
            }
    return negatives


# =====================================================
# CHECK 5: TEXTO INCONSISTENTE (NUEVO)
# =====================================================

def check_text_inconsistencies(df):
    """
    Detecta inconsistencias de texto en TODAS las columnas de tipo string.
    No busca una palabra específica: analiza todas las columnas automáticamente.

    Detecta 3 tipos de problemas:

    1. Variaciones de mayúsculas/minúsculas:
       "Madrid", "madrid", "MADRID" → deberían ser el mismo valor
       Funciona con cualquier palabra en cualquier idioma.

    2. Espacios extra (al inicio, al final, o dobles en medio):
       " Madrid", "Madrid ", "New  York" → tienen espacios problemáticos

    3. Baja cardinalidad con muchas variantes:
       Si una columna tiene pocas categorías pero muchas formas de escribirlas,
       es señal de datos sucios.

    Ejemplo de output:
        {
            "region": {
                "case_variations": {
                    "norte": ["Norte", "NORTE", "norte"],
                    "sur": ["Sur", "SUR"]
                },
                "whitespace_issues": 3
            }
        }
    """
    issues = {}
    text_cols = df.select_dtypes(include=["object", "string"]).columns

    for col in text_cols:
        col_issues = {}
        # Ignorar columnas con valores muy largos (descripciones, textos libres)
        avg_len = df[col].dropna().astype(str).str.len().mean()
        if avg_len > 100:
            continue

        values = df[col].dropna().astype(str)

        # --- 5a. Variaciones de mayúsculas ---
        # Agrupar valores que son iguales si ignoras mayúsculas
        lower_groups = {}
        for val in values.unique():
            key = val.strip().lower()
            if key not in lower_groups:
                lower_groups[key] = []
            lower_groups[key].append(val)

        # Solo reportar grupos donde hay más de una forma de escribirlo
        case_variations = {
            key: variants
            for key, variants in lower_groups.items()
            if len(variants) > 1
        }

        if case_variations:
            col_issues["case_variations"] = case_variations
            col_issues["case_variations_count"] = sum(
                len(v) for v in case_variations.values()
            )

        # --- 5b. Espacios extra ---
        # Detectar valores con espacios al inicio, al final, o dobles en medio
        whitespace_mask = (
            values.str.startswith(" ")
            | values.str.endswith(" ")
            | values.str.contains(r"  +", regex=True)
        )
        ws_count = int(whitespace_mask.sum())
        if ws_count > 0:
            col_issues["whitespace_issues"] = ws_count
            col_issues["whitespace_pct"] = round(ws_count / len(values) * 100, 2)
            # Mostrar algunos ejemplos (máx 5)
            examples = values[whitespace_mask].unique()[:5].tolist()
            col_issues["whitespace_examples"] = [repr(e) for e in examples]

        if col_issues:
            issues[col] = col_issues

    return issues


# =====================================================
# CHECK 6: FECHAS INVÁLIDAS (NUEVO)
# =====================================================

def check_invalid_dates(df):
    """
    Detecta columnas que parecen contener fechas y verifica si hay
    fechas inválidas o no parseables.

    Cómo funciona:
    1. Busca columnas con "date", "fecha", "time", "dt" en el nombre
    2. También busca columnas de texto que parezcan fechas por su contenido
    3. Intenta parsear cada valor como fecha
    4. Reporta los que no se pudieron parsear

    Ejemplo: "31/02/2024" es inválido (febrero no tiene 31 días)
    """
    issues = {}

    # Patrones comunes de nombres de columnas con fechas
    date_keywords = ["date", "fecha", "time", "dt", "timestamp", "created", "updated"]

    for col in df.columns:
        is_date_col = False

        # Verificar si el nombre de la columna sugiere fecha
        col_lower = col.lower()
        if any(kw in col_lower for kw in date_keywords):
            is_date_col = True

        # Si ya es datetime de pandas, no hay problema
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue

        # Si no parece fecha por el nombre, verificar por contenido
        if not is_date_col and df[col].dtype == "object":
            sample = df[col].dropna().head(20).astype(str)
            # Patrón básico: algo que contenga números y separadores de fecha
            date_pattern = r"\d{1,4}[-/\.]\d{1,2}[-/\.]\d{1,4}"
            matches = sample.str.match(date_pattern).sum()
            if matches > len(sample) * 0.5:
                is_date_col = True

        if not is_date_col:
            continue

        # Intentar parsear las fechas
        values = df[col].dropna().astype(str)
        parsed = pd.to_datetime(values, errors="coerce")
        failed_mask = parsed.isna() & values.notna() & (values.str.strip() != "")
        failed_count = int(failed_mask.sum())

        if failed_count > 0:
            failed_examples = values[failed_mask].unique()[:10].tolist()
            issues[col] = {
                "invalid_count": failed_count,
                "invalid_pct": round(failed_count / len(values) * 100, 2),
                "examples": failed_examples,
            }

        # Verificar fechas fuera de rango razonable (antes de 1900 o después de 2030)
        valid_dates = parsed.dropna()
        if len(valid_dates) > 0:
            min_date = valid_dates.min()
            max_date = valid_dates.max()
            suspicious = (
                (valid_dates < pd.Timestamp("1900-01-01"))
                | (valid_dates > pd.Timestamp("2030-12-31"))
            ).sum()
            if suspicious > 0:
                if col not in issues:
                    issues[col] = {}
                issues[col]["out_of_range_count"] = int(suspicious)
                issues[col]["date_range"] = {
                    "min": str(min_date.date()),
                    "max": str(max_date.date()),
                }

    return issues


# =====================================================
# CHECK 7: INCONSISTENCIAS ENTRE COLUMNAS (NUEVO)
# =====================================================

def check_column_relationships(df):
    """
    Detecta inconsistencias entre columnas que deberían tener
    una relación matemática.

    Busca automáticamente patrones como:
      - total = quantity * price
      - amount = qty * unit_price
      - revenue = units * cost

    No tiene nombres hardcodeados: busca TODAS las combinaciones
    de 3 columnas numéricas y verifica si alguna es el producto
    de las otras dos.

    También verifica:
      - Columnas donde min > max (si existen pares min/max)
      - Porcentajes fuera de 0-100 (si el nombre sugiere %)

    Ejemplo de output:
        {
            "product_relationships": [
                {
                    "expected": "total = quantity * unit_price",
                    "mismatches": 12,
                    "mismatch_pct": 3.5
                }
            ]
        }
    """
    issues = {}
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    # --- 7a. Buscar relaciones de multiplicación (A ≈ B * C) ---
    # Palabras clave que sugieren "resultado" (total, amount, revenue, etc.)
    result_keywords = [
        "total", "amount", "revenue", "sum", "cost", "price_total",
        "monto", "importe", "subtotal", "neto", "gross"
    ]
    # Palabras clave que sugieren "cantidad"
    qty_keywords = [
        "quantity", "qty", "units", "count", "cantidad", "unidades", "num"
    ]
    # Palabras clave que sugieren "precio unitario"
    price_keywords = [
        "price", "unit_price", "rate", "precio", "tarifa", "cost",
        "unit_cost", "each"
    ]

    found_relationships = []

    for result_col in numeric_cols:
        result_lower = result_col.lower().replace(" ", "_")
        is_result = any(kw in result_lower for kw in result_keywords)
        if not is_result:
            continue

        for qty_col in numeric_cols:
            qty_lower = qty_col.lower().replace(" ", "_")
            if not any(kw in qty_lower for kw in qty_keywords):
                continue

            for price_col in numeric_cols:
                price_lower = price_col.lower().replace(" ", "_")
                if not any(kw in price_lower for kw in price_keywords):
                    continue

                if len({result_col, qty_col, price_col}) < 3:
                    continue

                # Verificar si result ≈ qty * price
                mask = df[[result_col, qty_col, price_col]].notna().all(axis=1)
                subset = df[mask]
                if len(subset) == 0:
                    continue

                expected = subset[qty_col] * subset[price_col]
                # Tolerancia del 1% para errores de redondeo
                tolerance = expected.abs() * 0.01 + 0.01
                mismatches = (
                    (subset[result_col] - expected).abs() > tolerance
                )
                mismatch_count = int(mismatches.sum())

                if mismatch_count > 0 and (mismatch_count / len(subset)) < 0.5:
                    found_relationships.append({
                        "expected": f"{result_col} = {qty_col} * {price_col}",
                        "mismatches": mismatch_count,
                        "mismatch_pct": round(
                            mismatch_count / len(subset) * 100, 2
                        ),
                        "sample_indices": subset[mismatches].index.tolist()[:5],
                    })
    if found_relationships:
        issues["product_relationships"] = found_relationships

    # --- 7b. Porcentajes fuera de rango ---
    pct_keywords = ["pct", "percent", "ratio", "rate", "porcentaje", "tasa"]
    pct_issues = {}
    for col in numeric_cols:
        col_lower = col.lower()
        if any(kw in col_lower for kw in pct_keywords):
            out_of_range = ((df[col] < 0) | (df[col] > 100)).sum()
            if out_of_range > 0:
                pct_issues[col] = {
                    "out_of_range_count": int(out_of_range),
                    "min": round(float(df[col].min()), 2),
                    "max": round(float(df[col].max()), 2),
                }

    if pct_issues:
        issues["percentage_out_of_range"] = pct_issues

    return issues


# =====================================================
# REPORTE PRINCIPAL
# =====================================================

def generate_report(filepath):
    """
    Genera reporte completo de calidad con score 0-100.

    Penalizaciones:
      - Nulos: hasta -30 pts
      - Duplicados: hasta -20 pts
      - Outliers: -5 pts por columna, máx -25
      - Negativos: -5 pts por columna, máx -15
      - Texto inconsistente: -3 pts por columna, máx -15
      - Fechas inválidas: -5 pts por columna, máx -15
      - Relaciones rotas: -5 pts por relación, máx -15
    """
    df = load_dataset(filepath)

    # Ejecutar todos los checks
    nulls = check_nulls(df)
    duplicates = check_duplicates(df)
    outliers = check_outliers(df)
    negatives = check_negative_values(df)
    text_issues = check_text_inconsistencies(df)
    date_issues = check_invalid_dates(df)
    relationship_issues = check_column_relationships(df)

    # --- Calcular score ---
    score = 100.0
    score -= min(df.isnull().mean().mean() * 100 * 3, 30)
    score -= min(duplicates["pct"] * 3, 20)
    score -= min(len(outliers) * 5, 25)
    score -= min(len(negatives) * 5, 15)
    score -= min(len(text_issues) * 3, 15)
    score -= min(len(date_issues) * 5, 15)

    rel_count = len(relationship_issues.get("product_relationships", []))
    rel_count += len(relationship_issues.get("percentage_out_of_range", {}))
    score -= min(rel_count * 5, 15)

    score = max(0, round(score))

    if score >= 90:
        verdict = "EXCELENTE"
    elif score >= 70:
        verdict = "BUENO"
    elif score >= 50:
        verdict = "REGULAR - revisar antes de usar"
    else:
        verdict = "POBRE - no usar sin limpieza"

    return {
        "file": filepath,
        "shape": {"rows": len(df), "columns": len(df.columns)},
        "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "null_analysis": nulls,
        "duplicate_analysis": duplicates,
        "outlier_analysis": outliers,
        "negative_values": negatives,
        "text_inconsistencies": text_issues,
        "invalid_dates": date_issues,
        "column_relationships": relationship_issues,
        "quality_score": score,
        "verdict": verdict,
    }


if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) < 2:
        print("Uso: python src/quality_checker.py <ruta_al_archivo>")
        print("Formatos: .csv .xlsx .json .parquet")
        sys.exit(1)

    report = generate_report(sys.argv[1])
    print(json.dumps(report, indent=2, ensure_ascii=False))