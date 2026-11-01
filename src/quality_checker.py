"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    DATAPULSE QUALITY CHECKER v3.0                             ║
║                   Autonomous Data Quality Analyzer                          ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Módulo de análisis de calidad de datos para DataPulse.                     ║
║  Detecta problemas comunes en datasets y genera reportes detallados.        ║
║                                                                               ║
║  CAPACIDADES:                                                                ║
║  • Valores nulos y su distribución                                          ║
║  • Filas y columnas duplicadas                                              ║
║  • Outliers usando método IQR                                                ║
║  • Valores negativos en columnas numéricas                                   ║
║  • Inconsistencias de texto (mayúsculas/minúsculas, espacios)               ║
║  • Fechas inválidas o no parseables                                         ║
║  • Relaciones inconsistentes entre columnas                                  ║
║  • Análisis de tipos de variables                                           ║
║                                                                               ║
║  INTEGRACIÓN CON GRAFO DE CONOCIMIENTO:                                     ║
║  Este módulo puede consultarse para alimentar el Grafo de Conocimiento      ║
║  de DataPulse. Los problemas detectados se mapean a las categorías          ║
║  del grafo para determinar las acciones de limpieza apropiadas.             ║
╚══════════════════════════════════════════════════════════════════════════════╝

Módulo de análisis de calidad de datos para DataPulse.
Detecta problemas comunes en datasets y genera reportes detallados.

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

import csv
import codecs
import os
import re
import unicodedata
import warnings
from datetime import datetime
from typing import Dict, List, Any, Optional

import numpy as np
import pandas as pd

# ═══════════════════════════════════════════════════════════════════════════════
# CARGA DE DATOS (CSV, Excel, JSON, Parquet)
# ═══════════════════════════════════════════════════════════════════════════════

EXTENSIONES_TEXTO = (".csv", ".tsv", ".txt")
EXTENSIONES_SOPORTADAS = EXTENSIONES_TEXTO + (".xlsx", ".xls", ".json", ".parquet", ".pq")

# Orden de prueba de encodings: UTF-8 (con o sin BOM) primero; cp1252 cubre el
# CSV típico de Excel en español; latin-1 nunca falla y queda como último recurso.
_ENCODINGS_CANDIDATOS = ("utf-8-sig", "cp1252", "latin-1")
_SEPARADORES_CANDIDATOS = ",;\t|"
_TAM_MUESTRA = 64 * 1024


def _detectar_encoding(filepath: str) -> str:
    """Devuelve el primer encoding candidato que decodifica el archivo completo."""
    for encoding in _ENCODINGS_CANDIDATOS:
        decoder = codecs.getincrementaldecoder(encoding)()
        try:
            with open(filepath, "rb") as f:
                while True:
                    bloque = f.read(1024 * 1024)
                    if not bloque:
                        decoder.decode(b"", final=True)
                        break
                    decoder.decode(bloque)
            return encoding
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _detectar_separador(filepath: str, encoding: str) -> str:
    """
    Detecta el separador con csv.Sniffer sobre una muestra del archivo.
    Si el sniffer no decide, usa el candidato más frecuente en la cabecera.
    """
    with open(filepath, "r", encoding=encoding, newline="") as f:
        muestra = f.read(_TAM_MUESTRA)
    # Cortar en el último salto de línea para no analizar una fila a medias
    if len(muestra) == _TAM_MUESTRA and "\n" in muestra:
        muestra = muestra[: muestra.rfind("\n")]
    if not muestra.strip():
        return ","
    cabecera = muestra.splitlines()[0]
    conteos = {sep: cabecera.count(sep) for sep in _SEPARADORES_CANDIDATOS}
    try:
        sep = csv.Sniffer().sniff(muestra, delimiters=_SEPARADORES_CANDIDATOS).delimiter
        # Sanidad: el separador elegido debe aparecer en la cabecera
        if conteos.get(sep, 0) > 0:
            return sep
    except csv.Error:
        pass
    mejor = max(conteos, key=conteos.get)
    return mejor if conteos[mejor] > 0 else ","


def load_dataset(filepath) -> pd.DataFrame:
    """
    Carga un dataset automáticamente según su extensión.
    Soporta: .csv, .tsv, .txt, .xlsx, .xls, .json, .parquet, .pq

    Para archivos de texto detecta el encoding (utf-8-sig → cp1252 → latin-1)
    y el separador (``,`` ``;`` tabulador ``|``) con csv.Sniffer.

    Lanza ValueError si la extensión no está soportada.

    Ejemplo:
        df = load_dataset("data/ventas.csv")
        df = load_dataset("data/ventas.json")
        df = load_dataset("data/ventas.parquet")
    """
    filepath = os.fspath(filepath)
    ext = os.path.splitext(filepath)[1].lower()

    if ext not in EXTENSIONES_SOPORTADAS:
        raise ValueError(
            f"Formato no soportado: '{ext or filepath}'. "
            f"Extensiones válidas: {', '.join(EXTENSIONES_SOPORTADAS)}"
        )

    if ext in (".xlsx", ".xls"):
        try:
            return pd.read_excel(filepath)
        except ImportError as exc:
            motor = "xlrd" if ext == ".xls" else "openpyxl"
            raise ValueError(
                f"Leer '{ext}' requiere el paquete '{motor}' "
                "(ver requirements-optional.txt)."
            ) from exc
    if ext == ".json":
        try:
            return pd.read_json(filepath)
        except ValueError:
            # JSON Lines: un objeto por línea
            return pd.read_json(filepath, lines=True)
    if ext in (".parquet", ".pq"):
        return pd.read_parquet(filepath)

    encoding = _detectar_encoding(filepath)
    sep = _detectar_separador(filepath, encoding)
    return pd.read_csv(filepath, sep=sep, encoding=encoding, low_memory=False)

# ═══════════════════════════════════════════════════════════════════════════════
# UTILIDADES DE NOMBRES DE COLUMNA
# ═══════════════════════════════════════════════════════════════════════════════

def _tokens_nombre(nombre) -> List[str]:
    """
    Divide un nombre de columna en tokens en minúsculas y sin tildes.

    Separa por ``_``, espacios, guiones, puntos y camelCase, de modo que
    las palabras clave se comparan como tokens completos y no como subcadenas
    (``width`` ya no coincide con ``dt``).

    Ejemplos:
        "orderDate"        → ["order", "date"]
        "Fecha_Nacimiento" → ["fecha", "nacimiento"]
        "HTTPStatus-Code"  → ["http", "status", "code"]
    """
    texto = str(nombre)
    texto = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", texto)
    texto = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", texto)
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    return [t for t in re.split(r"[\s_\-\.]+", texto.lower()) if t]


def _nombre_contiene(nombre, palabras) -> bool:
    """True si algún token del nombre (o su singular simple) está en ``palabras``."""
    for token in _tokens_nombre(nombre):
        if token in palabras:
            return True
        if len(token) > 3 and token.endswith("s") and token[:-1] in palabras:
            return True
    return False


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

# Columnas cuyo nombre sugiere que no admiten negativos. Solo estas penalizan
# el score: longitudes geográficas, beneficios o saldos pueden ser negativos.
PALABRAS_NO_NEGATIVAS = {
    "edad", "edades", "age",
    "precio", "price",
    "cantidad", "qty", "quantity",
    "unidad", "unidades", "unit",
    "conteo", "count",
    "peso", "weight",
    "altura", "height",
    "stock",
    "duracion", "duration",
    "distancia", "distance",
}


def check_negative_values(df):
    """
    Detecta valores negativos en columnas numéricas.

    Reporta todas las columnas con negativos, pero marca ``penalizado=True``
    solo en las que, por su nombre, no deberían tener negativos
    (ver ``PALABRAS_NO_NEGATIVAS``). Solo esas restan puntos en el score.
    """
    negatives = {}
    for col in df.select_dtypes(include=[np.number]).columns:
        if pd.api.types.is_bool_dtype(df[col]):
            continue
        neg_count = (df[col] < 0).sum()
        if neg_count > 0:
            negatives[col] = {
                "count": int(neg_count),
                "pct": round(neg_count / len(df) * 100, 2),
                "penalizado": _nombre_contiene(col, PALABRAS_NO_NEGATIVAS),
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

PALABRAS_FECHA = {
    "date", "fecha", "time", "dt", "timestamp", "datetime",
    "created", "updated",
}
_PATRON_FECHA = r"\d{1,4}[-/\.]\d{1,2}[-/\.]\d{1,4}"


def _parsear_fechas(values: pd.Series) -> pd.Series:
    """
    Parsea fechas probando mes-primero y día-primero; devuelve el resultado
    con menos fallos (así «13/01/2024» no se marca como inválida).
    """
    mejor = None
    for dayfirst in (False, True):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            parsed = pd.to_datetime(values, errors="coerce", dayfirst=dayfirst)
        if mejor is None or parsed.isna().sum() < mejor.isna().sum():
            mejor = parsed
    return mejor


def check_invalid_dates(df):
    """
    Detecta columnas que parecen contener fechas y verifica si hay
    fechas inválidas o no parseables.

    Cómo funciona:
    1. Busca columnas cuyo nombre contenga como TOKEN (separando por ``_``,
       espacios, guiones y camelCase) "date", "fecha", "time", "dt", ...
    2. También busca columnas de texto que parezcan fechas por su contenido
    3. Las columnas numéricas nunca se evalúan como fecha
    4. Parsea con mes-primero y día-primero y se queda con el que menos falla
    5. Reporta los que no se pudieron parsear y las fechas fuera de rango
       (antes de 1900 o más de 5 años en el futuro)

    Ejemplo: "31/02/2024" es inválido (febrero no tiene 31 días)
    """
    issues = {}
    limite_superior = pd.Timestamp.now().normalize() + pd.DateOffset(years=5)
    limite_inferior = pd.Timestamp("1900-01-01")

    for col in df.columns:
        serie = df[col]

        # Si ya es datetime de pandas, no hay problema
        if pd.api.types.is_datetime64_any_dtype(serie):
            continue
        # Las columnas numéricas o booleanas no se evalúan como fecha
        if pd.api.types.is_numeric_dtype(serie) or pd.api.types.is_bool_dtype(serie):
            continue

        # Verificar si el nombre de la columna sugiere fecha
        is_date_col = _nombre_contiene(col, PALABRAS_FECHA)

        # Si no parece fecha por el nombre, verificar por contenido
        if not is_date_col:
            sample = serie.dropna().head(20).astype(str)
            matches = sample.str.match(_PATRON_FECHA).sum()
            if len(sample) > 0 and matches > len(sample) * 0.5:
                is_date_col = True

        if not is_date_col:
            continue

        # Intentar parsear las fechas
        values = serie.dropna().astype(str).str.strip()
        values = values[values != ""]
        if len(values) == 0:
            continue
        parsed = _parsear_fechas(values)
        failed_mask = parsed.isna()
        failed_count = int(failed_mask.sum())

        if failed_count > 0:
            failed_examples = values[failed_mask].unique()[:10].tolist()
            issues[col] = {
                "invalid_count": failed_count,
                "invalid_pct": round(failed_count / len(values) * 100, 2),
                "examples": failed_examples,
            }

        # Verificar fechas fuera de rango razonable
        valid_dates = parsed.dropna()
        if len(valid_dates) > 0:
            suspicious = (
                (valid_dates < limite_inferior) | (valid_dates > limite_superior)
            ).sum()
            if suspicious > 0:
                if col not in issues:
                    issues[col] = {}
                issues[col]["out_of_range_count"] = int(suspicious)
                issues[col]["date_range"] = {
                    "min": str(valid_dates.min().date()),
                    "max": str(valid_dates.max().date()),
                }

    return issues


# =====================================================
# CHECK 7: INCONSISTENCIAS ENTRE COLUMNAS (NUEVO)
# =====================================================

PALABRAS_PORCENTAJE = {"pct", "percent", "percentage", "porcentaje", "porc"}


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
      - Porcentajes fuera de 0-100 (solo si el nombre tiene un token de
        ``PALABRAS_PORCENTAJE``)

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
    # Solo nombres que indican porcentaje explícito. "rate", "tasa" o "ratio"
    # no implican escala 0-100 (p. ej. heart_rate, tasa de cambio).
    pct_issues = {}
    for col in numeric_cols:
        if _nombre_contiene(col, PALABRAS_PORCENTAJE):
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
# CHECK 8: TIPOS DE VARIABLES
# =====================================================

def analyze_variable_types(df):
    """
    Analiza el tipo de cada variable/columna y genera estadísticas.
    
    Returns:
        - table: DataFrame con información de cada variable
        - type_summary: Conteo de variables por tipo de dato
    
    La tabla incluye:
        - column: Nombre de la columna
        - dtype: Tipo de dato de pandas
        - inferred_type: Tipo inferred (numeric, categorical, datetime, text, boolean)
        - unique_count: Número de valores únicos
        - unique_pct: Porcentaje de valores únicos
        - null_count: Conteo de nulos
        - null_pct: Porcentaje de nulos
        - sample_values: Lista con hasta 5 valores ejemplo
    
    La lista por tipo agrupa las columnas que comparten el mismo tipo inferred.
    """
    variable_table = []
    type_groups = {}
    
    for col in df.columns:
        dtype = df[col].dtype
        non_null = df[col].dropna()
        unique_vals = non_null.unique()
        
        # Inferir tipo de variable
        if pd.api.types.is_bool_dtype(dtype):
            inferred = "boolean"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            inferred = "datetime"
        elif pd.api.types.is_numeric_dtype(dtype):
            if len(unique_vals) <= 2:
                # Pocos valores únicos puede ser categórico binario
                inferred = "binary"
            elif non_null.dtype in ['int64', 'int32', 'int16', 'int8']:
                inferred = "integer"
            else:
                inferred = "float"
        else:
            # Tipo objeto o string
            unique_pct = len(unique_vals) / len(non_null) * 100 if len(non_null) > 0 else 0
            
            if len(unique_vals) <= 10:
                # Pocas categorías = categórico
                inferred = "categorical"
            elif len(unique_vals) / len(df) < 0.05:
                # Baja cardinalidad = categórico
                inferred = "categorical"
            else:
                inferred = "text"
        
        # Agregar al grupo por tipo
        if inferred not in type_groups:
            type_groups[inferred] = []
        type_groups[inferred].append(col)
        
        # Construir fila de la tabla
        variable_table.append({
            "column": col,
            "dtype": str(dtype),
            "inferred_type": inferred,
            "unique_count": int(len(unique_vals)),
            "unique_pct": round(len(unique_vals) / len(df) * 100, 2) if len(df) > 0 else 0,
            "null_count": int(df[col].isna().sum()),
            "null_pct": round(df[col].isna().mean() * 100, 2),
            "sample_values": unique_vals[:5].tolist() if len(unique_vals) > 0 else [],
        })
    
    return {
        "variable_table": variable_table,
        "type_summary": {k: len(v) for k, v in type_groups.items()},
        "type_groups": type_groups,
    }


# =====================================================
# REPORTE PRINCIPAL
# =====================================================

def generate_report_df(df: pd.DataFrame, nombre: str = "dataset") -> Dict[str, Any]:
    """
    Genera el reporte completo de calidad de un DataFrame con score 0-100.

    Penalizaciones:
      - Nulos: hasta -30 pts
      - Duplicados: hasta -20 pts
      - Outliers: -5 pts por columna, máx -25
      - Negativos: -5 pts por columna penalizada (nombre que sugiere ≥0), máx -15
      - Texto inconsistente: -3 pts por columna, máx -15
      - Fechas inválidas: -5 pts por columna, máx -15
      - Relaciones rotas: -5 pts por relación, máx -15

    Lanza ValueError("dataset vacío") si el DataFrame no tiene filas o columnas.
    """
    if df is None or df.empty:
        raise ValueError("dataset vacío")

    # Los checks trabajan con nombres de columna string
    if not all(isinstance(c, str) for c in df.columns):
        df = df.copy()
        df.columns = [str(c) for c in df.columns]

    # Ejecutar todos los checks
    nulls = check_nulls(df)
    duplicates = check_duplicates(df)
    outliers = check_outliers(df)
    negatives = check_negative_values(df)
    text_issues = check_text_inconsistencies(df)
    date_issues = check_invalid_dates(df)
    relationship_issues = check_column_relationships(df)
    variable_types = analyze_variable_types(df)

    # --- Calcular score ---
    negativos_penalizados = sum(1 for v in negatives.values() if v.get("penalizado"))

    score = 100.0
    score -= min(df.isnull().mean().mean() * 100 * 3, 30)
    score -= min(duplicates["pct"] * 3, 20)
    score -= min(len(outliers) * 5, 25)
    score -= min(negativos_penalizados * 5, 15)
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
        "file": nombre,
        "shape": {"rows": len(df), "columns": len(df.columns)},
        "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "variable_analysis": variable_types,
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


def generate_report(filepath) -> Dict[str, Any]:
    """Carga el archivo con ``load_dataset`` y genera su reporte de calidad."""
    df = load_dataset(filepath)
    return generate_report_df(df, os.fspath(filepath))


if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) < 2:
        print("Uso: python src/quality_checker.py <ruta_al_archivo>")
        print("Formatos: .csv .xlsx .json .parquet")
        sys.exit(1)

    report = generate_report(sys.argv[1])
    print(json.dumps(report, indent=2, ensure_ascii=False))