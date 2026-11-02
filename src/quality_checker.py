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


def _normalizar_palabra(palabra: str) -> str:
    """Minúsculas y sin tildes, igual que los tokens de ``_tokens_nombre``."""
    texto = unicodedata.normalize("NFKD", str(palabra).lower())
    return "".join(c for c in texto if not unicodedata.combining(c))


def _nombre_contiene(nombre, palabras) -> bool:
    """
    True si algún token del nombre (o su singular simple: -s, -es) está en
    ``palabras``. Las palabras se comparan sin tildes ni mayúsculas.
    """
    claves = {_normalizar_palabra(p) for p in palabras}
    for token in _tokens_nombre(nombre):
        candidatos = {token}
        if len(token) > 3 and token.endswith("s"):
            candidatos.add(token[:-1])
        if len(token) > 4 and token.endswith("es"):
            candidatos.add(token[:-2])
        if candidatos & claves:
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

def columnas_indice(df) -> List[str]:
    """
    Columnas índice: enteros sin nulos, todos distintos y consecutivos
    (p. ej. ``Row`` = 1..N). No son datos: hacen que cada fila parezca única
    y esconden los duplicados reales.
    """
    indices = []
    if len(df) < 2:
        return indices
    for col in df.columns:
        serie = df[col]
        if not pd.api.types.is_integer_dtype(serie) or pd.api.types.is_bool_dtype(serie):
            continue
        if serie.isna().any() or not serie.is_unique:
            continue
        if int(serie.max()) - int(serie.min()) == len(serie) - 1:
            indices.append(col)
    return indices


def check_duplicates(df):
    """
    Detecta filas completamente duplicadas, sin tener en cuenta las columnas
    índice (que harían que toda fila pareciera única).
    """
    ignoradas = columnas_indice(df)
    datos = df.drop(columns=ignoradas) if 0 < len(ignoradas) < len(df.columns) else df
    mask = datos.duplicated(keep="first")
    return {
        "count": int(mask.sum()),
        "pct": round(mask.sum() / len(df) * 100, 2),
        "first_indices": df[mask].index.tolist()[:10],
        "columnas_ignoradas": [str(c) for c in ignoradas],
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

# FUENTE ÚNICA de las columnas que no admiten negativos. La usan
# check_negative_values (qué negativos penalizan el score) y la regla 4 de
# DIAGNOSTIC_PROMPT en datapulse_prod.py (qué negativos ve el LLM como error).
# Se comparan como tokens del nombre, sin tildes y admitiendo plurales.
PALABRAS_NO_NEGATIVAS = (
    "monto", "amount",
    "importe",
    "precio", "price",
    "costo", "cost",
    "edad", "age",
    "peso", "weight",
    "altura", "height",
    "cantidad", "quantity", "qty",
    "unidades", "units",
    "conteo", "count",
    "stock",
    "distancia", "distance",
    "duración", "duration",
)

# Excepciones: si el nombre contiene alguna de estas palabras, los negativos
# son válidos aunque también contenga una de PALABRAS_NO_NEGATIVAS
# (p. ej. "saldo_monto", "balance_amount", "margin_price").
PALABRAS_NEGATIVOS_PERMITIDOS = ("saldo", "balance", "margen", "margin")


def admite_negativos(nombre) -> bool:
    """False si el nombre sugiere una magnitud ≥ 0 (y no es una excepción)."""
    if _nombre_contiene(nombre, PALABRAS_NEGATIVOS_PERMITIDOS):
        return True
    return not _nombre_contiene(nombre, PALABRAS_NO_NEGATIVAS)


DECISIONES_NEGATIVOS_VALIDAS = ("validos", "invalidos", "incierto")


def normalizar_decision_negativos(valor) -> Optional[str]:
    """'validos' | 'invalidos' | 'incierto' (sin tildes ni mayúsculas) o None."""
    if isinstance(valor, dict):
        valor = valor.get("decision")
    if not isinstance(valor, str):
        return None
    valor = _normalizar_palabra(valor.strip())
    return valor if valor in DECISIONES_NEGATIVOS_VALIDAS else None


def discrepancia_negativos(col, decision) -> Optional[str]:
    """
    Control cruzado con PALABRAS_NO_NEGATIVAS: texto de la discrepancia si la
    lista y una decisión firme ("validos" / "invalidos") no coinciden. Una
    decisión "incierto" no se contrasta (no afirma nada).
    """
    decision = normalizar_decision_negativos(decision)
    if decision not in ("validos", "invalidos"):
        return None
    lista_invalidos = not admite_negativos(col)
    if (decision == "invalidos") == lista_invalidos:
        return None
    if lista_invalidos:
        return (f"La lista de control considera que '{col}' no admite negativos, "
                f"pero la decisión es '{decision}'.")
    return (f"La lista de control no incluye '{col}' entre las columnas sin negativos, "
            f"pero la decisión es '{decision}'.")


def check_negative_values(df, decisiones: Optional[Dict[str, Any]] = None):
    """
    Detecta valores negativos en columnas numéricas.

    Sin decisiones (API, línea de comandos): marca ``penalizado=True`` solo en
    las columnas que, por su nombre, no deberían tener negativos
    (``PALABRAS_NO_NEGATIVAS`` salvo ``PALABRAS_NEGATIVOS_PERMITIDOS``).

    Con las decisiones del diagnóstico ({columna: "validos" | "invalidos" |
    "incierto"} o {columna: {"decision": ..., "fuente": ...}}): "invalidos"
    penaliza, "validos" e "incierto" no; la lista queda solo como control
    cruzado (``discrepancia``, que no afecta al score). Las columnas sin
    decisión siguen el criterio de la lista.
    """
    decisiones = {str(k): v for k, v in (decisiones or {}).items()} if decisiones is not None else None
    negatives = {}
    for col in df.select_dtypes(include=[np.number]).columns:
        if pd.api.types.is_bool_dtype(df[col]):
            continue
        neg_count = (df[col] < 0).sum()
        if neg_count > 0:
            entrada = {
                "count": int(neg_count),
                "pct": round(neg_count / len(df) * 100, 2),
                "penalizado": not admite_negativos(col),
            }
            if decisiones is not None:
                bruto = decisiones.get(str(col))
                decision = normalizar_decision_negativos(bruto)
                if decision:
                    entrada.update({
                        "decision": decision,
                        "fuente": bruto.get("fuente") if isinstance(bruto, dict) else None,
                        "criterio": "diagnostico",
                        "penalizado": decision == "invalidos",
                        "discrepancia": discrepancia_negativos(col, decision),
                    })
                else:
                    entrada["criterio"] = "lista"
            negatives[col] = entrada
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


def quitar_zona_horaria(fechas):
    """
    Fechas con zona horaria → UTC sin zona, para poder compararlas con
    Timestamp sin zona (antes: TypeError con datetime64[us, UTC]). Las fechas
    sin zona se devuelven igual.
    """
    if isinstance(fechas, pd.Series):
        if isinstance(fechas.dtype, pd.DatetimeTZDtype):
            return fechas.dt.tz_convert("UTC").dt.tz_localize(None)
        return fechas
    if getattr(fechas, "tz", None) is not None:
        return fechas.tz_convert("UTC").tz_localize(None)
    return fechas


def _a_fechas(valores, **kwargs) -> pd.Series:
    """pd.to_datetime tolerante a zonas mezcladas: todo a UTC y sin zona."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return quitar_zona_horaria(pd.to_datetime(valores, errors="coerce", utc=True, **kwargs))


def _parsear_fechas(values: pd.Series) -> pd.Series:
    """
    Parsea fechas probando mes-primero y día-primero; devuelve el resultado
    con menos fallos (así «13/01/2024» no se marca como inválida).
    """
    mejor = None
    for dayfirst in (False, True):
        parsed = _a_fechas(values, dayfirst=dayfirst)
        if mejor is None or parsed.isna().sum() < mejor.isna().sum():
            mejor = parsed
    return mejor


# ═══════════════════════════════════════════════════════════════════════════════
# UTILIDADES DE ANÁLISIS POR COLUMNA (las usa la metadata que ve el LLM)
# ═══════════════════════════════════════════════════════════════════════════════

UMBRAL_MAYORMENTE = 0.8          # fracción de valores para "mayormente numérica/fecha"
_RE_COMA_DECIMAL = re.compile(r"^-?\d+,\d{1,2}$")


def convertir_numerico(serie: pd.Series) -> pd.Series:
    """
    Convierte una columna a número valor a valor (NaN si no se puede).
    Acepta coma decimal simple ("19,99"). Mantiene el índice de la serie.
    """
    if pd.api.types.is_numeric_dtype(serie) and not pd.api.types.is_bool_dtype(serie):
        return serie.astype(float)
    texto = serie.astype("string").str.strip()
    texto = texto.where(~texto.str.match(_RE_COMA_DECIMAL, na=False),
                        texto.str.replace(",", ".", regex=False))
    return pd.to_numeric(texto, errors="coerce")


def es_mayormente_numerica(serie: pd.Series, umbral: float = UMBRAL_MAYORMENTE) -> bool:
    """Columna de texto/objeto cuyos valores no nulos son números en ≥ umbral."""
    if pd.api.types.is_bool_dtype(serie) or pd.api.types.is_numeric_dtype(serie):
        return False
    no_nulos = serie.dropna()
    if len(no_nulos) == 0:
        return False
    return convertir_numerico(no_nulos).notna().mean() >= umbral


def extremos_iqr(numeros: pd.Series, factor: float = 1.5) -> pd.Series:
    """Valores fuera de [Q1 - f·IQR, Q3 + f·IQR], del más alejado al menos."""
    numeros = numeros.dropna()
    if len(numeros) < 4:
        return numeros.iloc[0:0]
    q1, q3 = numeros.quantile(0.25), numeros.quantile(0.75)
    iqr = q3 - q1
    if iqr == 0:
        return numeros.iloc[0:0]
    bajo, alto = q1 - factor * iqr, q3 + factor * iqr
    fuera = numeros[(numeros < bajo) | (numeros > alto)]
    distancia = (fuera - bajo).abs().where(fuera < bajo, (fuera - alto).abs())
    return fuera.loc[distancia.sort_values(ascending=False).index]


_RE_FECHA_DMY = re.compile(r"^(\d{1,2})[/\-.](\d{1,2})[/\-.]\d{2,4}$")


def _prefiere_dia_primero(valores: pd.Series) -> bool:
    """
    Decide el orden día/mes de las fechas ambiguas (dd/mm vs mm/dd) con la
    evidencia de la propia columna: un primer número > 12 solo puede ser día
    y un segundo número > 12 solo puede ser día. En empate, día primero.
    """
    partes = valores.astype(str).str.extract(_RE_FECHA_DMY).dropna().astype(int)
    if partes.empty:
        return True
    return int((partes[0] > 12).sum()) >= int((partes[1] > 12).sum())


_RE_ANIO_PRIMERO = re.compile(r"^\d{4}[/\-.]")


def _parsear_grupo(unicos: pd.Series, dia_primero: bool) -> pd.Series:
    """Parsea valores únicos: inferencia rápida y, para los que fallan, uno a uno."""
    pref = _a_fechas(unicos, dayfirst=dia_primero)
    otra = _a_fechas(unicos, dayfirst=not dia_primero)
    mejor, resto = (otra, pref) if otra.isna().sum() < pref.isna().sum() else (pref, otra)
    parsed = mejor.fillna(resto)
    faltan = parsed.isna()
    for dayfirst in (dia_primero, not dia_primero):
        if not faltan.any():
            break
        parsed = parsed.fillna(_a_fechas(unicos[faltan], format="mixed", dayfirst=dayfirst))
        faltan = parsed.isna()
    return parsed


def parsear_fechas_flexible(valores: pd.Series) -> pd.Series:
    """
    Parsea fechas en formatos mezclados (ISO, dd/mm/aaaa, aaaa/mm/dd...).

    Las fechas que empiezan por el año se leen siempre año-mes-día. En las que
    empiezan por día o mes, el orden de las ambiguas (10/02) sale de la
    evidencia de la columna. Trabaja sobre valores únicos: inferencia rápida y
    solo los que fallan se parsean uno a uno (format="mixed").
    """
    texto = valores.astype(str).str.strip()
    unicos = pd.Series(texto.unique(), dtype=object)
    anio_primero = unicos.str.match(_RE_ANIO_PRIMERO)
    parsed = pd.Series(pd.NaT, index=unicos.index, dtype="datetime64[ns]")
    if anio_primero.any():
        parsed[anio_primero] = _parsear_grupo(unicos[anio_primero].reset_index(drop=True),
                                              dia_primero=False).to_numpy()
    if (~anio_primero).any():
        resto = unicos[~anio_primero].reset_index(drop=True)
        parsed[~anio_primero] = _parsear_grupo(resto, _prefiere_dia_primero(resto)).to_numpy()
    mapa = dict(zip(unicos, parsed))
    return pd.Series([mapa[v] for v in texto], index=valores.index, dtype="datetime64[ns]")


def es_mayormente_fecha(serie: pd.Series, umbral: float = UMBRAL_MAYORMENTE) -> bool:
    """
    Columna de texto que parece de fechas (por nombre o porque ≥50 % tiene
    forma de fecha) y cuyos valores se parsean como fecha en ≥ umbral.
    """
    if pd.api.types.is_datetime64_any_dtype(serie):
        return True
    if pd.api.types.is_numeric_dtype(serie) or pd.api.types.is_bool_dtype(serie):
        return False
    no_nulos = serie.dropna().astype(str)
    if len(no_nulos) == 0:
        return False
    if not _nombre_contiene(serie.name, PALABRAS_FECHA) and \
            no_nulos.str.match(_PATRON_FECHA).mean() < 0.5:
        return False
    return parsear_fechas_flexible(no_nulos).notna().mean() >= umbral


def fechas_imposibles(serie: pd.Series) -> pd.Series:
    """Valores que no son una fecha válida en ningún formato o están fuera de 1900..hoy+5 años."""
    if pd.api.types.is_datetime64_any_dtype(serie):
        fechas = quitar_zona_horaria(serie.dropna())
        valores = fechas
    else:
        valores = serie.dropna().astype(str)
        valores = valores[valores.str.strip() != ""]
        fechas = parsear_fechas_flexible(valores)
    limite = pd.Timestamp.now().normalize() + pd.DateOffset(years=5)
    malas = fechas.isna() | (fechas < pd.Timestamp("1900-01-01")) | (fechas > limite)
    return valores[malas]


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

def generate_report_df(df: pd.DataFrame, nombre: str = "dataset",
                       decisiones_negativos: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Genera el reporte completo de calidad de un DataFrame con score 0-100.

    Penalizaciones:
      - Nulos: hasta -30 pts
      - Duplicados: hasta -20 pts
      - Outliers: -5 pts por columna, máx -25
      - Negativos: -5 pts por columna penalizada, máx -15. Con
        ``decisiones_negativos`` (diagnóstico) penalizan solo las "invalidos";
        sin ellas, las de nombre que sugiere ≥0 (PALABRAS_NO_NEGATIVAS)
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
    negatives = check_negative_values(df, decisiones_negativos)
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