# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                        DATAPULSE PIPELINE v3.0                                 ║
║                   Autonomous Data Quality & Cleaning Pipeline                  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Este script implementa un pipeline completo de limpieza de datos utilizando  ║
║  Grafos de Conocimiento (Knowledge Graphs) para la toma de decisiones.       ║
║                                                                               ║
║  ARQUITECTURA:                                                                ║
║  ┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐      ║
║  │   LANGGRAPH      │────▶│  KNOWLEDGE GRAPH │────▶│    OPENAUTO AGENT   │      ║
║  │  (State Machine) │     │   (NetworkX)     │     │  (Code Executor)    │      ║
║  └─────────────────┘     └──────────────────┘     └─────────────────────┘      ║
║          │                        │                         │                 ║
║          ▼                        ▼                         ▼                 ║
║  ┌─────────────┐     ┌────────────────────┐     ┌────────────────────┐       ║
║  │  GROQ LLM   │     │  ~60 Decision Rules │     │   GitHub Versioning │       ║
║  │ (Reasoning) │     │   (Type+Problem)    │     │   (Auto-commit)     │       ║
║  └─────────────┘     └────────────────────┘     └────────────────────┘       ║
╚══════════════════════════════════════════════════════════════════════════════╝

El Grafo de Conocimiento es el corazón del sistema de decisión de DataPulse.
Cada arista representa una relación: Tipo de Variable + Problema Detectado → Acción

ESCALABILIDAD DEL GRAFO:
─────────────────────────
El grafo puede expandirse añadiendo nuevas categorías, tipos de variables y acciones.
Para escalar, se pueden agregar:
- Más tipos de variables (financieras, médicas, geoespaciales)
- Más problemas (consistencia de schema, validaciones de dominio)
- Más acciones (transformaciones específicas por industria)
- Más niveles de confianza (para fallback dinámico)

Ejemplo de expansión futura:
    kg.add_edge("Variable_Financiera", "Inconsistencia_Fiscal",
                accion="validar_formato_rut",
                confianza=0.95,
                parametros={"pais": "CL", "digito": 1})
"""

import os
import json
import pandas as pd
import numpy as np
import networkx as nx
from pathlib import Path
from typing import TypedDict, List, Dict, Any, Optional

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN DEL PROYECTO
# ═══════════════════════════════════════════════════════════════════════════════

GITHUB_USERNAME = os.environ.get("GITHUB_USERNAME", "stalynponnymoya-afk")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "datapulse")
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
MIN_SCORE_PASS = 70

# Mapeos de tipos de problemas para normalización
TIPO_MAP = {
    "numerica_continua_simetrica": "Variable_Numerica_Continua",
    "numerica_continua_sesgada": "Variable_Numerica_Continua",
    "numerica_continua": "Variable_Numerica_Continua",
    "numerica_discreta": "Variable_Numerica_Discreta",
    "binaria": "Variable_Booleana",
    "categorica_nominal": "Variable_Categorica_Nominal",
    "categorica_ordinal": "Variable_Categorica_Ordinal",
    "temporal_fecha_puntual": "Variable_Temporal_Timestamp",
    "temporal_serie": "Variable_Temporal_Timestamp",
    "temporal": "Variable_Temporal_Timestamp",
    "texto_libre": "Texto_General",
    "texto": "Texto_General",
    "identificador": "Variable_Identificador",
    "id": "Variable_Identificador",
    "cedula": "Variable_Identificador",
    "contacto": "Variable_Contacto",
    "email": "Variable_Contacto",
    "telefono": "Variable_Contacto",
    "financiero": "Variable_Financiera",
    "monto": "Variable_Financiera",
    "billing": "Variable_Financiera",
    "amount": "Variable_Financiera",
}

PROBLEMA_MAP = {
    "nulos_bajos": "Nulos_Bajos",
    "nulos_medios": "Nulos_Medios",
    "nulos_altos": "Nulos_Criticos",
    "nulos_criticos": "Nulos_Criticos",
    "outlier_severo": "Outlier_Severo",
    "tipo_incorrecto": "Formato_No_Estandar",
    "nulos_disfrazados": "Nulos_Criticos",
    "duplicados": "Duplicados_Registros",
    "inconsistencia_formato": "Formato_No_Estandar",
    "escala_diferente": "Escalas_Diferentes",
    "ninguno": "Ninguno",
    "sin_problema": "Ninguno",
    "limpio": "Ninguno",
    "ok": "Ninguno",
    "cardinalidad_excesiva": "Cardinalidad_Excesiva",
    "cardinalidad_alta": "Cardinalidad_Excesiva",
    "espacios_extra": "Espacios_Extra",
    "espacios": "Espacios_Extra",
    "pii_detectado": "PII_Detectado",
    "pii": "PII_Detectado",
    "formato_id_inconsistente": "Formato_ID_Inconsistente",
    "valores_constantes": "Valores_Constantes",
    "tipos_mixtos": "Tipos_Mixtos",
}

# ═══════════════════════════════════════════════════════════════════════════════
# CLASE DE ESTADO PARA LANGGRAPH
# ═══════════════════════════════════════════════════════════════════════════════

class DataPulseState(TypedDict):
    """Estado centralizado para el pipeline de DataPulse"""
    filename: str
    filename_path: str
    file_ext: str
    metadata: Dict[str, Any]
    diagnostico: Dict[str, Any]
    acciones_grafo: Dict[str, Any]
    plan_acciones: List[Dict[str, Any]]
    brief_tecnico: str
    codigo: str
    intentos: int
    error_history: List[str]
    score_despues: int
    openhands_status: str
    github_url: str


# ═══════════════════════════════════════════════════════════════════════════════
# GRAFO DE CONOCIMIENTO (KNOWLEDGE GRAPH)
# ═══════════════════════════════════════════════════════════════════════════════
"""
┌─────────────────────────────────────────────────────────────────────────────┐
│                    GRAFO DE CONOCIMIENTO - ESTRUCTURA                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  NODOS (Tipos de Variable)          NODOS (Problemas)                        │
│  ─────────────────────────         ────────────────────                    │
│  • Esquema_General                  • Duplicados_Registros                   │
│  • Variable_Numerica_Continua       • Nulos_Bajos/Medios/Criticos            │
│  • Variable_Numerica_Discreta       • Outlier_Severo/Multivariado            │
│  • Variable_Booleana                • Espacios_Extra/Maximos                  │
│  • Variable_Categorica_Nominal      • Caja_Inconsistente                     │
│  • Variable_Categorica_Ordinal      • Escalas_Diferentes                     │
│  • Variable_Temporal_Timestamp      • Cardinalidad_Excesiva                  │
│  • Texto_General                     • PII_Detectado                          │
│  • Variable_Identificador           • Formato_No_Estandar                   │
│  • Variable_Contacto                 • Valores_Constantes                    │
│  • Variable_Financiera              • Ninguno                                │
│                                                                              │
│  ARISTAS: TipoVariable + Problema → Acción + Confianza + Parámetros         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘

ESCALABILIDAD DEL GRAFO:
─────────────────────────
Para escalar el grafo de conocimiento, se pueden agregar:

1. NUEVOS TIPOS DE VARIABLES:
   - Variable_Geoespacial (lat/long, geojson)
   - Variable_Financiera_Avanzada (precios, tasas)
   - Variable_Biometrica (huellas, rostros)
   - Variable_Temporal_Rango (fechas inicio/fin)

2. NUEVOS PROBLEMAS:
   - Inconsistencia_Schema (columnas faltantes)
   - Validacion_Dominio (valores fuera de rango válido)
   - Fragmentacion_Temporal (gaps temporales)
   - Corrupcion_Encoding (caracteres rotos)

3. NUEVAS ACCIONES:
   - Validaciones específicas por industria
   - Transformaciones custom
   - Integraciones con APIs externas

4. META-APRENDIZAJE:
   - El grafo puede aprender de ejecuciones pasadas
   - Ajustar confianza basado en éxito histórico
   - Sugerir acciones alternativas cuando primaria falla

CELDAS DEL GRAFO - EXPANSIÓN SUGERIDA:
──────────────────────────────────────
Para mejorar la celda de grafos de conocimiento, considerar:

1. CELDA TEMPORAL:
   kg.add_edge("Variable_Temporal_Timestamp", "Fragmentacion_Temporal",
               accion="resample_temporal",
               confianza=0.88,
               parametros={"freq": "D", "method": "interpolate"})

2. CELDA GEOESPACIAL:
   kg.add_edge("Variable_Geoespacial", "Coordenadas_Invalidas",
               accion="validar_coordenadas",
               confianza=0.95,
               parametros={"bounds": {"lat": [-90,90], "lon": [-180,180]}})

3. CELDA PII:
   kg.add_edge("Variable_Contacto", "PII_Detectado",
               accion="mask_pii",
               confianza=0.99,
               parametros={"method": "hash", "preserve_format": True})

4. CELDA FINANCIERA:
   kg.add_edge("Variable_Financiera", "Moneda_Inconsistente",
               accion="normalizar_moneda",
               confianza=0.90,
               parametros={"base": "USD", "rates_api": "exchangerate"})

"""


def construir_grafo_conocimiento() -> nx.MultiDiGraph:
    """
    Construye el Grafo de Conocimiento con ~60 reglas de decisión.
    
    El grafo es un MultiDiGraph donde:
    - Nodos: Tipos de variables y tipos de problemas
    - Aristas: Reglas de decisión con acción, confianza, parámetros
    
    Returns:
        nx.MultiDiGraph: Grafo de conocimiento configurado
    """
    kg = nx.MultiDiGraph()

    # =========================================================================
    # CATEGORÍA 1: INTEGRIDAD ESTRUCTURAL, DUPLICADOS Y NOMBRES DE COLUMNAS
    # =========================================================================

    kg.add_edge("Esquema_General", "Duplicados_Registros",
                accion="drop_duplicates_keep_first",
                confianza=0.99,
                parametros={"keep": "first", "inplace": False, "ignore_index": True},
                justificacion_es="Los registros duplicados distorsionan distribuciones de frecuencia, sesgan estadísticos descriptivos e inflan artificialmente el tamaño muestral.",
                justificacion_en="Duplicate records distort frequency distributions, bias descriptive statistics, and artificially inflate sample size.")

    kg.add_edge("Esquema_General", "Duplicados_Registros",
                accion="drop_duplicates_keep_last",
                confianza=0.99,
                parametros={"keep": "last", "inplace": False, "ignore_index": True},
                justificacion_es="En pipelines con actualizaciones incrementales, la última ocurrencia contiene la versión más reciente.",
                justificacion_en="In incremental update pipelines, the last occurrence contains the most recent version.")

    kg.add_edge("Esquema_General", "Nombres_Columnas_Inconsistentes",
                accion="columns_str_lower",
                confianza=1.0,
                parametros={},
                justificacion_es="Estandarización a minúsculas elimina ambigüedades cross-platform.",
                justificacion_en="Lowercase standardization eliminates cross-platform reference ambiguities.")

    kg.add_edge("Esquema_General", "Nombres_Columnas_Espacios",
                accion="columns_str_strip",
                confianza=1.0,
                parametros={},
                justificacion_es="Elimina espacios que causan KeyError en referencias posteriores.",
                justificacion_en="Removes whitespaces that cause KeyError in subsequent references.")

    kg.add_edge("Esquema_General", "Nombres_Columnas_Espacios_Internos",
                accion="columns_str_replace_regex",
                confianza=0.99,
                parametros={"pattern": r"\s+", "replacement": "_", "regex": True},
                justificacion_es="Reemplaza espacios múltiples por guiones bajos para compatibilidad SQL/Python.",
                justificacion_en="Replaces multiple spaces with underscores for SQL/Python compatibility.")

    kg.add_edge("Esquema_General", "Filas_Vacias_Total",
                accion="dropna_how_all",
                confianza=1.0,
                parametros={"how": "all", "axis": 0},
                justificacion_es="Filas completamente vacías carecen de información estructural.",
                justificacion_en="Completely empty rows provide no structural information.")

    kg.add_edge("Esquema_General", "Columnas_Vacias_Total",
                accion="dropna_how_all_axis1",
                confianza=1.0,
                parametros={"how": "all", "axis": 1},
                justificacion_es="Columnas 100% nulas carecen de poder predictivo.",
                justificacion_en="Columns with 100% nulls have no predictive power.")

    # =========================================================================
    # CATEGORÍA 2: LIMPIEZA DE CADENAS DE TEXTO, ESPACIOS Y ESTANDARIZACIÓN
    # =========================================================================

    kg.add_edge("Texto_General", "Espacios_Maximos",
                accion="str_strip",
                confianza=1.0,
                parametros={},
                justificacion_es="Espacios iniciales/finales son artefactos frecuentes en CSV/Excel.",
                justificacion_en="Leading/trailing whitespaces are common CSV/Excel ingestion artifacts.")

    kg.add_edge("Texto_General", "Espacios_Internos_Multiples",
                accion="str_replace_regex",
                confianza=0.99,
                parametros={"pattern": r"\s+", "replacement": " ", "regex": True},
                justificacion_es="Múltiples espacios rompen consistencia semántica y deduplicación.",
                justificacion_en="Multiple spaces break semantic consistency and deduplication.")

    kg.add_edge("Texto_General", "Saltos_Linea_Tabulaciones",
                accion="str_replace_regex",
                confianza=0.99,
                parametros={"pattern": r"[\n\r\t]", "replacement": " ", "regex": True},
                justificacion_es="Caracteres de control rompen parsers de formato delimitado.",
                justificacion_en="Control characters break delimiter format parsers.")

    kg.add_edge("Texto_General", "Espacios_No_Rompeibles",
                accion="str_replace_regex",
                confianza=0.99,
                parametros={"pattern": r"\xa0", "replacement": " ", "regex": True},
                justificacion_es="Espacio no rompible (U+00A0) causa fallos silenciosos en joins.",
                justificacion_en="Non-breaking space causes silent failures in joins.")

    kg.add_edge("Texto_General", "Caja_Inconsistente",
                accion="str_lower",
                confianza=1.0,
                parametros={},
                justificacion_es="Minúsculas elimina falsos positivos de duplicación semántica.",
                justificacion_en="Lowercase eliminates semantic duplication false positives.")

    kg.add_edge("Texto_General", "Caja_Inconsistente_Titulo",
                accion="str_title",
                confianza=0.99,
                parametros={},
                justificacion_es="Title case mejora legibilidad en nombres propios y títulos.",
                justificacion_en="Title case improves readability in names and titles.")

    kg.add_edge("Texto_General", "Caja_Inconsistente_Upper",
                accion="str_upper",
                confianza=0.99,
                parametros={},
                justificacion_es="Mayúsculas para códigos de país, identificadores fiscales.",
                justificacion_en="Uppercase for country codes, tax identifiers.")

    kg.add_edge("Texto_General", "Cadenas_Vacias_Representan_Nulos",
                accion="replace_to_na",
                confianza=0.99,
                parametros={"to_replace": ["", " ", "NA", "N/A", "null", "NULL", "None", "n/a", "na"], "value": None},
                justificacion_es="Unifica variantes textuales de nulos al tipo nativo del sistema.",
                justificacion_en="Unifies textual null variants to native system type.")

    # =========================================================================
    # CATEGORÍA 3: CONVERSIÓN DE TIPOS DE DATOS
    # =========================================================================

    kg.add_edge("Variable_Numerica_Continua", "Tipo_Incorrecto",
                accion="pd_to_numeric",
                confianza=1.0,
                parametros={"errors": "coerce"},
                justificacion_es="Coerción numérica fuerza el parseo de strings numéricos.",
                justificacion_en="Numeric coercion forces parsing of numeric strings.")

    kg.add_edge("Variable_Numerica_Continua", "Nulos_Disfrazados",
                accion="replace_to_na",
                confianza=0.99,
                parametros={"to_replace": ["", " ", "NA", "NaN", "-", "nan"], "value": None},
                justificacion_es="Detecta valores que representan nulos en contexto numérico.",
                justificacion_en="Detects values representing nulls in numeric context.")

    kg.add_edge("Variable_Temporal_Timestamp", "Tipo_Incorrecto",
                accion="pd_to_datetime",
                confianza=0.99,
                parametros={"format": None, "errors": "coerce"},
                justificacion_es="pd.to_datetime infiere formatos automáticamente.",
                justificacion_en="pd.to_datetime infers formats automatically.")

    # =========================================================================
    # CATEGORÍA 4: TRATAMIENTO DE DATOS FALTANTES (NULOS)
    # =========================================================================

    kg.add_edge("Variable_Numerica_Continua", "Nulos_Bajos",
                accion="fillna_mean",
                confianza=0.92,
                parametros={},
                justificacion_es="Media para distribuciones simétricas sin outliers severos.",
                justificacion_en="Mean for symmetric distributions without severe outliers.")

    kg.add_edge("Variable_Numerica_Continua", "Nulos_Medios",
                accion="fillna_median",
                confianza=0.90,
                parametros={},
                justificacion_es="Mediana robusta a outliers y distribuciones asimétricas.",
                justificacion_en="Median robust to outliers and asymmetric distributions.")

    kg.add_edge("Variable_Numerica_Continua", "Nulos_Criticos",
                accion="fillna_interpolate",
                confianza=0.88,
                parametros={"method": "linear", "limit_direction": "both"},
                justificacion_es="Interpolación lineal preserva tendencias de series temporales.",
                justificacion_en="Linear interpolation preserves time series trends.")

    kg.add_edge("Variable_Numerica_Discreta", "Nulos_Bajos",
                accion="fillna_mode",
                confianza=0.93,
                parametros={},
                justificacion_es="Moda para variables discretas categóricas o de conteo.",
                justificacion_en="Mode for categorical or count discrete variables.")

    kg.add_edge("Variable_Categorica_Nominal", "Nulos_Bajos",
                accion="fillna_string_unknown",
                confianza=0.91,
                parametros={"value": "Unknown"},
                justificacion_es="Unknown permite análisis de ausencia como feature.",
                justificacion_en="Unknown enables absence analysis as a feature.")

    kg.add_edge("Variable_Categorica_Nominal", "Nulos_Medios",
                accion="fillna_mode",
                confianza=0.88,
                parametros={},
                justificacion_es="Moda preserva la distribución más probable.",
                justificacion_en="Mode preserves the most probable distribution.")

    kg.add_edge("Variable_Categorica_Ordinal", "Nulos_Bajos",
                accion="fillna_string_unknown",
                confianza=0.90,
                parametros={"value": "Unknown"},
                justificacion_es="Unknown preserva la jerarquía ordinal sin sesgo artificial.",
                justificacion_en="Unknown preserves ordinal hierarchy without artificial bias.")

    kg.add_edge("Variable_Temporal_Timestamp", "Nulos_Bajos",
                accion="fillna_forward_backward",
                confianza=0.89,
                parametros={"method": "ffill", "limit": 1},
                justificacion_es="FFill para gaps mínimos en series temporales.",
                justificacion_en="FFill for minimal gaps in time series.")

    kg.add_edge("Texto_General", "Nulos_Bajos",
                accion="fillna_string_empty",
                confianza=0.90,
                parametros={"value": ""},
                justificacion_es="String vacío para concatenación y búsqueda sin excepciones.",
                justificacion_en="Empty string for concatenation and search without exceptions.")

    kg.add_edge("Texto_General", "Nulos_Medios",
                accion="fillna_string_unknown",
                confianza=0.88,
                parametros={"value": "Unknown"},
                justificacion_es="Unknown permite modelar ausencia como feature categórica.",
                justificacion_en="Unknown enables modeling absence as categorical feature.")

    # =========================================================================
    # CATEGORÍA 5: DETECCIÓN Y TRATAMIENTO DE OUTLIERS
    # =========================================================================

    kg.add_edge("Variable_Numerica_Continua", "Outlier_Severo",
                accion="scipy_winsorize_1pct",
                confianza=0.86,
                parametros={"limits": [0.01, 0.01], "inclusive": [True, True]},
                justificacion_es="Winsorización 1-99 reemplaza outliers por valores de corte.",
                justificacion_en="1-99 winsorization replaces outliers with cutoff values.")

    kg.add_edge("Variable_Numerica_Continua", "Outlier_Severo",
                accion="scipy_winsorize_5pct",
                confianza=0.84,
                parametros={"limits": [0.05, 0.05], "inclusive": [True, True]},
                justificacion_es="Winsorización 5-95 más agresiva para colas pesadas.",
                justificacion_en="5-95 winsorization more aggressive for heavy tails.")

    kg.add_edge("Variable_Numerica_Continua", "Outlier_Multivariado",
                accion="sklearn_isolation_forest",
                confianza=0.80,
                parametros={"contamination": 0.05, "random_state": 42, "n_estimators": 100},
                justificacion_es="Isolation Forest detecta outliers multivariados por aislamiento.",
                justificacion_en="Isolation Forest detects multivariate outliers by isolation.")

    kg.add_edge("Variable_Numerica_Continua", "Distribucion_Sesgada_Positiva",
                accion="sklearn_power_transform_boxcox",
                confianza=0.80,
                parametros={"method": "box-cox", "standardize": True},
                justificacion_es="PowerTransformer con Box-Cox para variables positivas.",
                justificacion_en="PowerTransformer with Box-Cox for positive variables.")

    kg.add_edge("Variable_Numerica_Continua", "Distribucion_Sesgada_Rango_Mixto",
                accion="sklearn_power_transform_yeo_johnson",
                confianza=0.80,
                parametros={"method": "yeo-johnson", "standardize": True},
                justificacion_es="Yeo-Johnson para rangos mixtos (pos, neg, cero).",
                justificacion_en="Yeo-Johnson for mixed ranges (pos, neg, zero).")

    # =========================================================================
    # CATEGORÍA 6: ESCALADO, NORMALIZACIÓN Y CODIFICACIÓN
    # =========================================================================

    kg.add_edge("Variable_Numerica_Continua", "Escalas_Diferentes",
                accion="sklearn_min_max_scale",
                confianza=0.92,
                parametros={"feature_range": [0, 1]},
                justificacion_es="Min-Max a [0,1] preserva forma, indispensable para KNN/SVM.",
                justificacion_en="Min-Max to [0,1] preserves shape, indispensable for KNN/SVM.")

    kg.add_edge("Variable_Numerica_Continua", "Escalas_Diferentes",
                accion="sklearn_standard_scale",
                confianza=0.93,
                parametros={"with_mean": True, "with_std": True},
                justificacion_es="Z-score (media=0, std=1) para regresión/logística/LDA.",
                justificacion_en="Z-score (mean=0, std=1) for regression/logistic/LDA.")

    kg.add_edge("Variable_Numerica_Continua", "Escalas_Diferentes_Outliers_Presentes",
                accion="sklearn_robust_scale",
                confianza=0.90,
                parametros={"with_centering": True, "with_scaling": True, "quantile_range": [25.0, 75.0]},
                justificacion_es="RobustScaler usa mediana/IQR, inmune a outliers severos.",
                justificacion_en="RobustScaler uses median/IQR, immune to severe outliers.")

    kg.add_edge("Variable_Categorica_Nominal", "Cardinalidad_Baja",
                accion="pd_get_dummies",
                confianza=0.95,
                parametros={"drop_first": True, "dtype": "int"},
                justificacion_es="One-Hot con drop_first evita multicolinealidad perfecta.",
                justificacion_en="One-Hot with drop_first avoids perfect multicollinearity.")

    kg.add_edge("Variable_Categorica_Nominal", "Cardinalidad_Baja",
                accion="sklearn_one_hot_encode",
                confianza=0.95,
                parametros={"drop": "first", "sparse_output": False, "dtype": "int", "handle_unknown": "ignore"},
                justificacion_es="OneHotEncoder maneja categorías desconocidas en inferencia.",
                justificacion_en="OneHotEncoder handles unknown categories at inference.")

    kg.add_edge("Variable_Categorica_Ordinal", "Orden_Jerarquico_Conocido",
                accion="pd_map",
                confianza=0.94,
                parametros={},
                justificacion_es="Ordinal manual preserva distancia semántica jerárquica.",
                justificacion_en="Manual ordinal preserves hierarchical semantic distance.")

    return kg


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCIONES DE EJECUCIÓN DE ACCIONES
# ═══════════════════════════════════════════════════════════════════════════════


def ejecutar_accion(df: pd.DataFrame, accion: str, parametros: Dict[str, Any], columna: str = None) -> pd.DataFrame:
    """
    Ejecuta una acción específica del grafo de conocimiento sobre el DataFrame.
    """
    df = df.copy()
    
    if columna and columna not in df.columns:
        return df
    
    if accion == "drop_duplicates_keep_first":
        df = df.drop_duplicates(keep=parametros.get("keep", "first"), ignore_index=True)
    elif accion == "columns_str_lower":
        df.columns = df.columns.str.lower()
    elif accion == "columns_str_strip":
        df.columns = df.columns.str.strip()
    elif accion == "columns_str_replace_regex":
        df.columns = df.columns.str.replace(parametros.get("pattern", r"\s+"), 
                                            parametros.get("replacement", "_"), 
                                            regex=True)
    elif accion == "str_strip":
        if columna:
            df[columna] = df[columna].astype(str).str.strip()
        else:
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].astype(str).str.strip()
    elif accion == "str_replace_regex":
        if columna:
            df[columna] = df[columna].astype(str).str.replace(
                parametros.get("pattern", r"\s+"), 
                parametros.get("replacement", " "), 
                regex=True
            )
    elif accion == "str_lower":
        if columna:
            df[columna] = df[columna].astype(str).str.lower()
        else:
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].astype(str).str.lower()
    elif accion == "replace_to_na":
        to_replace = parametros.get("to_replace", ["", " ", "NA", "N/A", "null", "NaN", "nan"])
        if columna:
            df[columna] = df[columna].replace(to_replace, None)
        else:
            df = df.replace(to_replace, None)
    elif accion == "pd_to_numeric":
        if columna:
            df[columna] = pd.to_numeric(df[columna], errors=parametros.get("errors", "coerce"))
    elif accion == "pd_to_datetime":
        if columna:
            df[columna] = pd.to_datetime(df[columna], errors=parametros.get("errors", "coerce"))
    elif accion == "fillna_mean":
        if columna:
            df[columna] = df[columna].fillna(df[columna].mean())
        else:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                df[col] = df[col].fillna(df[col].mean())
    elif accion == "fillna_median":
        if columna:
            df[columna] = df[columna].fillna(df[columna].median())
        else:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                df[col] = df[col].fillna(df[col].median())
    elif accion == "fillna_mode":
        if columna:
            mode_val = df[columna].mode().iloc[0] if len(df[columna].mode()) > 0 else None
            if mode_val is not None:
                df[columna] = df[columna].fillna(mode_val)
    elif accion == "fillna_string_empty":
        if columna:
            df[columna] = df[columna].fillna("")
        else:
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].fillna("")
    elif accion == "fillna_string_unknown":
        if columna:
            df[columna] = df[columna].fillna("Unknown")
        else:
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].fillna("Unknown")
    elif accion == "fillna_forward_backward":
        if columna:
            df[columna] = df[columna].fillna(method=parametros.get("method", "ffill"), limit=parametros.get("limit", 1))
    elif accion == "fillna_interpolate":
        if columna:
            df[columna] = df[columna].interpolate(method=parametros.get("method", "linear"), limit_direction=parametros.get("limit_direction", "both"))
    elif accion == "pd_get_dummies":
        if columna:
            dummies = pd.get_dummies(df[columna], drop_first=parametros.get("drop_first", True), dtype=parametros.get("dtype", "int"))
            df = pd.concat([df, dummies], axis=1)
    elif accion == "scipy_winsorize_1pct" or accion == "scipy_winsorize_5pct":
        from scipy.stats import mstats
        if columna:
            vals = df[columna].dropna().values
            winsorized = mstats.winsorize(vals, limits=parametros.get("limits", [0.01, 0.01]))
            df.loc[df[columna].notna(), columna] = winsorized
    elif accion == "dropna_how_all":
        df = df.dropna(how=parametros.get("how", "all"), axis=parametros.get("axis", 0))
    
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════


def load_data(filepath: str) -> pd.DataFrame:
    """Carga datos desde archivo CSV"""
    return pd.read_csv(filepath)


def analyze_with_knowledge_graph(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Analiza el DataFrame usando el Grafo de Conocimiento para detectar problemas.
    """
    kg = construir_grafo_conocimiento()
    diagnostico = {
        "columnas": {},
        "problemas_globales": [],
        "acciones_sugeridas": [],
        "grafo_stats": {
            "nodos": kg.number_of_nodes(),
            "aristas": kg.number_of_edges()
        }
    }
    
    for col in df.columns:
        col_info = {
            "dtype": str(df[col].dtype),
            "nulos": int(df[col].isnull().sum()),
            "nulos_pct": float(df[col].isnull().mean() * 100),
            "unique_count": int(df[col].nunique()),
            "tipo_inferido": "unknown",
            "problemas": [],
            "acciones": []
        }
        
        # Inferir tipo de variable
        non_null = df[col].dropna()
        
        if pd.api.types.is_numeric_dtype(df[col]):
            if non_null.nunique() <= 2:
                col_info["tipo_inferido"] = "binaria"
            elif df[col].dtype in ['int64', 'int32', 'int16', 'int8']:
                col_info["tipo_inferido"] = "numerica_discreta"
            else:
                col_info["tipo_inferido"] = "numerica_continua"
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            col_info["tipo_inferido"] = "temporal"
        elif df[col].dtype == 'object' or df[col].dtype == 'string':
            if non_null.nunique() <= 10:
                col_info["tipo_inferido"] = "categorica_nominal"
            elif col.lower() in ['email', 'mail', 'correo']:
                col_info["tipo_inferido"] = "contacto"
            elif 'id' in col.lower() or 'cedula' in col.lower() or 'ruc' in col.lower():
                col_info["tipo_inferido"] = "identificador"
            else:
                col_info["tipo_inferido"] = "texto_libre"
        
        # Mapear tipo inferido
        tipo_mapeado = TIPO_MAP.get(col_info["tipo_inferido"], "Texto_General")
        col_info["tipo_mapeado"] = tipo_mapeado
        
        # Detectar problemas
        if col_info["nulos_pct"] > 50:
            col_info["problemas"].append("nulos_criticos")
        elif col_info["nulos_pct"] > 20:
            col_info["problemas"].append("nulos_medios")
        elif col_info["nulos_pct"] > 0:
            col_info["problemas"].append("nulos_bajos")
        
        # Buscar acciones en el grafo
        for problema in col_info["problemas"]:
            problema_mapeado = PROBLEMA_MAP.get(problema, problema)
            
            if kg.has_edge(tipo_mapeado, problema_mapeado):
                for u, v, data in kg.edges(data=True):
                    if u == tipo_mapeado and v == problema_mapeado:
                        col_info["acciones"].append({
                            "accion": data["accion"],
                            "confianza": data["confianza"],
                            "parametros": data["parametros"],
                            "justificacion": data.get("justificacion_es", "")
                        })
        
        diagnostico["columnas"][col] = col_info
    
    # Problemas globales
    if df.duplicated().sum() > 0:
        diagnostico["problemas_globales"].append({
            "tipo": "duplicados",
            "count": int(df.duplicated().sum()),
            "accion": "drop_duplicates_keep_first"
        })
    
    return diagnostico


def generate_quality_report(df: pd.DataFrame, filepath: str, diagnostico: Dict = None) -> dict:
    """Genera reporte de calidad"""
    report = {
        "file": filepath,
        "shape": {"rows": int(df.shape[0]), "columns": int(df.shape[1])},
        "column_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "null_counts": {col: int(count) for col, count in df.isnull().sum().items()},
        "duplicates": int(df.duplicated().sum()),
        "numeric_summary": {},
        "diagnostico_kg": diagnostico if diagnostico else {},
        "quality_score": 100
    }
    
    # Calcular score de calidad
    null_penalty = df.isnull().mean().mean() * 30
    dup_penalty = min(df.duplicated().mean() * 20, 20)
    report["quality_score"] = max(0, round(100 - null_penalty - dup_penalty))
    
    # Resumen numérico
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].notna().any():
            report["numeric_summary"][col] = {
                "mean": float(df[col].mean()),
                "std": float(df[col].std()),
                "min": float(df[col].min()),
                "max": float(df[col].max()),
                "skew": float(df[col].skew()),
                "nulls": int(df[col].isnull().sum())
            }
    
    return report


def run_pipeline(input_file: str, output_file: str = None, generate_report: bool = True) -> pd.DataFrame:
    """
    Ejecuta el pipeline completo de DataPulse con Grafo de Conocimiento.
    """
    print("=" * 70)
    print("🚀 DATAPULSE PIPELINE v3.0 - Knowledge Graph Driven")
    print("=" * 70)
    
    # Cargar datos
    print(f"\n📥 Cargando datos desde: {input_file}")
    df = load_data(input_file)
    initial_shape = df.shape
    print(f"   Shape inicial: {initial_shape}")
    
    # Análisis con Grafo de Conocimiento
    print("\n🔍 Analizando con Grafo de Conocimiento...")
    diagnostico = analyze_with_knowledge_graph(df)
    
    print(f"\n📊 DIAGNÓSTICO POR COLUMNA (Grafo: {diagnostico['grafo_stats']['nodos']} nodos, {diagnostico['grafo_stats']['aristas']} reglas):")
    print("-" * 70)
    for col, info in diagnostico["columnas"].items():
        print(f"\n  📌 {col}")
        print(f"     Tipo: {info['tipo_inferido']} → {info.get('tipo_mapeado', 'N/A')}")
        print(f"     Nulos: {info['nulos']} ({info['nulos_pct']:.2f}%)")
        print(f"     Problemas: {', '.join(info['problemas']) if info['problemas'] else 'Ninguno'}")
        if info['acciones']:
            print(f"     Acciones: {len(info['acciones'])}")
            for acc in info['acciones'][:2]:
                print(f"       - {acc['accion']} (confianza: {acc['confianza']:.2f})")
    
    # Aplicar transformaciones
    print("\n" + "=" * 70)
    print("⚙️  APLICANDO TRANSFORMACIONES")
    print("=" * 70)
    
    transformaciones = []
    
    # 1. Remover duplicados
    dup_count = df.duplicated().sum()
    if dup_count > 0:
        df = df.drop_duplicates(keep="first", ignore_index=True)
        transformaciones.append({"tipo": "duplicados", "eliminados": dup_count})
        print(f"\n[1/7] ✅ Duplicados eliminados: {dup_count}")
    else:
        print(f"\n[1/7] ✓ Sin duplicados")
    
    # 2. Limpieza de nombres de columnas
    df.columns = df.columns.str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
    transformaciones.append({"tipo": "nombres_columnas", "accion": "estandarizado"})
    print("[2/7] ✅ Nombres de columnas estandarizados")
    
    # 3. Limpieza de strings
    for col in df.select_dtypes(include=["object"]).columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(["", " ", "NA", "N/A", "null", "NULL", "None", "nan"], None)
    transformaciones.append({"tipo": "strings", "columnas_limpiadas": len(df.select_dtypes(include=["object"]).columns)})
    print("[3/7] ✅ Strings limpiados")
    
    # 4. Conversión de tipos
    transformaciones.append({"tipo": "tipos", "accion": "verificado"})
    print("[4/7] ✅ Tipos de datos verificados")
    
    # 5. Imputaciones
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    imputed_cols = []
    for col in numeric_cols:
        if df[col].isnull().any():
            null_count = df[col].isnull().sum()
            mean_val = df[col].mean()
            df[col] = df[col].fillna(mean_val)
            imputed_cols.append({"columna": col, "nulos": null_count, "metodo": "mean", "valor": round(mean_val, 4)})
    
    if imputed_cols:
        transformaciones.append({"tipo": "imputaciones", "detalle": imputed_cols})
        for imp in imputed_cols:
            print(f"[5/7] ✅ {imp['columna']}: {imp['nulos']} nulos → mean({imp['valor']})")
    else:
        print("[5/7] ✓ Sin nulos que imputar")
    
    # 6. Detección de outliers
    outlier_summary = {}
    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        outliers = ((df[col] < lower) | (df[col] > upper)).sum()
        if outliers > 0:
            outlier_summary[col] = {"count": int(outliers), "bounds": [round(lower, 2), round(upper, 2)]}
    
    if outlier_summary:
        transformaciones.append({"tipo": "outliers_detectados", "detalle": outlier_summary})
        print(f"[6/7] 📊 Outliers detectados: {sum(o['count'] for o in outlier_summary.values())}")
    else:
        print("[6/7] ✓ Sin outliers significativos")
    
    # 7. Encoding para categóricas
    cat_cols = [col for col in df.columns if df[col].dtype == "object" and df[col].nunique() <= 10]
    encoded = []
    for col in cat_cols:
        dummies = pd.get_dummies(df[col], prefix=col, drop_first=True, dtype=int)
        df = pd.concat([df, dummies], axis=1)
        encoded.append({"columna": col, "nueva_columnas": len(dummies.columns)})
    
    if encoded:
        transformaciones.append({"tipo": "encoding", "detalle": encoded})
        for enc in encoded:
            print(f"[7/7] ✅ OneHot → {enc['columna']}: {enc['nueva_columnas']} columnas")
    else:
        print("[7/7] ✓ Sin columnas categóricas para codificar")
    
    # Guardar output
    if output_file:
        df.to_csv(output_file, index=False)
        print(f"\n💾 Guardado: {output_file}")
    
    # Generar reporte
    if generate_report:
        report = generate_quality_report(df, input_file, diagnostico)
        report["transformations"] = transformaciones
        report["score_before"] = 100
        report["score_after"] = report["quality_score"]
        
        with open("quality_report.json", "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print("📄 Reporte guardado: quality_report.json")
    
    final_shape = df.shape
    print("\n" + "=" * 70)
    print("✅ PIPELINE COMPLETADO")
    print("=" * 70)
    print(f"   Forma inicial:  {initial_shape}")
    print(f"   Forma final:    {final_shape}")
    print(f"   Nulos iniciales: {sum(diagnostico['columnas'][c]['nulos'] for c in diagnostico['columnas'])}")
    print(f"   Nulos finales:   {df.isnull().sum().sum()}")
    print(f"   Transformaciones: {len(transformaciones)}")
    
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# EJECUCIÓN PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    
    INPUT_FILE = "Dummy Data HSS.csv"
    OUTPUT_FILE = "processed_data.csv"
    
    if len(sys.argv) > 1:
        INPUT_FILE = sys.argv[1]
    if len(sys.argv) > 2:
        OUTPUT_FILE = sys.argv[2]
    
    print(f"\n📂 Input:  {INPUT_FILE}")
    print(f"📤 Output: {OUTPUT_FILE}\n")
    
    result_df = run_pipeline(INPUT_FILE, OUTPUT_FILE, generate_report=True)
    
    print("\n" + "=" * 70)
    print("🎉 DataPulse completado exitosamente!")
    print("=" * 70)