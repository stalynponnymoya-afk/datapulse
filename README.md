# DataPulse — Autonomous Data Quality & Cleaning Pipeline

> **[English](#english)** | **[Español](#español)**

---

<a name="english"></a>

## English

### What is DataPulse?

**DataPulse** is an autonomous data quality and cleaning pipeline that takes a raw dataset as input and produces:
- ✅ A clean dataset (`processed_data.csv`)
- ✅ A quality report (`quality_report.json`)
- ✅ A reproducible Python script (`datapulse_pipeline.py`)
- ✅ Automatic GitHub version control

**You upload a file. DataPulse does the rest.** No manual configuration of cleaning rules required.

---

### 🚀 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATAPULSE ARCHITECTURE                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   INPUT FILE                    CORE ENGINE                    OUTPUT        │
│   ───────────                   ───────────                    ──────        │
│                                                                              │
│   CSV/XLSX/JSON ──┐                                                          │
│                   ▼                                                          │
│   ┌──────────────────────────────────────────────────────────────────┐       │
│   │                    LANGGRAPH ORCHESTRATOR                        │       │
│   │  ┌─────────┐  ┌──────────────┐  ┌────────────┐  ┌────────────┐  │       │
│   │  │ INGEST  │─▶│  DIAGNOSTIC  │─▶│  CONSULT   │─▶│   PLAN     │  │       │
│   │  │  NODE   │  │   (GROQ)     │  │   (KG)     │  │   (GROQ)   │  │       │
│   │  └─────────┘  └──────────────┘  └────────────┘  └────────────┘  │       │
│   │                                              │                   │       │
│   │                   ┌──────────────┐            │                   │       │
│   │                   │   EXECUTE    │◀───────────┘                   │       │
│   │                   │ (OpenHands)  │                                │       │
│   │                   └──────┬───────┘                                │       │
│   │                          │                                        │       │
│   │    ┌─────────────────────┴─────────────────────┐                │       │
│   │    │         VALIDATE SCORE                     │                │       │
│   │    │         (Threshold ≥ 70)                   │                │       │
│   │    └─────────────────────┬─────────────────────┘                │       │
│   │                          │                                        │       │
│   │    ok              fail  │  retry (max 3)                       │       │
│   │    │                    │                                        │       │
│   │    ▼                    └──────────────────────────────────      │       │
│   │  ┌─────────┐                                                         │       │
│   │  │ COMMIT  │───▶ GitHub (feature branch, auto-commit)              │       │
│   │  └─────────┘                                                         │       │
│   └──────────────────────────────────────────────────────────────────┘       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Technology | Role |
|---|---|---|
| **Orchestrator** | LangGraph | State machine with 7 nodes, conditional retry logic |
| **Reasoning** | LLaMA 3.3 70B via Groq | Diagnoses column types, detects problems, builds technical brief |
| **Decision Rules** | NetworkX (Knowledge Graph) | ~60 deterministic statistical rules for cleaning decisions |
| **Autonomous Agent** | OpenHands Cloud API | Writes, executes, and validates the cleaning code autonomously |
| **Versioning** | GitHub API | Commits validated code to feature branch |

---

### 📊 Knowledge Graph (The Heart of DataPulse)

The Knowledge Graph is a `MultiDiGraph` (NetworkX) containing **~60 decision rules** organized in **6 categories**:

```
┌────────────────────────────────────────────────────────────────────────────┐
│                    KNOWLEDGE GRAPH STRUCTURE                               │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  NODES (Variable Types)              NODES (Problems)                     │
│  ──────────────────────────         ─────────────────────                 │
│  • Esquema_General                   • Duplicados_Registros                 │
│  • Variable_Numerica_Continua        • Nulos_Bajos/Medios/Criticos          │
│  • Variable_Numerica_Discreta        • Outlier_Severo/Multivariado          │
│  • Variable_Booleana                 • Espacios_Extra/Maximos               │
│  • Variable_Categorica_Nominal      • Caja_Inconsistente                   │
│  • Variable_Categorica_Ordinal      • Escalas_Diferentes                    │
│  • Variable_Temporal_Timestamp      • Cardinalidad_Excesiva                │
│  • Texto_General                     • PII_Detectado                         │
│  • Variable_Identificador           • Formato_No_Estandar                  │
│  • Variable_Contacto                • Valores_Constantes                   │
│  • Variable_Financiera              • Ninguno                              │
│                                                                            │
│  EDGES: VariableType + Problem → Action + Confidence + Parameters         │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

#### Rule Categories

| Category | Description | Example Rules |
|---|---|---|
| **1. Structural Integrity** | Duplicates, empty rows/columns, column name normalization | `drop_duplicates`, `columns_str_lower`, `dropna_how_all` |
| **2. String Cleaning** | Whitespace, line breaks, case normalization, email/phone validation | `str_strip`, `str_replace_regex`, `str_lower` |
| **3. Type Conversion** | Datetime parsing, boolean mapping, numeric coercion | `pd_to_datetime`, `pd_to_numeric`, `replace_to_na` |
| **4. Missing Data Treatment** | Mean/median/mode imputation, KNN imputer, forward fill, interpolation | `fillna_mean`, `fillna_median`, `fillna_interpolate` |
| **5. Outlier Detection** | Winsorization, Isolation Forest, Local Outlier Factor | `scipy_winsorize`, `sklearn_isolation_forest` |
| **6. Scaling & Encoding** | MinMax, Standard, Robust scaling, OneHot/Ordinal encoding | `sklearn_min_max_scale`, `pd_get_dummies`, `sklearn_standard_scale` |

Each rule contains:
- **Action**: The transformation to apply
- **Confidence**: Score (0.78–1.0) based on statistical evidence
- **Parameters**: Configuration for the action
- **Justification**: Bilingual explanation (ES/EN)

When the graph has no rule for a specific case, the decision falls back to the LLM with a maximum confidence of 0.70.

---

### 🔬 Knowledge Graph Scalability

The Knowledge Graph is designed for **horizontal scalability**. To expand the graph, you can add:

#### 1. New Variable Types
```python
# Geographic
kg.add_edge("Variable_Geoespacial", "Coordenadas_Invalidas",
            accion="validar_coordenadas",
            confianza=0.95,
            parametros={"bounds": {"lat": [-90,90], "lon": [-180,180]}})

# Financial
kg.add_edge("Variable_Financiera", "Moneda_Inconsistente",
            accion="normalizar_moneda",
            confianza=0.90,
            parametros={"base": "USD", "rates_api": "exchangerate"})

# Biometric
kg.add_edge("Variable_Biometrica", "Patron_Invalido",
            accion="validar_patron_biometrico",
            confianza=0.92,
            parametros={"tipo": "fingerprint"})
```

#### 2. New Problems
```python
# Schema inconsistency
kg.add_edge("Esquema_General", "Inconsistencia_Schema",
            accion="validar_columnas_requeridas",
            confianza=0.88,
            parametros={"requeridas": ["id", "timestamp", "monto"]})

# Domain validation
kg.add_edge("Variable_Financiera", "Validacion_Dominio",
            accion="validar_rango_valido",
            confianza=0.93,
            parametros={"min": 0, "max": 1000000})

# Temporal fragmentation
kg.add_edge("Variable_Temporal_Timestamp", "Fragmentacion_Temporal",
            accion="resample_temporal",
            confianza=0.88,
            parametros={"freq": "D", "method": "interpolate"})
```

#### 3. New Actions
```python
# PII Masking
kg.add_edge("Variable_Contacto", "PII_Detectado",
            accion="mask_pii",
            confianza=0.99,
            parametros={"method": "hash", "preserve_format": True})

# Cross-validation
kg.add_edge("Variable_Financiera", "Inconsistencia_Calculo",
            accion="validar_formula",
            confianza=0.95,
            parametros={"formula": "total = cantidad * precio"})
```

#### 4. Meta-Learning (Future)
```python
# Dynamic confidence adjustment based on execution history
def ajustar_confianza_basado_en_historial(accion, exito):
    confianza_base = get_confianza_base(accion)
    factor = 1.1 if exito else 0.9
    return min(confianza_base * factor, 1.0)
```

#### 5. Graph Cell Expansion Examples

| Cell Type | Expansion | Confidence |
|---|---|---|
| **Temporal** | `resample_temporal`, `interpolar_gaps`, `detectar_gaps` | 0.85-0.90 |
| **Geographic** | `validar_latitud`, `validar_longitud`, `geo编码` | 0.92-0.98 |
| **PII** | `mask_email`, `mask_phone`, `mask_credit_card` | 0.95-0.99 |
| **Financial** | `validar_iva`, `normalizar_moneda`, `detectar_fraude` | 0.88-0.95 |

---

### 🔄 Pipeline Execution Flow

```
Your CSV/XLSX/JSON/Parquet file
         │
         ▼
[1] node_ingestar ─────────── pandas reads file, extracts metadata
         │
         ▼
[2] node_diagnosticar ─────── Groq infers real column types + detects problems
         │
         ▼
[3] node_consultar_grafo ──── Knowledge Graph decides cleaning action per column
         │
         ▼
[4] node_planificar ───────── Groq resolves fallbacks + orders actions + builds brief
         │
         ▼
[5] node_ejecutar ─────────── OpenHands receives brief, writes code, executes, validates
         │
         ▼
[6] node_validar_score ────── Checks quality score >= threshold (70)
         │
    ┌────┴─────┐
    ok         fail → retry (max 3 attempts)
    │
    ▼
[7] node_commit ───────────── Pushes validated code to GitHub feature branch
```

---

### 📋 What It Produces

| File | Description |
|---|---|
| `datapulse_pipeline.py` | Executable Python script that reproduces the entire cleaning process |
| `processed_data.csv` | Clean dataset ready for analysis or modeling |
| `quality_report.json` | Quality metrics: null counts, types, statistics, duplicates, KG diagnostics |

### Example Output

```
🚀 DATAPULSE PIPELINE v3.0 - Knowledge Graph Driven
======================================================================

📥 Cargando datos desde: Dummy Data HSS.csv
   Shape inicial: (4572, 5)

🔍 Analizando con Grafo de Conocimiento...

📊 DIAGNÓSTICO POR COLUMNA (Grafo: 22 nodos, 45 reglas):
----------------------------------------------------------------------

  📌 TV
     Tipo: numerica_continua → Variable_Numerica_Continua
     Nulos: 10 (0.22%)
     Problemas: nulos_bajos
     Acciones: 1
       - fillna_mean (confianza: 0.92)

  📌 Radio
     Tipo: numerica_continua → Variable_Numerica_Continua
     Nulos: 4 (0.09%)
     Problemas: nulos_bajos
     Acciones: 1
       - fillna_mean (confianza: 0.92)

⚙️  APLICANDO TRANSFORMACIONES
======================================================================

[1/7] ✅ Duplicados eliminados: 0
[2/7] ✅ Nombres de columnas estandarizados
[3/7] ✅ Strings limpiados
[4/7] ✅ Tipos de datos verificados
[5/7] ✅ TV: 10 nulos → mean(54.0669)
[5/7] ✅ Radio: 4 nulos → mean(18.1604)
[5/7] ✅ Social Media: 6 nulos → mean(3.324)
[5/7] ✅ Sales: 6 nulos → mean(192.4666)
[6/7] 📊 Outliers detectados: 29
[7/7] ✅ OneHot → Influencer: 3 columnas

💾 Guardado: processed_data.csv
📄 Reporte guardado: quality_report.json

======================================================================
✅ PIPELINE COMPLETADO
======================================================================
   Forma inicial:  (4572, 5)
   Forma final:    (4572, 8)
   Nulos iniciales: 26
   Nulos finales:   0
   Transformaciones: 7

🎉 DataPulse completado exitosamente!
```

---

### 🔧 Requirements

```
# Core
pandas>=2.0.0
numpy>=1.24.0
scipy>=1.10.0
scikit-learn>=1.3.0

# Graph & State Machine
networkx>=3.1.0
langgraph>=0.0.20
langchain-core>=0.1.0
langchain-groq>=0.0.2

# API & Web
fastapi>=0.100.0
uvicorn>=0.23.0
requests>=2.28.0

# GitHub
PyGithub>=1.58.0
```

### API Keys Required

| Key | Service | Purpose |
|---|---|---|
| `GROQ_API_KEY` | [Groq Cloud](https://console.groq.com) | LLM inference (LLaMA 3.3 70B) |
| `OPENHANDS_API_KEY` | [OpenHands](https://app.all-hands.dev) | Autonomous code agent |
| `GITHUB_TOKEN` | [GitHub](https://github.com/settings/tokens) | Repository access (scope: repo) |

---

### 🚀 Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/stalynponnymoya-afk/datapulse.git
   cd datapulse
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   export GROQ_API_KEY="your-groq-api-key"
   export GITHUB_TOKEN="your-github-token"
   export OPENHANDS_API_KEY="your-openhands-api-key"
   ```

4. **Run the pipeline**
   ```bash
   python datapulse_pipeline.py
   ```

5. **Or use the API**
   ```bash
   uvicorn api.main:app --reload
   # Then POST to http://localhost:8000/analyze
   ```

---

### ⚙️ Tech Stack Decisions: Why This Combination?

| Question | Answer |
|---|---|
| **Why LangGraph over plain Python?** | Retry logic with accumulated error history requires persistent state across nodes. LangGraph provides this natively with TypedDict state and conditional edges. |
| **Why a Knowledge Graph over RAG?** | Data cleaning decisions need deterministic, traceable rules — not text similarity search. The Knowledge Graph navigates explicit logic: `Variable Type + Problem → Action + Justification + Confidence`. |
| **Why OpenHands over direct LLM code generation?** | OpenHands doesn't just generate code — it executes it, observes errors, and self-corrects through its Act→Observe→Reason cycle. A direct LLM call would require a separate validation sandbox. |
| **Why NetworkX for the knowledge graph?** | MultiDiGraph supports multiple edges between the same nodes (allowing different actions for the same type+problem pair), and provides efficient graph algorithms for traversal and querying. |

---

### 📈 Limitations

- Knowledge Graph covers common statistical cases. Domain-specific datasets (medical, regulated financial) may fall back to LLM decisions with lower confidence
- OpenHands execution takes 1-5 minutes per run
- GitHub API has a 100MB file size limit
- Groq free tier: 100K tokens/day (~5-10 full pipeline runs)
- OpenHands outputs are non-deterministic: two runs on the same input may produce slightly different code

---

### 📁 Project Structure

```
datapulse/
├── README.md                 # This file
├── LICENSE                  # MIT License
├── requirements.txt         # Python dependencies
├── datapulse_pipeline.py     # Main pipeline with Knowledge Graph
├── quality_report.json       # Generated quality report
├── Dummy Data HSS.csv        # Sample dataset
├── api/
│   └── main.py              # FastAPI endpoints
├── src/
│   └── quality_checker.py   # Quality analysis module
├── data/
│   ├── california_housing.csv
│   └── superstore_sales.csv
└── test/
    └── __init__.py
```

---

### 📜 License

MIT License - See [LICENSE](LICENSE) file for details.

---

<a name="español"></a>

## Español

### ¿Qué es DataPulse?

**DataPulse** es un pipeline autónomo de calidad y limpieza de datos que recibe un dataset crudo como input y produce:
- ✅ Un dataset limpio (`processed_data.csv`)
- ✅ Un reporte de calidad (`quality_report.json`)
- ✅ Un script Python reproducible (`datapulse_pipeline.py`)
- ✅ Control de versiones automático en GitHub

**Subes un archivo. DataPulse hace el resto.** No requiere configuración manual de reglas de limpieza.

---

### 🚀 Visión General de la Arquitectura

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ARQUITECTURA DE DATAPULSE                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ARCHIVO DE ENTRADA              MOTOR PRINCIPAL            SALIDA         │
│   ──────────────────              ───────────────            ───────         │
│                                                                              │
│   CSV/XLSX/JSON ──┐                                                          │
│                   ▼                                                          │
│   ┌──────────────────────────────────────────────────────────────────┐       │
│   │                    ORQUESTADOR LANGGRAPH                          │       │
│   │  ┌─────────┐  ┌──────────────┐  ┌────────────┐  ┌────────────┐  │       │
│   │  │ INGEST  │─▶│  DIAGNOSTIC  │─▶│  CONSULT   │─▶│   PLAN     │  │       │
│   │  │  NODO   │  │   (GROQ)     │  │   (KG)     │  │   (GROQ)   │  │       │
│   │  └─────────┘  └──────────────┘  └────────────┘  └────────────┘  │       │
│   │                                              │                   │       │
│   │                   ┌──────────────┐            │                   │       │
│   │                   │   EJECUTAR   │◀───────────┘                   │       │
│   │                   │ (OpenHands)  │                                │       │
│   │                   └──────┬───────┘                                │       │
│   │                          │                                        │       │
│   │    ┌─────────────────────┴─────────────────────┐                │       │
│   │    │         VALIDAR SCORE                      │                │       │
│   │    │         (Umbral ≥ 70)                      │                │       │
│   │    └─────────────────────┬─────────────────────┘                │       │
│   │                          │                                        │       │
│   │    ok              fail  │  retry (máx 3)                       │       │
│   │    │                    │                                        │       │
│   │    ▼                    └──────────────────────────────────      │       │
│   │  ┌─────────┐                                                         │       │
│   │  │ COMMIT  │───▶ GitHub (rama feature, auto-commit)                 │       │
│   │  └─────────┘                                                         │       │
│   └──────────────────────────────────────────────────────────────────┘       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Componentes

| Componente | Tecnología | Rol |
|---|---|---|
| **Orquestador** | LangGraph | Máquina de estados con 7 nodos y lógica de retry condicional |
| **Razonamiento** | LLaMA 3.3 70B vía Groq | Diagnostica tipos de columnas, detecta problemas, construye brief técnico |
| **Reglas de Decisión** | NetworkX (Grafo de Conocimiento) | ~60 reglas estadísticas deterministas para decisiones de limpieza |
| **Agente Autónomo** | OpenHands Cloud API | Escribe, ejecuta y valida el código de limpieza de forma autónoma |
| **Versionado** | API de GitHub | Confirma código validado en rama feature |

---

### 📊 Grafo de Conocimiento (El Corazón de DataPulse)

El Grafo de Conocimiento es un `MultiDiGraph` (NetworkX) que contiene **~60 reglas de decisión** organizadas en **6 categorías**:

```
┌────────────────────────────────────────────────────────────────────────────┐
│                   ESTRUCTURA DEL GRAFO DE CONOCIMIENTO                      │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  NODOS (Tipos de Variable)        NODOS (Problemas)                         │
│  ─────────────────────────        ──────────────────────                  │
│  • Esquema_General                 • Duplicados_Registros                   │
│  • Variable_Numerica_Continua      • Nulos_Bajos/Medios/Criticos            │
│  • Variable_Numerica_Discreta      • Outlier_Severo/Multivariado            │
│  • Variable_Booleana               • Espacios_Extra/Maximos                  │
│  • Variable_Categorica_Nominal    • Caja_Inconsistente                      │
│  • Variable_Categorica_Ordinal    • Escalas_Diferentes                      │
│  • Variable_Temporal_Timestamp     • Cardinalidad_Excesiva                  │
│  • Texto_General                   • PII_Detectado                           │
│  • Variable_Identificador         • Formato_No_Estandar                    │
│  • Variable_Contacto              • Valores_Constantes                     │
│  • Variable_Financiera            • Ninguno                                 │
│                                                                            │
│  ARISTAS: TipoVariable + Problema → Acción + Confianza + Parámetros         │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

#### Categorías de Reglas

| Categoría | Descripción | Ejemplos de Reglas |
|---|---|---|
| **1. Integridad Estructural** | Duplicados, filas/columnas vacías, normalización de nombres | `drop_duplicates`, `columns_str_lower`, `dropna_how_all` |
| **2. Limpieza de Texto** | Espacios, saltos de línea, normalización de caja, validación email/teléfono | `str_strip`, `str_replace_regex`, `str_lower` |
| **3. Conversión de Tipos** | Parseo de fechas, mapeo booleano, coerción numérica | `pd_to_datetime`, `pd_to_numeric`, `replace_to_na` |
| **4. Tratamiento de Nulos** | Imputación media/mediana/moda, KNN imputer, forward fill, interpolación | `fillna_mean`, `fillna_median`, `fillna_interpolate` |
| **5. Detección de Outliers** | Winsorización, Isolation Forest, Local Outlier Factor | `scipy_winsorize`, `sklearn_isolation_forest` |
| **6. Escalado y Encoding** | MinMax, Standard, Robust scaling, OneHot/Ordinal encoding | `sklearn_min_max_scale`, `pd_get_dummies`, `sklearn_standard_scale` |

Cada regla contiene:
- **Acción**: La transformación a aplicar
- **Confianza**: Puntuación (0.78–1.0) basada en evidencia estadística
- **Parámetros**: Configuración para la acción
- **Justificación**: Explicación bilingüe (ES/EN)

Cuando el grafo no tiene regla para un caso específico, la decisión cae al LLM con confianza máxima de 0.70.

---

### 🔬 Escalabilidad del Grafo de Conocimiento

El Grafo de Conocimiento está diseñado para **escalabilidad horizontal**. Para expandir el grafo, puedes agregar:

#### 1. Nuevos Tipos de Variables
```python
# Geoespacial
kg.add_edge("Variable_Geoespacial", "Coordenadas_Invalidas",
            accion="validar_coordenadas",
            confianza=0.95,
            parametros={"bounds": {"lat": [-90,90], "lon": [-180,180]}})

# Financiera
kg.add_edge("Variable_Financiera", "Moneda_Inconsistente",
            accion="normalizar_moneda",
            confianza=0.90,
            parametros={"base": "USD", "rates_api": "exchangerate"})

# Biométrica
kg.add_edge("Variable_Biometrica", "Patron_Invalido",
            accion="validar_patron_biometrico",
            confianza=0.92,
            parametros={"tipo": "fingerprint"})
```

#### 2. Nuevos Problemas
```python
# Inconsistencia de esquema
kg.add_edge("Esquema_General", "Inconsistencia_Schema",
            accion="validar_columnas_requeridas",
            confianza=0.88,
            parametros={"requeridas": ["id", "timestamp", "monto"]})

# Validación de dominio
kg.add_edge("Variable_Financiera", "Validacion_Dominio",
            accion="validar_rango_valido",
            confianza=0.93,
            parametros={"min": 0, "max": 1000000})

# Fragmentación temporal
kg.add_edge("Variable_Temporal_Timestamp", "Fragmentacion_Temporal",
            accion="resample_temporal",
            confianza=0.88,
            parametros={"freq": "D", "method": "interpolate"})
```

#### 3. Nuevas Acciones
```python
# Enmascaramiento PII
kg.add_edge("Variable_Contacto", "PII_Detectado",
            accion="mask_pii",
            confianza=0.99,
            parametros={"method": "hash", "preserve_format": True})

# Validación cruzada
kg.add_edge("Variable_Financiera", "Inconsistencia_Calculo",
            accion="validar_formula",
            confianza=0.95,
            parametros={"formula": "total = cantidad * precio"})
```

#### 4. Meta-Aprendizaje (Futuro)
```python
# Ajuste dinámico de confianza basado en historial de ejecuciones
def ajustar_confianza_basado_en_historial(accion, exito):
    confianza_base = get_confianza_base(accion)
    factor = 1.1 if exito else 0.9
    return min(confianza_base * factor, 1.0)
```

#### 5. Ejemplos de Expansión de Celdas

| Tipo de Celda | Expansión | Confianza |
|---|---|---|
| **Temporal** | `resample_temporal`, `interpolar_gaps`, `detectar_gaps` | 0.85-0.90 |
| **Geoespacial** | `validar_latitud`, `validar_longitud`, `geo_encoding` | 0.92-0.98 |
| **PII** | `mask_email`, `mask_phone`, `mask_credit_card` | 0.95-0.99 |
| **Financiera** | `validar_iva`, `normalizar_moneda`, `detectar_fraude` | 0.88-0.95 |

---

### 🔄 Flujo de Ejecución del Pipeline

```
Tu archivo CSV/XLSX/JSON/Parquet
         │
         ▼
[1] node_ingestar ─────────── pandas lee archivo, extrae metadata
         │
         ▼
[2] node_diagnosticar ─────── Groq infiere tipos reales + detecta problemas
         │
         ▼
[3] node_consultar_grafo ──── Grafo de Conocimiento decide acción por columna
         │
         ▼
[4] node_planificar ───────── Groq resuelve fallbacks + ordena acciones + construye brief
         │
         ▼
[5] node_ejecutar ─────────── OpenHands recibe brief, escribe código, ejecuta, valida
         │
         ▼
[6] node_validar_score ────── Verifica score de calidad >= umbral (70)
         │
    ┌────┴─────┐
    ok         falla → retry (máx 3 intentos)
    │
    ▼
[7] node_commit ───────────── Push del código validado a rama feature en GitHub
```

---

### 📋 Qué Produce

| Archivo | Descripción |
|---|---|
| `datapulse_pipeline.py` | Script Python ejecutable que reproduce toda la limpieza |
| `processed_data.csv` | Dataset limpio listo para análisis o modelado |
| `quality_report.json` | Métricas de calidad: nulos, tipos, estadísticas, duplicados, diagnóstico KG |

---

### ⚙️ Requisitos

```
# Core
pandas>=2.0.0
numpy>=1.24.0
scipy>=1.10.0
scikit-learn>=1.3.0

# Grafo y Máquina de Estados
networkx>=3.1.0
langgraph>=0.0.20
langchain-core>=0.1.0
langchain-groq>=0.0.2

# API y Web
fastapi>=0.100.0
uvicorn>=0.23.0
requests>=2.28.0

# GitHub
PyGithub>=1.58.0
```

### API Keys Necesarias

| Key | Servicio | Propósito |
|---|---|---|
| `GROQ_API_KEY` | [Groq Cloud](https://console.groq.com) | Inferencia LLM (LLaMA 3.3 70B) |
| `OPENHANDS_API_KEY` | [OpenHands](https://app.all-hands.dev) | Agente autónomo de código |
| `GITHUB_TOKEN` | [GitHub](https://github.com/settings/tokens) | Acceso al repositorio (scope: repo) |

---

### 🚀 Inicio Rápido

1. **Clonar el repositorio**
   ```bash
   git clone https://github.com/stalynponnymoya-afk/datapulse.git
   cd datapulse
   ```

2. **Instalar dependencias**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configurar variables de entorno**
   ```bash
   export GROQ_API_KEY="tu-clave-groq"
   export GITHUB_TOKEN="tu-token-github"
   export OPENHANDS_API_KEY="tu-clave-openhands"
   ```

4. **Ejecutar el pipeline**
   ```bash
   python datapulse_pipeline.py
   ```

5. **O usar la API**
   ```bash
   uvicorn api.main:app --reload
   # Luego POST a http://localhost:8000/analyze
   ```

---

### 🔧 Decisiones Técnicas: ¿Por Qué Esta Combinación?

| Pregunta | Respuesta |
|---|---|
| **¿Por qué LangGraph sobre Python puro?** | La lógica de retry con historial de errores acumulado requiere estado persistente entre nodos. LangGraph lo proporciona nativamente con estado TypedDict y edges condicionales. |
| **¿Por qué un Grafo de Conocimiento en vez de RAG?** | Las decisiones de limpieza de datos necesitan reglas deterministas y trazables, no búsqueda de similitud textual. El Grafo de Conocimiento navega lógica explícita: `Tipo de Variable + Problema → Acción + Justificación + Confianza`. |
| **¿Por qué OpenHands en vez de generación directa con LLM?** | OpenHands no solo genera código — lo ejecuta, observa errores y se autocorrige mediante su ciclo Act→Observe→Reason. Una llamada directa al LLM requeriría un sandbox de validación separado. |
| **¿Por qué NetworkX para el grafo de conocimiento?** | MultiDiGraph soporta múltiples aristas entre los mismos nodos (permitiendo diferentes acciones para el mismo par tipo+problema), y proporciona algoritmos eficientes de grafo para recorrido y consulta. |

---

### 📈 Limitaciones

- El Grafo de Conocimiento cubre casos estadísticos comunes. Datasets de dominio muy específico (médico, financiero regulado) pueden caer en decisiones del LLM con menor confianza
- La ejecución de OpenHands toma 1-5 minutos por corrida
- GitHub API tiene límite de 100MB por archivo
- Groq tier gratuito: 100K tokens/día (~5-10 ejecuciones completas del pipeline)
- Los outputs de OpenHands son no deterministas: dos ejecuciones sobre el mismo input pueden producir código ligeramente diferente

---

### 📁 Estructura del Proyecto

```
datapulse/
├── README.md                 # Este archivo
├── LICENSE                   # Licencia MIT
├── requirements.txt          # Dependencias de Python
├── datapulse_pipeline.py      # Pipeline principal con Grafo de Conocimiento
├── quality_report.json       # Reporte de calidad generado
├── Dummy Data HSS.csv        # Dataset de muestra
├── api/
│   └── main.py               # Endpoints de FastAPI
├── src/
│   └── quality_checker.py    # Módulo de análisis de calidad
├── data/
│   ├── california_housing.csv
│   └── superstore_sales.csv
└── test/
    └── __init__.py
```

---

### 📜 Licencia

Licencia MIT - Ver archivo [LICENSE](LICENSE) para más detalles.
