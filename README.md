# DataPulse — Autonomous Data Quality & Cleaning Pipeline

> **[English](#english)** | **[Español](#español)**

---

<a name="english"></a>

## English

### What is DataPulse?

DataPulse is an autonomous data quality and cleaning pipeline that takes a raw dataset as input and produces a clean dataset, a quality report, and a reproducible script — all versioned automatically in GitHub — without any manual configuration of cleaning rules.

You upload a file. DataPulse does the rest.

### How It Works

DataPulse combines four technologies in a single orchestrated pipeline:

| Component | Technology | Role |
|---|---|---|
| **Orchestrator** | LangGraph | State machine with 7 nodes, conditional retry logic |
| **Reasoning** | LLaMA 3.3 70B via Groq | Diagnoses column types, detects problems, builds technical brief |
| **Decision Rules** | NetworkX (Knowledge Graph) | ~58 deterministic statistical rules for cleaning decisions |
| **Autonomous Agent** | OpenHands Cloud API | Writes, executes, and validates the cleaning code autonomously |

### Pipeline Architecture

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
[6] node_validar_score ────── Checks quality score >= threshold
         │
    ┌────┴─────┐
    ok         fail → retry (max 3 attempts)
    │
    ▼
[7] node_commit ───────────── Pushes validated code to GitHub feature branch
```

### Knowledge Graph

The Knowledge Graph is a `MultiDiGraph` (NetworkX) with ~58 rules organized in 6 categories:

1. **Structural Integrity** — duplicates, empty rows/columns, column name normalization
2. **String Cleaning** — whitespace, line breaks, case normalization, email/phone validation
3. **Type Conversion** — datetime parsing, boolean mapping, numeric coercion
4. **Missing Data Treatment** — mean/median/mode imputation, KNN imputer, forward fill, interpolation
5. **Outlier Detection** — winsorization, Isolation Forest, Local Outlier Factor
6. **Scaling & Encoding** — MinMax, Standard, Robust scaling, OneHot/Ordinal encoding

Each rule contains: action, confidence score (0.78–0.90), parameters, and bilingual justification.

When the graph has no rule for a specific case, the decision falls back to the LLM with a maximum confidence of 0.70.

### What It Produces

| File | Description |
|---|---|
| `datapulse_pipeline.py` | Executable Python script that reproduces the entire cleaning process |
| `processed_data.csv` | Clean dataset ready for analysis or modeling |
| `quality_report.json` | Quality metrics: null counts, types, statistics, duplicates |

### Example Output

```
[1/7] Removing duplicates...        → 0 removed
[2/7] Structural dropna...          → 0 rows removed
[3/7] String cleaning...            → 1 string column cleaned
[4/7] Type conversion...            → numeric types verified
[5/7] Applying fillna_mean...       → TV: 10 nulls, Radio: 4, Social Media: 6, Sales: 6
[6/7] Outliers handling...          → 29 outliers detected (IQR method)
[7/7] Applying onehot encoding...   → Influencer → 4 columns (Macro, Mega, Micro, Nano)

Result: 4572 rows × 5 cols → 4572 rows × 9 cols | 26 nulls → 0 nulls
```

### Requirements

```
langgraph
langchain-groq
networkx
pandas
numpy
openpyxl
pyarrow
pygithub
requests
```

### API Keys Required

| Key | Service | Purpose |
|---|---|---|
| `GROQ_API_KEY` | [Groq Cloud](https://console.groq.com) | LLM inference (LLaMA 3.3 70B) |
| `OPENHANDS_API_KEY` | [OpenHands](https://app.all-hands.dev) | Autonomous code agent |
| `GITHUB_TOKEN` | [GitHub](https://github.com/settings/tokens) | Repository access (scope: repo) |

### Quick Start

1. Open the notebook in Google Colab
2. Configure the 3 API keys in Colab Secrets
3. Run all cells in order
4. Upload your dataset when prompted
5. Wait 3-5 minutes
6. Download `processed_data.csv` — your clean dataset

### Limitations

- Knowledge Graph covers common statistical cases. Domain-specific datasets (medical, regulated financial) may fall back to LLM decisions with lower confidence
- OpenHands execution takes 1-5 minutes per run
- GitHub API has a 100MB file size limit
- Groq free tier: 100K tokens/day — approximately 5-10 full pipeline runs
- OpenHands outputs are non-deterministic: two runs on the same input may produce slightly different code

### Tech Stack Decision: Why This Combination?

**Why LangGraph over plain Python?** Retry logic with accumulated error history requires persistent state across nodes. LangGraph provides this natively with TypedDict state and conditional edges.

**Why a Knowledge Graph over RAG?** Data cleaning decisions need deterministic, traceable rules — not text similarity search. The Knowledge Graph navigates explicit logic: variable type + problem → action + justification + confidence.

**Why OpenHands over direct LLM code generation?** OpenHands doesn't just generate code — it executes it, observes errors, and self-corrects through its Act→Observe→Reason cycle. A direct LLM call would require a separate validation sandbox.

### License

MIT

---

<a name="español"></a>

## Español

### ¿Qué es DataPulse?

DataPulse es un pipeline autónomo de calidad y limpieza de datos que recibe un dataset crudo como input y produce un dataset limpio, un reporte de calidad y un script reproducible — todo versionado automáticamente en GitHub — sin configuración manual de reglas de limpieza.

Subes un archivo. DataPulse hace el resto.

### Cómo Funciona

DataPulse combina cuatro tecnologías en un pipeline orquestado:

| Componente | Tecnología | Rol |
|---|---|---|
| **Orquestador** | LangGraph | Máquina de estados con 7 nodos y lógica de retry condicional |
| **Razonamiento** | LLaMA 3.3 70B vía Groq | Diagnostica tipos de columnas, detecta problemas, construye brief técnico |
| **Reglas de Decisión** | NetworkX (Grafo de Conocimiento) | ~58 reglas estadísticas deterministas para decisiones de limpieza |
| **Agente Autónomo** | OpenHands Cloud API | Escribe, ejecuta y valida el código de limpieza de forma autónoma |

### Arquitectura del Pipeline

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
[6] node_validar_score ────── Verifica score de calidad >= umbral
         │
    ┌────┴─────┐
    ok         falla → retry (máx 3 intentos)
    │
    ▼
[7] node_commit ───────────── Push del código validado a rama feature en GitHub
```

### Grafo de Conocimiento

El Grafo de Conocimiento es un `MultiDiGraph` (NetworkX) con ~58 reglas organizadas en 6 categorías:

1. **Integridad Estructural** — duplicados, filas/columnas vacías, normalización de nombres
2. **Limpieza de Texto** — espacios, saltos de línea, normalización de caja, validación email/teléfono
3. **Conversión de Tipos** — parseo de fechas, mapeo booleano, coerción numérica
4. **Tratamiento de Datos Faltantes** — imputación media/mediana/moda, KNN imputer, forward fill, interpolación
5. **Detección de Outliers** — winsorización, Isolation Forest, Local Outlier Factor
6. **Escalado y Encoding** — MinMax, Standard, Robust scaling, OneHot/Ordinal encoding

Cada regla contiene: acción, confianza (0.78–0.90), parámetros y justificación bilingüe.

Cuando el grafo no tiene regla para un caso específico, la decisión cae al LLM con confianza máxima de 0.70.

### Qué Produce

| Archivo | Descripción |
|---|---|
| `datapulse_pipeline.py` | Script Python ejecutable que reproduce toda la limpieza |
| `processed_data.csv` | Dataset limpio listo para análisis o modelado |
| `quality_report.json` | Métricas de calidad: nulos, tipos, estadísticas, duplicados |

### Ejemplo de Ejecución

```
[1/7] Removiendo duplicados...       → 0 eliminados
[2/7] Dropna estructural...          → 0 filas eliminadas
[3/7] Limpieza de strings...         → 1 columna de texto limpiada
[4/7] Conversión de tipos...         → tipos numéricos verificados
[5/7] Aplicando fillna_mean...       → TV: 10 nulos, Radio: 4, Social Media: 6, Sales: 6
[6/7] Tratamiento de outliers...     → 29 outliers detectados (método IQR)
[7/7] Aplicando onehot encoding...   → Influencer → 4 columnas (Macro, Mega, Micro, Nano)

Resultado: 4572 filas × 5 cols → 4572 filas × 9 cols | 26 nulos → 0 nulos
```

### Requisitos

```
langgraph
langchain-groq
networkx
pandas
numpy
openpyxl
pyarrow
pygithub
requests
```

### API Keys Necesarias

| Key | Servicio | Propósito |
|---|---|---|
| `GROQ_API_KEY` | [Groq Cloud](https://console.groq.com) | Inferencia LLM (LLaMA 3.3 70B) |
| `OPENHANDS_API_KEY` | [OpenHands](https://app.all-hands.dev) | Agente autónomo de código |
| `GITHUB_TOKEN` | [GitHub](https://github.com/settings/tokens) | Acceso al repositorio (scope: repo) |

### Inicio Rápido

1. Abrir el notebook en Google Colab
2. Configurar las 3 API keys en Colab Secrets
3. Ejecutar todas las celdas en orden
4. Subir tu dataset cuando se solicite
5. Esperar 3-5 minutos
6. Descargar `processed_data.csv` — tu dataset limpio

### Limitaciones

- El Grafo de Conocimiento cubre casos estadísticos comunes. Datasets de dominio muy específico (médico, financiero regulado) pueden caer en decisiones del LLM con menor confianza
- La ejecución de OpenHands toma 1-5 minutos por corrida
- GitHub API tiene límite de 100MB por archivo
- Groq tier gratuito: 100K tokens/día — aproximadamente 5-10 ejecuciones completas del pipeline
- Los outputs de OpenHands son no deterministas: dos ejecuciones sobre el mismo input pueden producir código ligeramente diferente

### Decisiones Técnicas: ¿Por Qué Esta Combinación?

**¿Por qué LangGraph sobre Python puro?** La lógica de retry con historial de errores acumulado requiere estado persistente entre nodos. LangGraph lo proporciona nativamente con estado TypedDict y edges condicionales.

**¿Por qué un Grafo de Conocimiento en vez de RAG?** Las decisiones de limpieza de datos necesitan reglas deterministas y trazables, no búsqueda de similitud textual. El Grafo de Conocimiento navega lógica explícita: tipo de variable + problema → acción + justificación + confianza.

**¿Por qué OpenHands en vez de generación directa con LLM?** OpenHands no solo genera código — lo ejecuta, observa errores y se autocorrige mediante su ciclo Act→Observe→Reason. Una llamada directa al LLM requeriría un sandbox de validación separado.

### Licencia

MIT
