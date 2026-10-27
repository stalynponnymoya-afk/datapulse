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
| **Reasoning** | Groq-hosted LLM (user-selectable, default `openai/gpt-oss-120b`) | Diagnoses column types, detects problems, builds technical brief |
| **Decision Rules** | NetworkX (Knowledge Graph) | ~58 base statistical rules + 24 deterministic domain rules (Health/WHO-AHA, Finance/IFRS, Economics/IMF, Occupational Safety/OSHA) — over 100 edges total |
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

The Knowledge Graph is a `MultiDiGraph` (NetworkX) with ~58 base statistical rules organized in 6 categories, plus 24 deterministic domain rules (Health/WHO-AHA, Finance/IFRS, Economics/IMF, Occupational Safety/OSHA) — over 100 edges total:

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
| `processed_data.csv` | Clean dataset ready for analysis or modeling (omitted from commit if >50MB) |
| `quality_report.json` | Quality metrics: null counts, types, statistics, duplicates, and before/after quality scores |
| `cleaning_log.json` | Comprehensive audit log: every single decision taken with its bilingual justification |
| `eda_report.html` | Visual Exploratory Data Analysis report with distributions, correlations, and summary plots |

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
| `GROQ_API_KEY` | [Groq Cloud](https://console.groq.com) | LLM inference (model selectable via `GROQ_MODEL`) |
| `OPENHANDS_API_KEY` | [OpenHands](https://app.all-hands.dev) | Autonomous code agent |
| `GITHUB_TOKEN` | [GitHub](https://github.com/settings/tokens) | Repository access (scope: repo) |

> **About the LLM model.** The model is user-selectable via the `GROQ_MODEL` environment
> variable; the default is `openai/gpt-oss-120b`. We originally used
> `llama-3.3-70b-versatile` because it was free on Groq's tier, but Groq **deprecated it
> on 16 Aug 2026** (it moved to Enterprise / contact-sales). Pick any model your Groq
> account has access to — see the [Groq supported models](https://console.groq.com/docs/models).
>
> **Reasoning models** (GPT-OSS, Qwen, Muse, MiniMax) spend part of the token budget
> thinking before answering. DataPulse detects them and raises `max_tokens` to 8192 with
> `reasoning_effort="low"`, so the JSON reply is not truncated. If you pick a model with a
> very low completion limit, raise `max_tokens` in `get_groq_llm()` accordingly.

### Quick Start

#### Option A: Google Colab (Recommended for GUI)
1. Open the notebook in Google Colab
2. Configure the 3 API keys in Colab Secrets
3. Run all cells in order
4. Upload your dataset when prompted
5. Wait 3-5 minutes
6. Download `processed_data.csv` — your clean dataset

#### Option B: Local Execution (Standard Python Environment)
1. Clone the repository and install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   This covers the orchestrator, the `src/` package, the API and the tests. For the
   generated pipeline's advanced cleaning (scipy/scikit-learn), legacy `.xls` files or
   EDA charts, also install the optional set:
   ```bash
   pip install -r requirements.txt -r requirements-optional.txt
   ```
2. Configure the required environment variables:
   * On Linux/macOS:
     ```bash
     export GROQ_API_KEY="your_groq_key"
     export GROQ_MODEL="openai/gpt-oss-120b"   # optional, override the default model
     export OPENHANDS_API_KEY="your_openhands_key"
     export GITHUB_TOKEN="your_github_token"
     export GITHUB_USERNAME="your_github_username"
     export GITHUB_REPO="datapulse"
     ```
   * On Windows (PowerShell):
     ```powershell
     $env:GROQ_API_KEY="your_groq_key"
     $env:GROQ_MODEL="openai/gpt-oss-120b"   # optional, override the default model
     $env:OPENHANDS_API_KEY="your_openhands_key"
     $env:GITHUB_TOKEN="your_github_token"
     $env:GITHUB_USERNAME="your_github_username"
     $env:GITHUB_REPO="datapulse"
     ```
3. Run the orchestrator script:
   ```bash
   python datapulse_prod.py
   ```
   *Note: If executed locally, the script automatically detects CSV/XLSX/JSON/Parquet files in the current working directory, or automatically generates a mock dataset (`ventas_prueba.csv`) to test-run the complete pipeline.*

### Limitations

- Knowledge Graph covers common statistical cases. Domain-specific datasets (medical, regulated financial) may fall back to LLM decisions with lower confidence
- OpenHands execution takes 1-5 minutes per run
- GitHub API has a 100MB file size limit
- LLM rate limits depend on the Groq model you select via `GROQ_MODEL` and your plan tier
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
| **Razonamiento** | LLM alojado en Groq (elegible por el usuario, por defecto `openai/gpt-oss-120b`) | Diagnostica tipos de columnas, detecta problemas, construye brief técnico |
| **Reglas de Decisión** | NetworkX (Grafo de Conocimiento) | ~58 reglas estadísticas base + 24 reglas deterministas de dominio (Salud/OMS-AHA, Finanzas/IFRS, Economía/FMI, Seguridad Ocupacional/OSHA) — más de 100 aristas totales |
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

El Grafo de Conocimiento es un `MultiDiGraph` (NetworkX) con ~58 reglas estadísticas base organizadas en 6 categorías, más 24 reglas deterministas de dominio (Salud/OMS-AHA, Finanzas/IFRS, Economía/FMI, Seguridad Ocupacional/OSHA) — superando las 100 aristas totales:

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
| `processed_data.csv` | Dataset limpio listo para análisis o modelado (se omite del commit si >50MB) |
| `quality_report.json` | Métricas de calidad: nulos, tipos, estadísticas, duplicados y scores antes/después |
| `cleaning_log.json` | Auditoría completa: cada decisión tomada con su respectiva justificación bilingüe |
| `eda_report.html` | Reporte visual de análisis exploratorio (EDA) con distribuciones, correlaciones y resumen |

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
| `GROQ_API_KEY` | [Groq Cloud](https://console.groq.com) | Inferencia LLM (modelo elegible vía `GROQ_MODEL`) |
| `OPENHANDS_API_KEY` | [OpenHands](https://app.all-hands.dev) | Agente autónomo de código |
| `GITHUB_TOKEN` | [GitHub](https://github.com/settings/tokens) | Acceso al repositorio (scope: repo) |

> **Sobre el modelo LLM.** El modelo es elegible por el usuario mediante la variable de
> entorno `GROQ_MODEL`; por defecto es `openai/gpt-oss-120b`. Originalmente usábamos
> `llama-3.3-70b-versatile` porque era gratuito en el tier de Groq, pero Groq **lo deprecó
> el 16/08/2026** (pasó a Enterprise / contacto con ventas). Usa cualquier modelo al que tu
> cuenta de Groq tenga acceso — ver [modelos soportados de Groq](https://console.groq.com/docs/models).
>
> **Modelos de razonamiento** (GPT-OSS, Qwen, Muse, MiniMax) gastan parte del presupuesto
> de tokens pensando antes de responder. DataPulse los detecta y sube `max_tokens` a 8192
> con `reasoning_effort="low"`, para que la respuesta JSON no se trunque. Si eliges un
> modelo con un límite de salida muy bajo, sube `max_tokens` en `get_groq_llm()`.

### Inicio Rápido

#### Opción A: Google Colab (Recomendado para Interfaz Gráfica)
1. Abrir el notebook en Google Colab
2. Configurar las 3 API keys en Colab Secrets
3. Ejecutar todas las celdas en orden
4. Subir tu dataset cuando se solicite
5. Esperar 3-5 minutos
6. Descargar `processed_data.csv` — tu dataset limpio

#### Opción B: Ejecución Local (Entorno Python Estándar)
1. Clona el repositorio e instala las dependencias:
   ```bash
   pip install -r requirements.txt
   ```
   Esto cubre el orquestador, el paquete `src/`, la API y los tests. Para la limpieza
   avanzada del pipeline generado (scipy/scikit-learn), ficheros `.xls` heredados o
   gráficos EDA, instala también el conjunto opcional:
   ```bash
   pip install -r requirements.txt -r requirements-optional.txt
   ```
2. Configura las variables de entorno obligatorias:
   * En Linux/macOS:
     ```bash
     export GROQ_API_KEY="tu_key_groq"
     export GROQ_MODEL="openai/gpt-oss-120b"   # opcional, sobrescribe el modelo por defecto
     export OPENHANDS_API_KEY="tu_key_openhands"
     export GITHUB_TOKEN="tu_token_github"
     export GITHUB_USERNAME="tu_usuario_github"
     export GITHUB_REPO="datapulse"
     ```
   * En Windows (PowerShell):
     ```powershell
     $env:GROQ_API_KEY="tu_key_groq"
     $env:GROQ_MODEL="openai/gpt-oss-120b"   # opcional, sobrescribe el modelo por defecto
     $env:OPENHANDS_API_KEY="tu_key_openhands"
     $env:GITHUB_TOKEN="tu_token_github"
     $env:GITHUB_USERNAME="tu_usuario_github"
     $env:GITHUB_REPO="datapulse"
     ```
3. Ejecuta el script del orquestador:
   ```bash
   python datapulse_prod.py
   ```
   *Nota: Si se ejecuta de forma local, el script detectará automáticamente archivos de datos (CSV/XLSX/JSON/Parquet) en tu directorio de trabajo actual, o generará un dataset de prueba mock (`ventas_prueba.csv`) de forma automática para simular el flujo completo del pipeline.*

### Limitaciones

- El Grafo de Conocimiento cubre casos estadísticos comunes. Datasets de dominio muy específico (médico, financiero regulado) pueden caer en decisiones del LLM con menor confianza
- La ejecución de OpenHands toma 1-5 minutos por corrida
- GitHub API tiene límite de 100MB por archivo
- Los límites de tasa del LLM dependen del modelo de Groq que elijas vía `GROQ_MODEL` y de tu plan
- Los outputs de OpenHands son no deterministas: dos ejecuciones sobre el mismo input pueden producir código ligeramente diferente

### Decisiones Técnicas: ¿Por Qué Esta Combinación?

**¿Por qué LangGraph sobre Python puro?** La lógica de retry con historial de errores acumulado requiere estado persistente entre nodos. LangGraph lo proporciona nativamente con estado TypedDict y edges condicionales.

**¿Por qué un Grafo de Conocimiento en vez de RAG?** Las decisiones de limpieza de datos necesitan reglas deterministas y trazables, no búsqueda de similitud textual. El Grafo de Conocimiento navega lógica explícita: tipo de variable + problema → acción + justificación + confianza.

**¿Por qué OpenHands en vez de generación directa con LLM?** OpenHands no solo genera código — lo ejecuta, observa errores y se autocorrige mediante su ciclo Act→Observe→Reason. Una llamada directa al LLM requeriría un sandbox de validación separado.

### Licencia

MIT
