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
| **Decision Rules** | NetworkX (Knowledge Graph) | 139 rules: 113 base statistical/structural rules + 26 deterministic domain rules (Health/WHO-AHA, Finance/IFRS, Economics/IMF, Occupational Safety/OSHA) |
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
[6] node_validar_score ────── Recomputes the score on the real processed_data.csv
         │
    ┌────┴─────┐
    ok         fail → retry (infra failure: 2 attempts total;
    │                       low score / not validated: 3 attempts total)
    ▼
[7] node_commit ───────────── Pushes validated code to GitHub feature branch
```

### Knowledge Graph

The Knowledge Graph is a `MultiDiGraph` (NetworkX) with **139 rules (edges)**: 113 base statistical/structural rules organized in 6 categories, plus 26 deterministic domain rules (Health/WHO-AHA, Finance/IFRS, Economics/IMF, Occupational Safety/OSHA). The 113 base rules are the 106 original ones plus 7 `Valores_Centinela` rules added in phase 1: `reemplazar_centinela_por_nan` for numeric/financial columns, `replace_to_na` for categorical and text columns (same string list as the empty-string rule), and `marcar_fecha_centinela` for dates, which only **flags** the sentinel dates listed in `DATAPULSE_FECHAS_CENTINELA` (empty by default) and never imputes them:

1. **Structural Integrity** — duplicates, empty rows/columns, column name normalization
2. **String Cleaning** — whitespace, line breaks, case normalization, email/phone validation
3. **Type Conversion** — datetime parsing, boolean mapping, numeric coercion
4. **Missing Data Treatment** — mean/median/mode imputation, KNN imputer, forward fill, interpolation
5. **Outlier Detection** — winsorization, Isolation Forest, Local Outlier Factor
6. **Scaling & Encoding** — MinMax, Standard, Robust scaling, OneHot/Ordinal encoding

Each rule contains: action, confidence score (between 0.78 and 1.0), parameters, and bilingual justification.

When the graph has no rule for a specific case, the decision falls back to the LLM with a maximum confidence of 0.70.

### What It Produces

| File | Description |
|---|---|
| `datapulse_pipeline.py` | Executable Python script that reproduces the entire cleaning process |
| `processed_data.csv` | Clean dataset ready for analysis or modeling (omitted from commit if >50MB) |
| `quality_report.json` | Quality metrics written by the generated script (its score is informative only: the real score is recomputed independently) |
| `cleaning_log.json` | Comprehensive audit log: every single decision taken with its bilingual justification |
| `audit_log.json` | Run summary: before/after/reported scores and every planned action with its source (graph or LLM) |

> `eda_report.html` is **not implemented** yet: earlier versions of this README listed it, but the pipeline does not generate it.

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

### Validation and Retry Policy

The score that decides the commit is **not** the one written by the generated script.
`node_validar_score` recomputes it with `src/quality_checker.py`:

- `score_antes`: quality of the original dataset.
- `score_despues`: quality of the real `processed_data.csv` produced by the run.
- `score_reportado`: what the script wrote in `quality_report.json` (informative only).

If there is no `processed_data.csv` to read, the attempt ends as `sin_validar` and **nothing is
committed**. It is retried only when the file should have existed and did not appear (the script
did not write it, or the agent did not commit it); if the results could not be read at all,
retrying would not help and the run stops. A `processed_data.csv` that exists but is not a valid
dataset (empty, unreadable) counts as a validated failure with score 0. When the score is below `DATAPULSE_MIN_SCORE_PASS`, a summary of the report (score
and main problems) is added to the error history so the next attempt knows what to fix.

Attempt limits are **total attempts**, the first one included:

| Situation | Status | Total attempts |
|---|---|---|
| Infrastructure failure (`error`, `timeout`, `stuck`, `stopped`) or the script crashed | `error`… | 2 (`OH_MAX_INTENTOS_INFRA`) |
| Score below the threshold, or `processed_data.csv` should have existed and did not appear | `finished` / `sin_validar` | 3 (`OH_MAX_INTENTOS_SCORE`) |
| Results could not be read (`sin_validar` with nothing to expect) | `sin_validar` | 1 (no retry) |
| Configuration or policy rejection (missing user/repo, data upload not allowed, results not readable) | `rechazado` | 1 (no retry, no sandbox) |

### Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `GROQ_API_KEY` | — | Groq LLM (required) |
| `GROQ_MODEL` | `openai/gpt-oss-120b` | Groq model |
| `OPENHANDS_API_KEY` | empty | Empty → **local mode** (the script is generated with Groq and run on your machine) |
| `GITHUB_TOKEN` | empty | Empty → outputs are saved in `outputs/` |
| `GITHUB_USERNAME`, `GITHUB_REPO` | **no default** | Target repo. If missing, OpenHands mode does not start and outputs are saved locally |
| `DATAPULSE_MIN_SCORE_PASS` | `70` | Minimum independent score to commit |
| `DATAPULSE_PERMITIR_SUBIDA_DATOS` | unset | `1` allows uploading data files to GitHub (only if the repo is private) |
| `DATAPULSE_ENMASCARAR` | `1` | `0` disables masking of the samples sent to the LLM |
| `DATAPULSE_FECHAS_CENTINELA` | empty | Comma-separated sentinel dates (`1900-01-01,9999-12-31`) to **flag** (never impute) |
| `DATAPULSE_API_MAX_MB` | `100` | Max upload size per file in the API (413 if exceeded) |

### Data Privacy

- **Data upload policy.** The raw dataset and any data file (`.csv`, `.tsv`, `.xlsx`, `.xls`,
  `.parquet`, data `.json`) are uploaded to GitHub **only** if `DATAPULSE_PERMITIR_SUBIDA_DATOS=1`
  **and** the repository is private. Otherwise OpenHands mode does not start (status
  `rechazado`, before any branch or sandbox is created) and suggests local mode: the dataset could
  not be uploaded and `processed_data.csv` could not be read back to validate it. This also applies
  to a file that is already in GitHub (`github_file=`): it does not count as an upload, but its
  results would still be unreadable. In local mode, `node_commit` uploads only the script, the
  reports and `audit_log.json`; data files stay in `outputs/`.
- **Masked samples.** Before sending column samples to the LLM, emails become `u***@domain`,
  sequences of 6+ digits (IDs, phones, accounts) become `9`s of the same length, and
  high-cardinality text columns (>50 % unique: names, free text) become a shape pattern
  (`José Muñoz` → `Aaaa Aaaaa`). Low-cardinality categories are kept as they are.
- **Generated code environment.** The locally generated script runs with a minimal environment
  (`PATH`, `HOME`, `LANG`, `PYTHONIOENCODING`) and no API keys. Full isolation (container without
  network) is planned for phase 2.

### Quick Start

#### Option A: Google Colab (Recommended for GUI)
1. Open the notebook in Google Colab
2. **Clone the full repository** in the first cell — `datapulse_prod.py` imports the data
   loader from `src/`, so uploading the script alone is not enough:
   ```python
   !git clone https://github.com/<your_user>/datapulse.git
   %cd datapulse
   !pip install -r requirements.txt
   ```
3. Configure the API keys in Colab Secrets (`GROQ_API_KEY`, `GITHUB_TOKEN`, `OPENHANDS_API_KEY`) and
   set the target repo as environment variables (they have no default):
   ```python
   import os
   os.environ["GITHUB_USERNAME"] = "<your_user>"
   os.environ["GITHUB_REPO"] = "datapulse"
   ```
4. Run all cells in order
5. Upload your dataset when prompted
6. Wait 3-5 minutes
7. Download `processed_data.csv` — your clean dataset (DataPulse downloads it from the real
   run output folder)

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
| **Reglas de Decisión** | NetworkX (Grafo de Conocimiento) | 139 reglas: 113 reglas estadísticas/estructurales base + 26 reglas deterministas de dominio (Salud/OMS-AHA, Finanzas/IFRS, Economía/FMI, Seguridad Ocupacional/OSHA) |
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
[6] node_validar_score ────── Recalcula el score sobre el processed_data.csv real
         │
    ┌────┴─────┐
    ok         falla → retry (fallo de infraestructura: 2 intentos en total;
    │                         score bajo / sin validar: 3 intentos en total)
    ▼
[7] node_commit ───────────── Push del código validado a rama feature en GitHub
```

### Grafo de Conocimiento

El Grafo de Conocimiento es un `MultiDiGraph` (NetworkX) con **139 reglas (aristas)**: 113 reglas estadísticas/estructurales base organizadas en 6 categorías, más 26 reglas deterministas de dominio (Salud/OMS-AHA, Finanzas/IFRS, Economía/FMI, Seguridad Ocupacional/OSHA). Las 113 base son las 106 originales más 7 reglas de `Valores_Centinela` añadidas en la fase 1: `reemplazar_centinela_por_nan` en columnas numéricas y financieras, `replace_to_na` en categóricas y texto (con la misma lista de cadenas que la regla de cadenas vacías) y `marcar_fecha_centinela` en fechas, que solo **marca con un flag** las fechas centinela definidas en `DATAPULSE_FECHAS_CENTINELA` (vacía por defecto) y nunca las imputa:

1. **Integridad Estructural** — duplicados, filas/columnas vacías, normalización de nombres
2. **Limpieza de Texto** — espacios, saltos de línea, normalización de caja, validación email/teléfono
3. **Conversión de Tipos** — parseo de fechas, mapeo booleano, coerción numérica
4. **Tratamiento de Datos Faltantes** — imputación media/mediana/moda, KNN imputer, forward fill, interpolación
5. **Detección de Outliers** — winsorización, Isolation Forest, Local Outlier Factor
6. **Escalado y Encoding** — MinMax, Standard, Robust scaling, OneHot/Ordinal encoding

Cada regla contiene: acción, confianza (entre 0,78 y 1,0), parámetros y justificación bilingüe.

Cuando el grafo no tiene regla para un caso específico, la decisión cae al LLM con confianza máxima de 0.70.

### Qué Produce

| Archivo | Descripción |
|---|---|
| `datapulse_pipeline.py` | Script Python ejecutable que reproduce toda la limpieza |
| `processed_data.csv` | Dataset limpio listo para análisis o modelado (se omite del commit si >50MB) |
| `quality_report.json` | Métricas de calidad que escribe el script generado (su score es solo informativo: el score real se recalcula de forma independiente) |
| `cleaning_log.json` | Auditoría completa: cada decisión tomada con su respectiva justificación bilingüe |
| `audit_log.json` | Resumen de la ejecución: scores antes/después/reportado y cada acción planificada con su fuente (grafo o LLM) |

> `eda_report.html` **no está implementado** todavía: versiones anteriores de este README lo listaban, pero el pipeline no lo genera.

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

### Validación y Política de Intentos

El score que decide el commit **no** es el que escribe el script generado.
`node_validar_score` lo recalcula con `src/quality_checker.py`:

- `score_antes`: calidad del dataset original.
- `score_despues`: calidad del `processed_data.csv` real de la ejecución.
- `score_reportado`: lo que escribió el script en `quality_report.json` (solo informativo).

Si no hay `processed_data.csv` que leer, el intento termina como `sin_validar` y **no hay
commit**. Solo se reintenta cuando el archivo debía existir y no apareció (el script no lo escribió
o el agente no hizo commit de él); si los resultados no se pudieron leer, reintentar no lo arregla
y la ejecución termina. Un `processed_data.csv` que existe pero no es un dataset válido (vacío,
ilegible) cuenta como un fallo validado con score 0. Cuando el score queda bajo `DATAPULSE_MIN_SCORE_PASS`, se agrega al historial de errores
un resumen del reporte (score y principales problemas) para que el siguiente intento sepa qué
corregir.

Los límites son **intentos totales**, incluido el primero:

| Situación | Estado | Intentos totales |
|---|---|---|
| Fallo de infraestructura (`error`, `timeout`, `stuck`, `stopped`) o el script falló | `error`… | 2 (`OH_MAX_INTENTOS_INFRA`) |
| Score bajo el umbral, o `processed_data.csv` debía existir y no apareció | `finished` / `sin_validar` | 3 (`OH_MAX_INTENTOS_SCORE`) |
| Resultados ilegibles (`sin_validar` sin nada que esperar) | `sin_validar` | 1 (sin reintento) |
| Rechazo por configuración o política (falta usuario/repo, subida de datos no permitida, resultados ilegibles) | `rechazado` | 1 (sin reintento ni sandbox) |

### Variables de Entorno

| Variable | Por defecto | Uso |
|---|---|---|
| `GROQ_API_KEY` | — | LLM de Groq (obligatoria) |
| `GROQ_MODEL` | `openai/gpt-oss-120b` | Modelo de Groq |
| `OPENHANDS_API_KEY` | vacía | Vacía → **modo local** (el script se genera con Groq y se ejecuta en tu máquina) |
| `GITHUB_TOKEN` | vacía | Vacía → las salidas se guardan en `outputs/` |
| `GITHUB_USERNAME`, `GITHUB_REPO` | **sin valor por defecto** | Repo destino. Si faltan, el modo OpenHands no arranca y las salidas se guardan en local |
| `DATAPULSE_MIN_SCORE_PASS` | `70` | Score independiente mínimo para hacer commit |
| `DATAPULSE_PERMITIR_SUBIDA_DATOS` | sin definir | `1` permite subir archivos de datos a GitHub (solo si el repo es privado) |
| `DATAPULSE_ENMASCARAR` | `1` | `0` desactiva el enmascarado de las muestras que se envían al LLM |
| `DATAPULSE_FECHAS_CENTINELA` | vacía | Fechas centinela separadas por comas (`1900-01-01,9999-12-31`) que se **marcan** (nunca se imputan) |
| `DATAPULSE_API_MAX_MB` | `100` | Tamaño máximo por archivo en la API (413 si se supera) |

### Privacidad de los Datos

- **Política de subida de datos.** El dataset crudo y cualquier archivo de datos (`.csv`, `.tsv`,
  `.xlsx`, `.xls`, `.parquet`, `.json` de datos) se suben a GitHub **solo** si
  `DATAPULSE_PERMITIR_SUBIDA_DATOS=1` **y** el repositorio es privado. Si no, el modo OpenHands no
  arranca (estado `rechazado`, antes de crear rama o sandbox) y sugiere el modo local: no se podría
  subir el dataset ni leer de vuelta `processed_data.csv` para validarlo. Esto aplica también a un
  archivo que ya está en GitHub (`github_file=`): no cuenta como subida, pero sus resultados
  seguirían siendo ilegibles. En modo local, `node_commit` sube solo el script, los reportes y
  `audit_log.json`; los archivos de datos quedan en `outputs/`.
- **Muestras enmascaradas.** Antes de enviar las muestras de cada columna al LLM, los emails pasan
  a `u***@dominio`, las secuencias de 6 o más dígitos (cédulas, teléfonos, cuentas) a `9` de la
  misma longitud y las columnas de texto de alta cardinalidad (>50 % únicos: nombres, texto libre)
  a un patrón de forma (`José Muñoz` → `Aaaa Aaaaa`). Las categorías de baja cardinalidad se
  mantienen tal cual.
- **Entorno del código generado.** El script generado en modo local se ejecuta con un entorno
  mínimo (`PATH`, `HOME`, `LANG`, `PYTHONIOENCODING`) y sin claves de API. El aislamiento completo
  (contenedor sin red) queda para la fase 2.

### Inicio Rápido

#### Opción A: Google Colab (Recomendado para Interfaz Gráfica)
1. Abrir el notebook en Google Colab
2. **Clonar el repositorio completo** en la primera celda — `datapulse_prod.py` importa el
   cargador de datos desde `src/`, así que no basta con subir solo el script:
   ```python
   !git clone https://github.com/<tu_usuario>/datapulse.git
   %cd datapulse
   !pip install -r requirements.txt
   ```
3. Configurar las API keys en Colab Secrets (`GROQ_API_KEY`, `GITHUB_TOKEN`, `OPENHANDS_API_KEY`) y
   definir el repo destino como variables de entorno (no tienen valor por defecto):
   ```python
   import os
   os.environ["GITHUB_USERNAME"] = "<tu_usuario>"
   os.environ["GITHUB_REPO"] = "datapulse"
   ```
4. Ejecutar todas las celdas en orden
5. Subir tu dataset cuando se solicite
6. Esperar 3-5 minutos
7. Descargar `processed_data.csv` — tu dataset limpio (DataPulse lo descarga desde la
   carpeta real de salida de la ejecución)

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
