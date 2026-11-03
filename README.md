# DataPulse — Automatic Data Cleaning with an Editable Knowledge Graph

> **[English](#english)** | **[Español](#español)**

---

## English

### What is DataPulse?

DataPulse cleans datasets automatically. You give it a dirty file (CSV, TSV, Excel, JSON or Parquet) and it returns:

- the clean file (`processed_data.csv`);
- the Python script that did the cleaning (`datapulse_pipeline.py`);
- a report that explains every decision and what could not be cleaned (`informe_final.md`).

The cleaning rules are not hidden inside an AI model: they live in a **knowledge graph** that you can read and edit for your line of business. Cleaning quality depends on that graph; DataPulse guarantees that the process is always the same, that every decision is recorded and that the result is validated independently.

### How it works

Seven steps, always in the same order:

```
file ─▶ [1] Read ─▶ [2] Diagnose ─▶ [3] Query the graph ─▶ [4] Plan
                      (LLM)           (rules, no LLM)        (LLM)
     ─▶ [5] Execute ─▶ [6] Validate ─┬─ passed ──────▶ [7] Save ─▶ final report
       (OpenHands or local LLM)      └─ not passed ──▶ back to [5] (limited attempts)
```

1. **Read.** Loads the file and summarizes each column: type, nulls, extreme values, negatives, date formats and a few examples. Personal data in the examples is masked before it is sent to the LLM.
2. **Diagnose (LLM).** The LLM (Claude or Groq) labels each column with its type and its problems, choosing from the labels the graph offers. If a column has negative values, it decides whether they are valid, invalid or uncertain, and says why.
3. **Query the graph (no LLM).** For each type-and-problem pair, the graph picks the rule with the highest confidence. This step is deterministic.
4. **Plan (LLM).** The LLM orders the actions and writes the cleaning instructions. It cannot change the graph's actions. If a problem has no rule, it proposes a fallback action with a maximum confidence of 0.70, labelled `llm_fallback`.
5. **Execute.** The cleaning script is written and run, either by OpenHands or by the LLM locally (see the next section).
6. **Validate.** DataPulse measures the quality of the clean file with its own analyzer (`src/quality_checker.py`); it does not trust the score the script gives itself. If the result is below the threshold or worse than the original, step 5 is repeated.
7. **Save.** Publishes the results to a GitHub branch or, if GitHub is not configured, to the `outputs/` folder.

At the end DataPulse writes the final report (`informe_final.md`). It is written without an LLM, from what was measured.

### Two ways to run it: local LLM or OpenHands

Both ways use the same graph, the same plan and the same validation, and produce the same files. The only difference is who writes and runs the script in step 5. DataPulse picks the mode depending on whether `OPENHANDS_API_KEY` is set.

**Local mode (no `OPENHANDS_API_KEY`)**

- The LLM (Claude or Groq) writes the script and DataPulse runs it on your machine or in Colab.
- You need: an Anthropic key (`ANTHROPIC_API_KEY`) or a Groq key (`GROQ_API_KEY`).
- It is fast: on a 515-row Excel file, a full run took about one minute.
- The script runs with your permissions, although with no API keys in its environment. Use it with data you trust, and do not mount folders with sensitive information.
- Recommended to get started and to work with your own data.

**OpenHands mode (with `OPENHANDS_API_KEY`)**

- The LLM writes the instructions; [OpenHands](https://app.all-hands.dev) writes the script with its own model, runs it, fixes errors and commits the results to a branch of a GitHub repository. Everything happens in OpenHands' isolated cloud environment.
- On top of the LLM key, you need:
  - `OPENHANDS_API_KEY`;
  - a **private** GitHub repository with a `main` (or `master`) branch — for example, created with a README — that OpenHands can access;
  - `GITHUB_TOKEN` with read and write access to that repository, plus `GITHUB_USERNAME` and `GITHUB_REPO` with its owner and name;
  - `DATAPULSE_PERMITIR_SUBIDA_DATOS=1`, because the dataset is uploaded to that repository. If the repository is public, OpenHands mode does not start.
- It is slower: several minutes per attempt, at most 10 minutes of start-up plus 15 of execution.
- Recommended if you prefer the code to run isolated from your machine, or if you want the agent's self-correction.

LLM calls per run: the diagnosis and the plan happen once (2 calls, plus 1 if the diagnosis missed columns). In local mode, each attempt adds 1 call because the LLM writes the script; in OpenHands mode the script is written by OpenHands with its own model.

### Quick start in Google Colab

**1. Store your keys as Colab secrets** (key icon in the sidebar) and give the notebook access to each one. Never put keys in the code or in the repository.

- `ANTHROPIC_API_KEY` (or `GROQ_API_KEY` if you use Groq).
- OpenHands mode only: `OPENHANDS_API_KEY`, `GITHUB_TOKEN`, `GITHUB_USERNAME` and `GITHUB_REPO`.

DataPulse reads each value from Colab secrets first and, if missing, from environment variables. Keys are read when `datapulse_prod` is imported: if you change a secret, restart the session.

**2. Clone the repository and install the dependencies.**

```python
%cd /content
!rm -rf datapulse
!git clone https://github.com/<your_user>/datapulse.git
!git -C datapulse log --oneline -1
!pip install -q -r datapulse/requirements.txt
```

pip may warn that `google-colab` expects other versions of pandas or requests; that is expected. Then **restart the session** (Runtime → Restart session) so the new versions load and no old code stays in memory.

**3. Run in local mode.**

```python
%cd /content/datapulse
import datapulse_prod as dp      # prints the detected configuration, never the keys

dp.OPENHANDS_API_KEY = ""        # forces local mode even if you have an OpenHands key
dp.CONTEXTO_DATASET = "Store sales: customer age, quantity, unit price and total."

estado = dp.run_datapulse("data/ventas_prueba.csv")   # or the path to your file
```

To use your own file, upload it with the file panel (or `google.colab.files.upload()`) and pass its path.

**4. Check the result.**

```python
print(open(estado["informe_final"]["ruta"] + "/informe_final.md", encoding="utf-8").read())

from google.colab import files
files.download(estado["ruta_processed"])   # downloads the clean file
```

**5. (Optional) Run in OpenHands mode.** First meet the requirements in the previous section. If you did not store `GITHUB_USERNAME` and `GITHUB_REPO` as secrets, set them as environment variables **before importing** (they are read on import), in a freshly restarted session:

```python
import os
os.environ["GITHUB_USERNAME"] = "<your_user>"
os.environ["GITHUB_REPO"] = "<your_private_repo>"      # exact name: hyphen ≠ underscore
os.environ["DATAPULSE_PERMITIR_SUBIDA_DATOS"] = "1"

%cd /content/datapulse
import datapulse_prod as dp
repo = dp._get_github_repo()                            # free pre-check
print(repo.full_name, "| private:", repo.private, "|", dp._subida_datos_permitida(repo))

estado = dp.run_datapulse("data/ventas_prueba.csv")
```

**Common problems**

- *Old code in memory or a changed secret that is not picked up:* restart the session and run the cells again.
- *404 in `_get_github_repo()`:* the token cannot access the repository, or the name or owner does not match.
- *Status `rechazado` in OpenHands mode:* the repository is public or `DATAPULSE_PERMITIR_SUBIDA_DATOS` is not `1`.

### Configuration

Options in the configuration block of `datapulse_prod.py`. In Colab, change them after importing (`dp.OPTION = ...`) and before `run_datapulse`:

- `CONTEXTO_DATASET` (default `""`): a free-text description of the dataset that helps the diagnosis. Example: `"Retail sales with returns; one row per purchase"`. If empty, the `DATAPULSE_CONTEXTO` environment variable is used.
- `DECISIONES_NEGATIVOS` (default `{}`): your decision on the negative values of a column, which replaces the LLM's. Example: `{"Temperature": "validos", "Age": "invalidos"}`.
- `MODO_SALIDA` (default `"bi"`): `"bi"` or `"ml"` (see [Output mode](#output-mode-bi-or-ml)).
- `DOMINIOS_ACTIVOS` (default `[]`): sets of specialized rules added to the graph: `"salud"`, `"finanzas"`, `"economia"`, `"ocupacional"`, the `"retail"` demo or your own.

Environment variables (those marked *secret* can also be Colab secrets, which take priority):

- `ANTHROPIC_API_KEY` (*secret*): Claude. If set, Claude is the default LLM.
- `GROQ_API_KEY` and `GROQ_MODEL` (*secret*; default model `openai/gpt-oss-120b`): Groq, as an alternative.
- `DATAPULSE_LLM` (*secret*): `claude` or `groq` to choose by hand; empty means Claude if its key exists, otherwise Groq.
- `CLAUDE_MODEL` (default `claude-sonnet-5-5`) and `DATAPULSE_CLAUDE_EFFORT` (default `medium`) (*secret*).
- `OPENHANDS_API_KEY` (*secret*): turns on OpenHands mode; empty means local mode.
- `GITHUB_TOKEN`, `GITHUB_USERNAME`, `GITHUB_REPO` (*secret*): where results are published; without them, results stay in `outputs/`.
- `DATAPULSE_PERMITIR_SUBIDA_DATOS`: `1` allows data files to be uploaded to GitHub, only if the repository is private.
- `DATAPULSE_MIN_SCORE_PASS` (default `70`): minimum score to commit. Read on import.
- `DATAPULSE_ENMASCARAR` (default `1`): `0` turns off masking of personal data.
- `DATAPULSE_FECHAS_CENTINELA`: dates that mean "no data" (for example `1900-01-01,9999-12-31`); they are flagged, never imputed.
- `DATAPULSE_API_MAX_MB` (default `100`): maximum file size in the REST API.

### What it produces

- `processed_data.csv`: the clean dataset. Left out of the commit if it is larger than 50 MB.
- `datapulse_pipeline.py`: the script that did the cleaning; it takes the dataset path as an argument.
- `cleaning_log.json`: every action applied, with its justification.
- `quality_report.json`: the score the script gave itself (informative only).
- `audit_log.json`: scores, every planned action with its source (graph or LLM fallback), problems without a rule, rules skipped by output mode, decisions on negatives and warnings.
- `informe_final.md` and `.json`: the human-readable report. Saved locally only, in `outputs/<date>-informe/`.

On GitHub, results go to a `feature/datapulse-<date>` branch; in OpenHands mode, the dataset and what the agent wrote also stay in a `data/datapulse-<date>` branch. Without GitHub, results go to `outputs/<date>/`.

### How to read the final report

- **Score:** quality of the original → quality of the result, measured by DataPulse. "Declared by the script" is what the script said about itself and is often higher than the real value.
- **Values that could not be cleaned:** what is still dirty in the clean file (nulls, extreme values, broken relationships between columns), with examples.
- **Problems without a rule:** the graph has no rule and the LLM did not propose an action. This is the list of rules worth adding to your graph.
- **Skipped by output mode:** rules that exist but were not applied because of the output mode (for example, one-hot encoding in BI mode).
- **Columns not in the score:** new columns created by the cleaning (flags) and original columns that were removed.
- **Decisions on negatives:** valid, invalid or uncertain per column, with the reason and who decided (LLM or you).
- **Diagnosis warnings:** incomplete or contradictory LLM answers; the raw answer is saved.
- **Discrepancies:** columns where the decision on negatives does not match DataPulse's internal checklist. Informative only.

### Output mode: BI or ML

- **BI (default):** for reports, dashboards or Excel. The result keeps the original columns, readable and in their real units. Rules that encode (one-hot, ordinal), scale, transform the distribution (Box-Cox, Yeo-Johnson, quantiles), reduce dimensions (PCA) or cap real values (winsorize) are skipped; extreme values are flagged instead of capped.
- **ML:** for training models. Those rules are applied and the table is turned into numeric features.

Change it with `dp.MODO_SALIDA = "ml"`. A rule without the `modo` attribute applies in both modes.

### Negative values: decided by context

For each column with negative values, the LLM decides:

- `validos`: they make sense (temperatures, balances, coordinates). They are not touched.
- `invalidos`: they do not (ages, prices). They are flagged and replaced with a null; never clipped to 0, which would make up a value.
- `incierto`: there is not enough context. They are flagged without being changed.

You can add context with `CONTEXTO_DATASET` or decide yourself with `DECISIONES_NEGATIVOS`.

### How to edit your graph

**Where the rules are.** Only in `datapulse_prod.py`: `construir_grafo_conocimiento()` holds the base rules, valid for any business, and each domain has its own function `agregar_reglas_<domain>(kg)`, registered in `DOMINIOS`. `src/knowledge_graph.py` only re-exports them: do not add rules there.

**Rule format.** Each rule is an edge from a variable-type node to a problem node:

```python
kg.add_edge("Variable_Numerica_Continua", "Outlier_Severo",
    accion="flag_outlier_no_remove",      # explicit name: says what to do
    modo="bi",                             # optional: "bi" | "ml"; no modo = both
    confianza=0.85,                        # 0–1; the highest wins for the same type and problem
    parametros={"strategy": "flag_no_remove", "flag_suffix": "_outlier"},
    justificacion_es="Para BI el valor real se conserva: se marca, no se recorta.",
    justificacion_en="For BI the real value is kept: it is flagged, not capped.")
```

- **Name actions explicitly and put every value in `parametros`.** No table maps action names to functions: the executor (OpenHands or the local LLM) reads the action and its parameters as text and writes the code from them. Use `reemplazar_centinela_por_nan`, not `limpiar`.
- **New nodes need no aliases.** The labels the LLM can use are generated from the graph itself.
- **Structural problems** that do not depend on the column type (duplicates, empty rows…) hang from the `Esquema_General` node.

**Create and activate a domain.**

1. Copy `agregar_reglas_plantilla(kg)` as `agregar_reglas_<domain>(kg)` in `datapulse_prod.py` (`agregar_reglas_retail` is a full example).
2. Declare its new nodes with `_agregar_nodos_dominio(kg, problems, types)` and add its rules (they can also hang from existing nodes such as `Variable_Numerica_Continua`).
3. Register it: `DOMINIOS["<domain>"] = agregar_reglas_<domain>`.
4. Activate it: `DOMINIOS_ACTIVOS = ["<domain>"]` (in Colab, `dp.DOMINIOS_ACTIVOS = ["<domain>"]` before `run_datapulse`).

**Automatic validation.** `validar_grafo` runs when the graph is built and when a run starts. If a rule is malformed, it stops with a message naming the edge and the attribute: `accion` not empty, `confianza` between 0 and 1, `parametros` as a dict, both justifications present, `modo` equal to `"bi"` or `"ml"` if present, and no node without edges. The tests do not depend on how many rules there are, so editing the graph does not break them.

**Try a rule in Colab without calling any LLM.** Set `DOMINIOS_ACTIVOS` first (changing it rebuilds the graph and discards live edits):

```python
import datapulse_prod as dp
dp.kg.add_edge("Variable_Categorica_Nominal", "Codigo_Region_Inconsistente",
    accion="str_upper_y_strip", confianza=0.9, parametros={"strip": True},
    justificacion_es="Códigos de región en mayúsculas y sin espacios.",
    justificacion_en="Region codes in upper case and without spaces.")
dp.validar_grafo(dp.kg)
dp.node_consultar_grafo({"diagnostico": {"region": {
    "tipo_inferido": "categorica_nominal", "problemas": ["codigo_region_inconsistente"]}}})
```

**Example: the `retail` demo domain.** It is registered but inactive (`DOMINIOS_ACTIVOS = ["retail"]` turns it on). Its business values are **example values, not final decisions**, passed to each rule as explicit parameters:

- `mapear_etiquetas_satisfaccion_a_numero`: `mapeo = {"excelente": 5, "muy bueno": 4}`.
- `reemplazar_categoria_ambigua`: `reemplazos = {"TARJETA": "Tarjeta sin especificar"}`.
- `convertir_numeros_en_palabras`: `palabras = {"tres": 3, "veinte": 20, "treinta": 30}` (extendable).
- `recalcular_total_y_marcar`: total = `Cantidad_Articulos` × `Precio_Unitario`, flagging with `_recalculado` only the rows that change.

To change them permanently, edit the "VALORES DE EJEMPLO" block at the top of `agregar_reglas_retail(kg)`; each value is copied to every edge that uses it. To change them live in Colab:

```python
dp.DOMINIOS_ACTIVOS = ["retail"]
dp.aplicar_dominios_activos()
for _, _, datos in dp.kg.edges(data=True):
    if datos["accion"] == "convertir_numeros_en_palabras":
        datos["parametros"]["palabras"]["cuarenta"] = 40
dp.validar_grafo(dp.kg)
```

**When there is no rule.** If the graph has no rule for a type-and-problem pair, the planner (LLM) proposes a fallback action with confidence ≤ 0.70, labelled `llm_fallback`; whatever it does not resolve appears under "Problems without a rule". If rules exist but all belong to the other output mode, the problem is recorded as `omitido_por_modo` and is not sent to the fallback.

### Validation and retries

- **Commit condition:** the independent score must reach `DATAPULSE_MIN_SCORE_PASS` (70 by default) **and** must not be worse than the original's.
- **What is measured:** only the original columns. New columns (flags) and removed columns are listed in the report.
- **Attempt limits** (totals, first one included):
  - infrastructure failure (`error`, `timeout`, `stuck`, `stopped`) or a failing script: 2 attempts;
  - score too low, or `processed_data.csv` should exist and does not: 3 attempts;
  - results that cannot be read: 1 attempt;
  - the validator itself fails (`error_validador`): 1 attempt, no commit;
  - rejected by configuration or policy (`rechazado`): 1 attempt, no sandbox.
- Each retry receives the previous errors and a summary of what is still dirty.

### Privacy and security

- **Keys:** only in Colab secrets or environment variables; never in the code, the repository or the prompts. The generated script does not receive them.
- **Data on GitHub:** data files are uploaded only if `DATAPULSE_PERMITIR_SUBIDA_DATOS=1` **and** the repository is private. Otherwise they stay in `outputs/`, and OpenHands mode does not start.
- **Masking:** emails become `u***@domain`, sequences of 6 or more digits become `9`s of the same length, and person-name columns become a shape pattern (`José Muñoz` → `Aaaa Aaaaa`). Columns that read as numbers or dates are not masked, so the LLM can see real problems, except personal identifiers by column name (`cedula`, `telefono`, `cuenta`…).
- **Generated code:** in local mode it runs with your permissions (minimal environment, no keys); in OpenHands mode it runs in OpenHands' isolated environment.

### Running outside Colab

1. Install the dependencies: `pip install -r requirements.txt` (add `-r requirements-optional.txt` for scipy/scikit-learn, legacy `.xls` files or charts).
2. Set the environment variables (see [Configuration](#configuration)), for example `export ANTHROPIC_API_KEY="..."`.
3. Run it:
   - from Python: `import datapulse_prod as dp; dp.run_datapulse("path/to/file.xlsx")`;
   - or from the command line: `python datapulse_prod.py` uses the first data file it finds in `.`, `data/` or `datasets/`, or creates a small test dataset (`ventas_prueba.csv`).

### REST API (optional)

`uvicorn api.main:app` starts an API that only uses the quality analyzer (no LLM):

- `GET /health`: checks that the service is up.
- `POST /analyze`: upload a file and get its quality report and score.
- `POST /compare`: upload two versions of a dataset and detect changes in their numeric columns (drift).

### Tests

`pytest -q` runs the whole suite without network access: the LLM, OpenHands and GitHub are simulated.

### Limitations

- Quality depends on the graph: the base graph covers generic problems. For your line of business, add rules or a domain.
- The LLM is not deterministic: two runs on the same file can diagnose or plan differently. The report records every decision.
- The graph treats each column separately: relationships between columns (for example total = quantity × price) need a domain rule (see `retail`).
- In BI mode, flagged extreme values are kept and the validator still counts them as residual problems.
- `eda_report.html` is not implemented.
- GitHub: files larger than 50 MB are not published.

### Technical decisions

- **Why LangGraph?** Retries with an error history need state that persists between steps; LangGraph provides it with a typed state and conditional edges.
- **Why a knowledge graph instead of RAG?** Cleaning decisions need explicit, traceable rules (type + problem → action + justification + confidence), not text-similarity search.
- **Why two executors?** Local mode is fast and simple; OpenHands runs isolated and corrects itself. The pipeline is the same in both, and you choose with a key.

### License

MIT

---

## Español

### ¿Qué es DataPulse?

DataPulse limpia bases de datos de forma automática. Le das un archivo sucio (CSV, TSV, Excel, JSON o Parquet) y te devuelve:

- el archivo limpio (`processed_data.csv`);
- el script de Python que hizo la limpieza (`datapulse_pipeline.py`);
- un informe que explica cada decisión y lo que no se pudo limpiar (`informe_final.md`).

Las reglas de limpieza no están escondidas dentro de un modelo de IA: viven en un **grafo de conocimiento** que puedes leer y editar para tu giro de negocio. La calidad de la limpieza depende de ese grafo; DataPulse garantiza que el proceso sea siempre el mismo, que cada decisión quede registrada y que el resultado se valide de forma independiente.

### Cómo funciona

Siete pasos, siempre en el mismo orden:

```
archivo ─▶ [1] Leer ─▶ [2] Diagnosticar ─▶ [3] Consultar el grafo ─▶ [4] Planificar
                          (LLM)               (reglas, sin LLM)        (LLM)
        ─▶ [5] Ejecutar ─▶ [6] Validar ─┬─ aprobado ──────▶ [7] Guardar ─▶ informe final
          (OpenHands o LLM local)       └─ no aprobado ───▶ vuelve a [5] (intentos limitados)
```

1. **Leer.** Carga el archivo y resume cada columna: tipo, nulos, valores extremos, negativos, formatos de fecha y algunos ejemplos. Los datos personales de los ejemplos se enmascaran antes de enviarlos al LLM.
2. **Diagnosticar (LLM).** El LLM (Claude o Groq) etiqueta cada columna con su tipo y sus problemas, eligiendo entre las etiquetas que le ofrece el grafo. Si una columna tiene negativos, decide si son válidos, inválidos o inciertos, y explica por qué.
3. **Consultar el grafo (sin LLM).** Para cada combinación de tipo y problema, el grafo elige la regla de mayor confianza. Este paso es determinista.
4. **Planificar (LLM).** El LLM ordena las acciones y redacta las instrucciones de limpieza. No puede cambiar las acciones del grafo. Si un problema no tiene regla, propone una acción de respaldo con confianza máxima de 0,70, marcada como `llm_fallback`.
5. **Ejecutar.** Se escribe y se ejecuta el script de limpieza, con OpenHands o con el LLM en local (ver la sección siguiente).
6. **Validar.** DataPulse mide la calidad del archivo limpio con su propio analizador (`src/quality_checker.py`); no se fía del score que se pone el script. Si el resultado no alcanza el umbral o es peor que el original, se repite el paso 5.
7. **Guardar.** Publica los resultados en una rama de GitHub o, si GitHub no está configurado, en la carpeta `outputs/`.

Al terminar, DataPulse escribe el informe final (`informe_final.md`). Lo escribe sin LLM, a partir de lo que midió.

### Dos formas de ejecutar: LLM local u OpenHands

Las dos formas usan el mismo grafo, el mismo plan y la misma validación, y producen los mismos archivos. Solo cambia quién escribe y ejecuta el script del paso 5. DataPulse elige el modo según exista o no `OPENHANDS_API_KEY`.

**Modo local (sin `OPENHANDS_API_KEY`)**

- El LLM (Claude o Groq) escribe el script y DataPulse lo ejecuta en tu máquina o en Colab.
- Necesitas: una clave de Anthropic (`ANTHROPIC_API_KEY`) o de Groq (`GROQ_API_KEY`).
- Es rápido: con un Excel de 515 filas, una ejecución completa tardó alrededor de un minuto.
- El script se ejecuta con tus permisos, aunque sin ninguna clave de API en su entorno. Úsalo con datos en los que confías y no montes carpetas con información sensible.
- Recomendado para empezar y para trabajar con tus propios datos.

**Modo OpenHands (con `OPENHANDS_API_KEY`)**

- El LLM redacta las instrucciones; [OpenHands](https://app.all-hands.dev) escribe el script con su propio modelo, lo ejecuta, corrige errores y hace commit de los resultados en una rama de un repositorio de GitHub. Todo ocurre en el entorno aislado de OpenHands, en la nube.
- Además de la clave del LLM, necesitas:
  - `OPENHANDS_API_KEY`;
  - un repositorio **privado** de GitHub con una rama `main` (o `master`) —por ejemplo, creado con un README— al que OpenHands tenga acceso;
  - `GITHUB_TOKEN` con permiso de lectura y escritura sobre ese repositorio, y `GITHUB_USERNAME` y `GITHUB_REPO` con su dueño y su nombre;
  - `DATAPULSE_PERMITIR_SUBIDA_DATOS=1`, porque el dataset se sube a ese repositorio. Si el repositorio es público, el modo OpenHands no arranca.
- Es más lento: varios minutos por intento, como máximo 10 minutos de arranque más 15 de ejecución.
- Recomendado si prefieres que el código corra aislado de tu máquina o si quieres la autocorrección del agente.

Llamadas al LLM por ejecución: el diagnóstico y el plan se hacen una sola vez (2 llamadas, más 1 si al diagnóstico le faltaron columnas). En modo local, cada intento suma 1 llamada porque el LLM escribe el script; en modo OpenHands, el script lo escribe OpenHands con su propio modelo.

### Inicio rápido en Google Colab

**1. Guarda tus claves como secretos de Colab** (icono de la llave en la barra lateral) y activa el acceso del cuaderno a cada una. Nunca pongas claves en el código ni en el repositorio.

- `ANTHROPIC_API_KEY` (o `GROQ_API_KEY` si usas Groq).
- Solo para el modo OpenHands: `OPENHANDS_API_KEY`, `GITHUB_TOKEN`, `GITHUB_USERNAME` y `GITHUB_REPO`.

DataPulse lee cada valor primero de los secretos de Colab y, si no existe, de las variables de entorno. Las claves se leen al importar `datapulse_prod`: si cambias un secreto, reinicia la sesión.

**2. Clona el repositorio e instala las dependencias.**

```python
%cd /content
!rm -rf datapulse
!git clone https://github.com/<tu_usuario>/datapulse.git
!git -C datapulse log --oneline -1
!pip install -q -r datapulse/requirements.txt
```

pip puede avisar de que `google-colab` espera otras versiones de pandas o requests; es normal. Después, **reinicia la sesión** (Entorno de ejecución → Reiniciar sesión) para que se carguen las versiones nuevas y no quede código viejo en memoria.

**3. Ejecuta en modo local.**

```python
%cd /content/datapulse
import datapulse_prod as dp      # muestra la configuración detectada, nunca las claves

dp.OPENHANDS_API_KEY = ""        # fuerza el modo local aunque tengas clave de OpenHands
dp.CONTEXTO_DATASET = "Ventas de una tienda: edad del cliente, cantidad, precio unitario y total."

estado = dp.run_datapulse("data/ventas_prueba.csv")   # o la ruta de tu archivo
```

Para usar tu propio archivo, súbelo con el panel de archivos (o con `google.colab.files.upload()`) y pasa su ruta.

**4. Revisa el resultado.**

```python
print(open(estado["informe_final"]["ruta"] + "/informe_final.md", encoding="utf-8").read())

from google.colab import files
files.download(estado["ruta_processed"])   # descarga el archivo limpio
```

**5. (Opcional) Ejecuta en modo OpenHands.** Cumple primero los requisitos de la sección anterior. Si no guardaste `GITHUB_USERNAME` y `GITHUB_REPO` como secretos, defínelos como variables de entorno **antes de importar** (se leen al importar), en una sesión recién reiniciada:

```python
import os
os.environ["GITHUB_USERNAME"] = "<tu_usuario>"
os.environ["GITHUB_REPO"] = "<tu_repo_privado>"        # nombre exacto: guion medio ≠ guion bajo
os.environ["DATAPULSE_PERMITIR_SUBIDA_DATOS"] = "1"

%cd /content/datapulse
import datapulse_prod as dp
repo = dp._get_github_repo()                            # comprobación previa, sin costo
print(repo.full_name, "| privado:", repo.private, "|", dp._subida_datos_permitida(repo))

estado = dp.run_datapulse("data/ventas_prueba.csv")
```

**Problemas frecuentes**

- *Código viejo en memoria o un secreto cambiado que no se aplica:* reinicia la sesión y vuelve a correr las celdas.
- *Error 404 en `_get_github_repo()`:* el token no tiene acceso al repositorio, o el nombre o el dueño no coinciden.
- *Estado `rechazado` en modo OpenHands:* el repositorio es público o `DATAPULSE_PERMITIR_SUBIDA_DATOS` no vale `1`.

### Configuración

Opciones del bloque de configuración de `datapulse_prod.py`. En Colab se cambian después de importar (`dp.OPCION = ...`) y antes de `run_datapulse`:

- `CONTEXTO_DATASET` (por defecto `""`): describe el dataset en texto libre para ayudar al diagnóstico. Ejemplo: `"Ventas minoristas con devoluciones; una fila por compra"`. Si está vacío se usa la variable de entorno `DATAPULSE_CONTEXTO`.
- `DECISIONES_NEGATIVOS` (por defecto `{}`): tu decisión sobre los negativos de una columna, que reemplaza la del LLM. Ejemplo: `{"Temperatura": "validos", "Edad": "invalidos"}`.
- `MODO_SALIDA` (por defecto `"bi"`): `"bi"` o `"ml"` (ver [Modo de salida](#modo-de-salida-bi-o-ml)).
- `DOMINIOS_ACTIVOS` (por defecto `[]`): conjuntos de reglas especializadas que se añaden al grafo: `"salud"`, `"finanzas"`, `"economia"`, `"ocupacional"`, la demo `"retail"` o uno tuyo.

Variables de entorno (las marcadas como *secreto* también pueden ser secretos de Colab, que tienen prioridad):

- `ANTHROPIC_API_KEY` (*secreto*): Claude. Si existe, Claude es el LLM por defecto.
- `GROQ_API_KEY` y `GROQ_MODEL` (*secreto*; modelo por defecto `openai/gpt-oss-120b`): Groq, como alternativa.
- `DATAPULSE_LLM` (*secreto*): `claude` o `groq` para elegir a mano; vacía significa Claude si existe su clave y, si no, Groq.
- `CLAUDE_MODEL` (por defecto `claude-sonnet-5-5`) y `DATAPULSE_CLAUDE_EFFORT` (por defecto `medium`) (*secreto*).
- `OPENHANDS_API_KEY` (*secreto*): activa el modo OpenHands; vacía significa modo local.
- `GITHUB_TOKEN`, `GITHUB_USERNAME`, `GITHUB_REPO` (*secreto*): dónde se publican los resultados; sin ellas, los resultados quedan en `outputs/`.
- `DATAPULSE_PERMITIR_SUBIDA_DATOS`: `1` permite subir archivos de datos a GitHub, solo si el repositorio es privado.
- `DATAPULSE_MIN_SCORE_PASS` (por defecto `70`): score mínimo para hacer commit. Se lee al importar.
- `DATAPULSE_ENMASCARAR` (por defecto `1`): `0` desactiva el enmascarado de datos personales.
- `DATAPULSE_FECHAS_CENTINELA`: fechas que significan «sin dato» (por ejemplo `1900-01-01,9999-12-31`); se marcan, nunca se imputan.
- `DATAPULSE_API_MAX_MB` (por defecto `100`): tamaño máximo por archivo en la API REST.

### Qué produce

- `processed_data.csv`: el dataset limpio. Se omite del commit si pesa más de 50 MB.
- `datapulse_pipeline.py`: el script que hizo la limpieza; recibe la ruta del dataset como argumento.
- `cleaning_log.json`: cada acción aplicada, con su justificación.
- `quality_report.json`: el score que se puso el propio script (solo informativo).
- `audit_log.json`: scores, cada acción del plan con su origen (grafo o respaldo del LLM), problemas sin regla, reglas omitidas por el modo de salida, decisiones sobre negativos y avisos.
- `informe_final.md` y `.json`: el informe para personas. Se guarda solo en local, en `outputs/<fecha>-informe/`.

En GitHub, los resultados van a una rama `feature/datapulse-<fecha>`; en modo OpenHands, el dataset y lo que escribió el agente quedan además en una rama `data/datapulse-<fecha>`. Sin GitHub, los resultados van a `outputs/<fecha>/`.

### Cómo leer el informe final

- **Score:** calidad del original → calidad del resultado, medida por DataPulse. «Declarado por el script» es lo que el script dijo de sí mismo y suele ser más alto que el real.
- **Valores que no se pudieron limpiar:** lo que sigue sucio en el archivo limpio (nulos, valores extremos, relaciones rotas entre columnas), con ejemplos.
- **Problemas sin regla:** el grafo no tiene regla y el LLM no propuso una acción. Es la lista de reglas que conviene añadir a tu grafo.
- **Omitidos por modo de salida:** reglas que existen pero no se aplicaron por el modo (por ejemplo, one-hot en modo BI).
- **Columnas que no entran en el score:** columnas nuevas que creó la limpieza (flags) y columnas originales eliminadas.
- **Decisiones sobre negativos:** válidos, inválidos o inciertos por columna, con la razón y quién decidió (el LLM o tú).
- **Avisos del diagnóstico:** respuestas incompletas o contradictorias del LLM; se guarda su respuesta original.
- **Discrepancias:** columnas donde la decisión sobre negativos no coincide con la lista de control interna de DataPulse. Son solo informativas.

### Modo de salida: BI o ML

- **BI (por defecto):** para reportes, dashboards o Excel. El resultado conserva las columnas originales, legibles y en sus unidades reales. Se omiten las reglas que codifican (one-hot, ordinal), escalan, transforman la distribución (Box-Cox, Yeo-Johnson, cuantiles), reducen dimensiones (PCA) o recortan valores reales (winsorize); los valores extremos se marcan con un flag en vez de recortarse.
- **ML:** para entrenar modelos. Se aplican esas reglas y la tabla se convierte en variables numéricas.

Se cambia con `dp.MODO_SALIDA = "ml"`. Una regla sin el atributo `modo` se aplica en los dos modos.

### Negativos: decisión por contexto

Para cada columna con valores negativos, el LLM decide:

- `validos`: tienen sentido (temperaturas, saldos, coordenadas). No se tocan.
- `invalidos`: no lo tienen (edades, precios). Se marcan y se reemplazan por nulo; nunca se recortan a 0, porque eso inventaría un valor.
- `incierto`: no hay contexto suficiente. Se marcan sin modificarlos.

Puedes dar contexto con `CONTEXTO_DATASET` o decidir tú con `DECISIONES_NEGATIVOS`.

### Cómo editar tu grafo

**Dónde están las reglas.** Solo en `datapulse_prod.py`: `construir_grafo_conocimiento()` tiene las reglas base, válidas para cualquier negocio, y cada dominio tiene su función `agregar_reglas_<dominio>(kg)`, registrada en `DOMINIOS`. `src/knowledge_graph.py` solo las re-exporta: no agregues reglas ahí.

**Formato de una regla.** Cada regla es una arista que va de un nodo de tipo de variable a un nodo de problema:

```python
kg.add_edge("Variable_Numerica_Continua", "Outlier_Severo",
    accion="flag_outlier_no_remove",      # nombre explícito: dice qué hacer
    modo="bi",                             # opcional: "bi" | "ml"; sin modo = ambos
    confianza=0.85,                        # 0–1; gana la mayor para el mismo tipo y problema
    parametros={"strategy": "flag_no_remove", "flag_suffix": "_outlier"},
    justificacion_es="Para BI el valor real se conserva: se marca, no se recorta.",
    justificacion_en="For BI the real value is kept: it is flagged, not capped.")
```

- **Nombra las acciones de forma explícita y pon todos los valores en `parametros`.** No hay una tabla que traduzca nombres de acción a funciones: el ejecutor (OpenHands o el LLM local) lee la acción y sus parámetros como texto y escribe el código a partir de ellos. Usa `reemplazar_centinela_por_nan`, no `limpiar`.
- **Los nodos nuevos no necesitan alias.** Las etiquetas que puede usar el LLM se generan desde el propio grafo.
- **Los problemas estructurales** que no dependen del tipo de columna (duplicados, filas vacías…) cuelgan del nodo `Esquema_General`.

**Crear y activar un dominio.**

1. Copia `agregar_reglas_plantilla(kg)` como `agregar_reglas_<dominio>(kg)` en `datapulse_prod.py` (`agregar_reglas_retail` es un ejemplo completo).
2. Declara sus nodos nuevos con `_agregar_nodos_dominio(kg, problemas, tipos)` y añade sus reglas (también pueden colgar de nodos existentes, como `Variable_Numerica_Continua`).
3. Regístralo: `DOMINIOS["<dominio>"] = agregar_reglas_<dominio>`.
4. Actívalo: `DOMINIOS_ACTIVOS = ["<dominio>"]` (en Colab, `dp.DOMINIOS_ACTIVOS = ["<dominio>"]` antes de `run_datapulse`).

**Validación automática.** `validar_grafo` se ejecuta al construir el grafo y al empezar cada ejecución. Si una regla está mal formada, se detiene con un mensaje que dice qué arista y qué atributo fallan: `accion` no vacía, `confianza` entre 0 y 1, `parametros` como diccionario, las dos justificaciones, `modo` igual a `"bi"` o `"ml"` si existe, y ningún nodo sin aristas. Los tests no dependen de cuántas reglas haya, así que editar el grafo no los rompe.

**Probar una regla en Colab sin llamar a ningún LLM.** Fija primero `DOMINIOS_ACTIVOS` (cambiarlo reconstruye el grafo y descarta las ediciones en caliente):

```python
import datapulse_prod as dp
dp.kg.add_edge("Variable_Categorica_Nominal", "Codigo_Region_Inconsistente",
    accion="str_upper_y_strip", confianza=0.9, parametros={"strip": True},
    justificacion_es="Códigos de región en mayúsculas y sin espacios.",
    justificacion_en="Region codes in upper case and without spaces.")
dp.validar_grafo(dp.kg)
dp.node_consultar_grafo({"diagnostico": {"region": {
    "tipo_inferido": "categorica_nominal", "problemas": ["codigo_region_inconsistente"]}}})
```

**Ejemplo: el dominio de demostración `retail`.** Está registrado pero inactivo (`DOMINIOS_ACTIVOS = ["retail"]` lo activa). Sus valores de negocio son **valores de ejemplo, no decisiones definitivas**, y cada regla los recibe como parámetros explícitos:

- `mapear_etiquetas_satisfaccion_a_numero`: `mapeo = {"excelente": 5, "muy bueno": 4}`.
- `reemplazar_categoria_ambigua`: `reemplazos = {"TARJETA": "Tarjeta sin especificar"}`.
- `convertir_numeros_en_palabras`: `palabras = {"tres": 3, "veinte": 20, "treinta": 30}` (ampliable).
- `recalcular_total_y_marcar`: total = `Cantidad_Articulos` × `Precio_Unitario`, marcando con `_recalculado` solo las filas que cambian.

Para cambiarlos de forma permanente, edita el bloque «VALORES DE EJEMPLO» al inicio de `agregar_reglas_retail(kg)`; cada valor se copia a todas las aristas que lo usan. Para cambiarlos en caliente en Colab:

```python
dp.DOMINIOS_ACTIVOS = ["retail"]
dp.aplicar_dominios_activos()
for _, _, datos in dp.kg.edges(data=True):
    if datos["accion"] == "convertir_numeros_en_palabras":
        datos["parametros"]["palabras"]["cuarenta"] = 40
dp.validar_grafo(dp.kg)
```

**Qué pasa si no hay regla.** Si el grafo no tiene regla para un par de tipo y problema, el planificador (LLM) propone una acción de respaldo con confianza ≤ 0,70, marcada como `llm_fallback`; lo que no resuelva aparece en «Problemas sin regla». Si hay reglas pero todas son del otro modo de salida, el problema queda como `omitido_por_modo` y no pasa al respaldo del LLM.

### Validación y reintentos

- **Condición para el commit:** el score independiente debe alcanzar `DATAPULSE_MIN_SCORE_PASS` (70 por defecto) **y** no ser peor que el del original.
- **Qué se mide:** solo las columnas originales. Las columnas nuevas (flags) y las eliminadas se listan en el informe.
- **Límite de intentos** (totales, incluido el primero):
  - fallo de infraestructura (`error`, `timeout`, `stuck`, `stopped`) o script que falla: 2 intentos;
  - score bajo, o `processed_data.csv` debía existir y no apareció: 3 intentos;
  - resultados que no se pueden leer: 1 intento;
  - falla el propio validador (`error_validador`): 1 intento, sin commit;
  - rechazo por configuración o política (`rechazado`): 1 intento, sin entorno de OpenHands.
- Cada reintento recibe los errores anteriores y un resumen de lo que sigue sucio.

### Privacidad y seguridad

- **Claves:** solo en secretos de Colab o variables de entorno; nunca en el código, en el repositorio ni en los prompts. El script generado no las recibe.
- **Datos en GitHub:** los archivos de datos se suben solo si `DATAPULSE_PERMITIR_SUBIDA_DATOS=1` **y** el repositorio es privado. Si no, quedan en `outputs/` y el modo OpenHands no arranca.
- **Enmascarado:** los emails pasan a `u***@dominio`, las secuencias de 6 o más dígitos a `9` de la misma longitud y las columnas de nombres de persona a un patrón de forma (`José Muñoz` → `Aaaa Aaaaa`). Las columnas que se leen como número o fecha no se enmascaran, para que el LLM vea los problemas reales, salvo los identificadores personales por nombre de columna (`cedula`, `telefono`, `cuenta`…).
- **Código generado:** en modo local se ejecuta con tus permisos (entorno mínimo, sin claves); en modo OpenHands se ejecuta en el entorno aislado de OpenHands.

### Ejecutar fuera de Colab

1. Instala las dependencias: `pip install -r requirements.txt` (añade `-r requirements-optional.txt` para scipy/scikit-learn, archivos `.xls` antiguos o gráficos).
2. Define las variables de entorno (ver [Configuración](#configuración)), por ejemplo `export ANTHROPIC_API_KEY="..."`.
3. Ejecuta:
   - desde Python: `import datapulse_prod as dp; dp.run_datapulse("ruta/al/archivo.xlsx")`;
   - o desde la terminal: `python datapulse_prod.py` usa el primer archivo de datos que encuentra en `.`, `data/` o `datasets/`, o crea un dataset de prueba pequeño (`ventas_prueba.csv`).

### API REST (opcional)

`uvicorn api.main:app` levanta una API que solo usa el analizador de calidad (sin LLM):

- `GET /health`: comprueba que el servicio está activo.
- `POST /analyze`: subes un archivo y recibes su reporte de calidad y su score.
- `POST /compare`: subes dos versiones de un dataset y detecta cambios en sus columnas numéricas (drift).

### Tests

`pytest -q` ejecuta todos los tests sin conexión: el LLM, OpenHands y GitHub están simulados.

### Limitaciones

- La calidad depende del grafo: el grafo base cubre problemas genéricos. Para tu giro de negocio, añade reglas o un dominio.
- El LLM no es determinista: dos ejecuciones sobre el mismo archivo pueden diagnosticar o planificar distinto. El informe registra cada decisión.
- El grafo trata cada columna por separado: las relaciones entre columnas (por ejemplo, total = cantidad × precio) necesitan una regla de dominio (ver `retail`).
- En modo BI, los valores extremos marcados se conservan y el validador los sigue contando como problemas residuales.
- `eda_report.html` no está implementado.
- GitHub: los archivos de más de 50 MB no se publican.

### Decisiones técnicas

- **¿Por qué LangGraph?** Los reintentos con historial de errores necesitan un estado que persista entre pasos; LangGraph lo ofrece con un estado tipado y transiciones condicionales.
- **¿Por qué un grafo de conocimiento y no RAG?** Las decisiones de limpieza necesitan reglas explícitas y trazables (tipo + problema → acción + justificación + confianza), no búsqueda por similitud de texto.
- **¿Por qué dos ejecutores?** El modo local es rápido y sencillo; OpenHands se ejecuta aislado y se autocorrige. El pipeline es el mismo en los dos y eliges con una clave.

### Licencia

MIT
