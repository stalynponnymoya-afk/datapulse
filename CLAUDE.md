# DataPulse — guía para trabajar en este repositorio

## Objetivo del proyecto

Que la arquitectura funcione para que **cualquier persona edite su propio grafo de
conocimiento**. La calidad de la limpieza depende del grafo configurado para cada giro de
negocio; un dataset de prueba (por ejemplo `base_datos_tienda.xlsx`) mide que la arquitectura
funcione, no que limpie perfecto.

## Reglas de arquitectura (obligatorias)

Si una tarea exige romper alguna, **detente y pregunta** antes de escribir código.

1. Los 7 nodos y su orden no cambian: ingestar → diagnosticar → consultar_grafo → planificar →
   ejecutar → validar_score → router. El router solo usa las ramas `retry`, `commit` y
   `end_no_commit`.
2. El LLM trabaja en `diagnosticar` y `planificar`; el grafo NetworkX en `consultar_grafo`;
   OpenHands o el LLM local escriben y ejecutan el código en `ejecutar`.
3. `DataPulseState`: solo se añaden claves, nunca se renombran ni se quitan.
4. Las reglas del grafo viven solo en `datapulse_prod.py`, con el formato existente:
   `kg.add_edge(tipo, problema, accion=..., confianza=..., parametros={...},
   justificacion_es=..., justificacion_en=...)` (atributo opcional `modo="bi"|"ml"`).
5. `datapulse_prod.py` es la implementación canónica; `src/knowledge_graph.py` y
   `src/graph_nodes.py` solo re-exportan.
6. No cambian la firma de `run_datapulse` ni los nombres de los archivos de salida.

## Reglas de trabajo

- Commits solo en la rama de trabajo asignada, uno por punto pedido; nunca fusionar.
- Tests sin red: Groq, Claude, OpenHands y GitHub siempre simulados (`monkeypatch`).
- Comentarios y docstrings en español.
- No ejecutar nada con las claves del usuario.
- No subir datasets al repositorio (los de prueba se generan dentro de los tests).

## Opciones de configuración

Están en el bloque de configuración de `datapulse_prod.py`, junto a `GITHUB_DATA_FILE`. En Colab
se pueden cambiar después de importar (`dp.OPCION = ...`) y antes de llamar a `run_datapulse`.

| Opción | Por defecto | Uso |
|---|---|---|
| `CONTEXTO_DATASET` | `""` | Texto libre que describe el dataset; se añade al prompt de diagnóstico. Si está vacío se usa la variable de entorno `DATAPULSE_CONTEXTO`. |
| `DECISIONES_NEGATIVOS` | `{}` | `{"columna": "validos" \| "invalidos"}`: reemplaza la decisión del LLM sobre los negativos de esa columna (fuente `usuario`). |
| `MODO_SALIDA` | `"bi"` | `"bi"` (análisis/BI) o `"ml"` (modelos). `consultar_grafo` omite las reglas cuyo `modo` no coincide; las reglas sin `modo` se aplican en ambos. |
| `DOMINIOS_ACTIVOS` | `[]` | Dominios de reglas que se agregan al grafo (`salud`, `finanzas`, `economia`, `ocupacional`, o uno propio registrado). Solo sus tipos y problemas aparecen en el vocabulario del diagnóstico. |

Las claves (`ANTHROPIC_API_KEY`, `GROQ_API_KEY`, `OPENHANDS_API_KEY`, `GITHUB_TOKEN`) y
`GITHUB_USERNAME`/`GITHUB_REPO` se leen de los Secrets de Colab o de variables de entorno; nunca
van en el código.

## Comandos útiles

```bash
pip install -r requirements.txt
pytest -q --cov=src --cov=api --cov=datapulse_prod
python src/quality_checker.py data/<archivo>
```
