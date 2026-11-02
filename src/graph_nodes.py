"""
DataPulse — Nodos del pipeline LangGraph (módulo de acceso)
===========================================================

Re-exporta los 9 nodos del orquestador definidos en ``datapulse_prod.py`` para
que el paquete ``src`` los consuma sin mantener una segunda copia. Al compartir
una única implementación, las correcciones de estado (retornos parciales),
el manejo del fallback local de Groq y la resolución del Grafo de Conocimiento
quedan garantizadas en ambos puntos de entrada.

Nodos expuestos:
- ``node_ingestar``       — carga el archivo y extrae metadata
- ``node_diagnosticar``   — el LLM diagnostica con etiquetas generadas del grafo
- ``node_consultar_grafo``— el Grafo de Conocimiento decide la acción
- ``node_planificar``     — plan = acciones del grafo; el LLM ordena y redacta el brief
- ``node_ejecutar``       — OpenHands (o modo local con el LLM configurado)
- ``node_validar_score``  — mide el processed_data.csv real (score independiente)
- ``node_revisar_resultado`` — el LLM revisa el resultado y pide correcciones
- ``node_commit``         — publica en GitHub o en ``outputs/`` localmente
- ``node_informe_final``  — tabla con el score y los valores sin limpiar
- ``build_datapulse_graph`` / ``run_datapulse``
"""

import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from datapulse_prod import (  # noqa: E402
    DataPulseState,
    build_datapulse_graph,
    node_commit,
    node_consultar_grafo,
    node_diagnosticar,
    node_ejecutar,
    node_informe_final,
    node_ingestar,
    node_planificar,
    node_revisar_resultado,
    node_validar_score,
    router_validar_score,
    run_datapulse,
)

__all__ = [
    "DataPulseState",
    "node_ingestar",
    "node_diagnosticar",
    "node_consultar_grafo",
    "node_planificar",
    "node_ejecutar",
    "node_validar_score",
    "node_revisar_resultado",
    "node_commit",
    "node_informe_final",
    "router_validar_score",
    "build_datapulse_graph",
    "run_datapulse",
]
