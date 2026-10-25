"""
DataPulse — Nodos del pipeline LangGraph (módulo de acceso)
===========================================================

Re-exporta los 7 nodos del orquestador definidos en ``datapulse_prod.py`` para
que el paquete ``src`` los consuma sin mantener una segunda copia. Al compartir
una única implementación, las correcciones de estado (retornos parciales),
el manejo del fallback local de Groq y la resolución del Grafo de Conocimiento
quedan garantizadas en ambos puntos de entrada.

Nodos expuestos:
- ``node_ingestar``       — carga el archivo y extrae metadata
- ``node_diagnosticar``   — Groq infiere tipos y detecta problemas
- ``node_consultar_grafo``— el Grafo de Conocimiento decide la acción
- ``node_planificar``     — Groq resuelve fallbacks y construye el brief
- ``node_ejecutar``       — OpenHands (o fallback local con Groq)
- ``node_validar_score``  — passthrough hacia el router condicional
- ``node_commit``         — publica en GitHub o en ``outputs/`` localmente
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
    node_ingestar,
    node_planificar,
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
    "node_commit",
    "router_validar_score",
    "build_datapulse_graph",
    "run_datapulse",
]
