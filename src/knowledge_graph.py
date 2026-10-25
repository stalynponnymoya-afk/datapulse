"""
DataPulse — Knowledge Graph (módulo de acceso)
==============================================

Expone el Grafo de Conocimiento y sus mapeos canónicos definidos en
``datapulse_prod.py`` para que puedan consumirse desde el paquete ``src``.

La implementación canónica vive en ``datapulse_prod.py`` (script exportado de
Colab). Este módulo la re-exporta en lugar de duplicarla, de modo que ambas
rutas permanezcan sincronizadas por construcción: no hay dos copias de las
reglas estadísticas ni de las reglas de dominio que puedan divergir.

Contenido expuesto:
- ``construir_grafo_conocimiento``  — ~106 reglas estadísticas base
- ``agregar_reglas_dominio``        — +24 reglas deterministas de dominio
- ``KNOWLEDGE_GRAPH`` / ``kg``      — grafo completo ya construido
- ``TIPO_MAP`` / ``PROBLEMA_MAP``   — mapeos tipo/problema → nodos del grafo
- ``TIPO_MAP_DOMINIO`` / ``PROBLEMA_MAP_DOMINIO``
- ``ESQUEMA_GENERAL_TARGETS``       — problemas estructurales sin tipo asociado
- ``_map_tipo_problema``            — resolución (tipo, problema) → (src, tgt)
"""

import os
import sys

# El módulo canónico vive en la raíz del repositorio (padre de src/).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from datapulse_prod import (  # noqa: E402
    KNOWLEDGE_GRAPH,
    PROBLEMA_MAP,
    PROBLEMA_MAP_DOMINIO,
    TIPO_MAP,
    TIPO_MAP_DOMINIO,
    ESQUEMA_GENERAL_TARGETS,
    _map_tipo_problema,
    agregar_reglas_dominio,
    construir_grafo_conocimiento,
    kg,
)

__all__ = [
    "KNOWLEDGE_GRAPH",
    "kg",
    "TIPO_MAP",
    "PROBLEMA_MAP",
    "TIPO_MAP_DOMINIO",
    "PROBLEMA_MAP_DOMINIO",
    "ESQUEMA_GENERAL_TARGETS",
    "construir_grafo_conocimiento",
    "agregar_reglas_dominio",
    "_map_tipo_problema",
]
