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
- ``construir_grafo_conocimiento``  — reglas estadísticas y estructurales base
- ``agregar_reglas_dominio``        — agrega los dominios indicados (sin argumentos, todos)
- ``agregar_reglas_salud`` / ``_finanzas`` / ``_economia`` / ``_ocupacional`` / ``_retail`` — un dominio cada una (retail: demo con valores de ejemplo)
- ``agregar_reglas_plantilla``      — plantilla documentada para un dominio propio
- ``DOMINIOS``                      — registro nombre → función de dominio
- ``aplicar_dominios_activos``      — reconstruye el grafo global según DOMINIOS_ACTIVOS
- ``validar_grafo`` / ``errores_grafo`` / ``GrafoInvalidoError`` — validador del formato
- ``KNOWLEDGE_GRAPH`` / ``kg``      — grafo construido al importar (base + DOMINIOS_ACTIVOS,
  vacía por defecto). Si cambias ``datapulse_prod.DOMINIOS_ACTIVOS`` usa
  ``datapulse_prod.kg``: estos nombres apuntan al grafo de la importación.
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
    DOMINIOS,
    GrafoInvalidoError,
    KNOWLEDGE_GRAPH,
    PROBLEMA_MAP,
    PROBLEMA_MAP_DOMINIO,
    TIPO_MAP,
    TIPO_MAP_DOMINIO,
    ESQUEMA_GENERAL_TARGETS,
    _map_tipo_problema,
    agregar_reglas_dominio,
    agregar_reglas_economia,
    agregar_reglas_finanzas,
    agregar_reglas_ocupacional,
    agregar_reglas_plantilla,
    agregar_reglas_retail,
    agregar_reglas_salud,
    aplicar_dominios_activos,
    construir_grafo_conocimiento,
    errores_grafo,
    kg,
    validar_grafo,
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
    "agregar_reglas_salud",
    "agregar_reglas_finanzas",
    "agregar_reglas_economia",
    "agregar_reglas_ocupacional",
    "agregar_reglas_retail",
    "agregar_reglas_plantilla",
    "DOMINIOS",
    "aplicar_dominios_activos",
    "validar_grafo",
    "errores_grafo",
    "GrafoInvalidoError",
    "_map_tipo_problema",
]
