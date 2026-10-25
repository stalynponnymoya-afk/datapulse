# DataPulse Source Package
# ==========================

"""
Este paquete contiene los módulos principales de DataPulse:

- quality_checker.py: Analizador de calidad de datos
- knowledge_graph.py: Grafo de Conocimiento (re-exporta la implementación canónica)
- graph_nodes.py: Nodos del pipeline LangGraph (re-exporta la implementación canónica)

El módulo se integra con datapulse_prod.py (implementación canónica del
orquestador) para proporcionar análisis de calidad y recomendaciones basadas
en el Grafo de Conocimiento.
"""

from .quality_checker import (
    load_dataset,
    generate_report,
    check_nulls,
    check_duplicates,
    check_outliers,
    check_negative_values,
    check_text_inconsistencies,
    check_invalid_dates,
    analyze_variable_types,
)

__all__ = [
    "load_dataset",
    "generate_report",
    "check_nulls",
    "check_duplicates",
    "check_outliers",
    "check_negative_values",
    "check_text_inconsistencies",
    "check_invalid_dates",
    "analyze_variable_types",
]

# El Grafo de Conocimiento y los nodos del pipeline se cargan de forma
# perezosa: importar sólo el analizador de calidad no debe arrastrar
# LangGraph ni las dependencias del orquestador.
_LAZY = {
    "KNOWLEDGE_GRAPH": ("src.knowledge_graph", "KNOWLEDGE_GRAPH"),
    "kg": ("src.knowledge_graph", "kg"),
    "TIPO_MAP": ("src.knowledge_graph", "TIPO_MAP"),
    "PROBLEMA_MAP": ("src.knowledge_graph", "PROBLEMA_MAP"),
    "TIPO_MAP_DOMINIO": ("src.knowledge_graph", "TIPO_MAP_DOMINIO"),
    "PROBLEMA_MAP_DOMINIO": ("src.knowledge_graph", "PROBLEMA_MAP_DOMINIO"),
    "ESQUEMA_GENERAL_TARGETS": ("src.knowledge_graph", "ESQUEMA_GENERAL_TARGETS"),
    "construir_grafo_conocimiento": ("src.knowledge_graph", "construir_grafo_conocimiento"),
    "agregar_reglas_dominio": ("src.knowledge_graph", "agregar_reglas_dominio"),
    "_map_tipo_problema": ("src.knowledge_graph", "_map_tipo_problema"),
    "DataPulseState": ("src.graph_nodes", "DataPulseState"),
    "node_ingestar": ("src.graph_nodes", "node_ingestar"),
    "node_diagnosticar": ("src.graph_nodes", "node_diagnosticar"),
    "node_consultar_grafo": ("src.graph_nodes", "node_consultar_grafo"),
    "node_planificar": ("src.graph_nodes", "node_planificar"),
    "node_ejecutar": ("src.graph_nodes", "node_ejecutar"),
    "node_validar_score": ("src.graph_nodes", "node_validar_score"),
    "node_commit": ("src.graph_nodes", "node_commit"),
    "router_validar_score": ("src.graph_nodes", "router_validar_score"),
    "build_datapulse_graph": ("src.graph_nodes", "build_datapulse_graph"),
    "run_datapulse": ("src.graph_nodes", "run_datapulse"),
}


def __getattr__(name):
    if name in _LAZY:
        import importlib

        module_name, attr = _LAZY[name]
        value = getattr(importlib.import_module(module_name), attr)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals().keys()) + list(_LAZY.keys()))


__version__ = "3.0.0"