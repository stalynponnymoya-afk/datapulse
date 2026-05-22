# DataPulse Source Package
# ==========================

"""
Este paquete contiene los módulos principales de DataPulse:

- quality_checker.py: Analizador de calidad de datos
- (future) graph_nodes.py: Nodos del grafo de conocimiento
- (future) transformers.py: Transformaciones de datos

El módulo se integra con datapulse_pipeline.py para proporcionar
análisis de calidad y recomendaciones basadas en el Grafo de Conocimiento.
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

__version__ = "3.0.0"