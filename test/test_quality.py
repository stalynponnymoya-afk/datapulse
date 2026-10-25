"""
DataPulse Test Suite
====================

Unit tests for DataPulse quality checker, Knowledge Graph, and pipeline nodes.
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_quality_checker_imports():
    """Test that quality checker module imports correctly."""
    from src.quality_checker import (
        load_dataset,
        generate_report,
        check_nulls,
        check_duplicates,
    )
    assert load_dataset is not None
    assert generate_report is not None
    print("✅ All imports successful")


def test_knowledge_graph_construction():
    """Test that the full knowledge graph can be constructed."""
    from datapulse_prod import construir_grafo_conocimiento, agregar_reglas_dominio

    kg = construir_grafo_conocimiento()
    assert kg.number_of_nodes() > 0, "Graph should have nodes"
    assert kg.number_of_edges() > 0, "Graph should have edges"

    edges_before = kg.number_of_edges()
    kg = agregar_reglas_dominio(kg)
    assert kg.number_of_edges() > edges_before, "Domain rules should add edges"

    # Base (~58 estadísticas + estructurales) + 24 de dominio > 100 aristas
    assert kg.number_of_edges() > 100, (
        f"Total edges should exceed 100, got {kg.number_of_edges()}"
    )
    print(f"✅ Knowledge Graph: {kg.number_of_nodes()} nodes, {kg.number_of_edges()} edges")


def test_domain_rules_connected():
    """Domain rules must be reachable through the global TIPO_MAP/PROBLEMA_MAP."""
    from datapulse_prod import (
        TIPO_MAP,
        PROBLEMA_MAP,
        TIPO_MAP_DOMINIO,
        PROBLEMA_MAP_DOMINIO,
        _map_tipo_problema,
    )

    for alias, node in TIPO_MAP_DOMINIO.items():
        assert TIPO_MAP.get(alias) == node, f"TIPO_MAP missing domain alias {alias}"

    for alias, node in PROBLEMA_MAP_DOMINIO.items():
        assert PROBLEMA_MAP.get(alias) == node, f"PROBLEMA_MAP missing domain alias {alias}"

    src, tgt = _map_tipo_problema("signo_vital", "rango_clinico_invalido")
    assert (src, tgt) == ("Variable_Clinica_Signo_Vital", "Rango_Clinico_Invalido")
    print("✅ Domain rules connected through global maps")


def test_pipeline_functions():
    """Test that pipeline functions and nodes exist."""
    from datapulse_prod import (
        build_datapulse_graph,
        node_ingestar,
        node_diagnosticar,
        node_consultar_grafo,
        node_planificar,
        node_ejecutar,
        node_validar_score,
        node_commit,
        run_datapulse,
    )

    for fn in (
        node_ingestar,
        node_diagnosticar,
        node_consultar_grafo,
        node_planificar,
        node_ejecutar,
        node_validar_score,
        node_commit,
    ):
        assert callable(fn), f"{fn} should be callable"

    app = build_datapulse_graph()
    assert app is not None
    assert callable(run_datapulse)
    print("✅ All pipeline functions imported and graph compiled")


def test_type_maps():
    """Test that type and problem maps are defined."""
    from datapulse_prod import TIPO_MAP, PROBLEMA_MAP

    assert len(TIPO_MAP) > 0, "TIPO_MAP should not be empty"
    assert len(PROBLEMA_MAP) > 0, "PROBLEMA_MAP should not be empty"
    assert "nulos_bajos" in PROBLEMA_MAP, "Should have nulos_bajos mapped"
    print(f"✅ Type maps: {len(TIPO_MAP)} types, {len(PROBLEMA_MAP)} problems")


def test_clean_data_edges_activated():
    """
    "ninguno"/"none"/"clean"/"no_issues" must map to "Ninguno" (string) so the
    clean-data edges are reachable instead of falling back to the LLM.
    """
    from datapulse_prod import PROBLEMA_MAP, TIPO_MAP, node_consultar_grafo, kg

    for alias in ("ninguno", "none", "clean", "no_issues"):
        assert PROBLEMA_MAP[alias] == "Ninguno", (
            f"{alias!r} must map to the string 'Ninguno', got {PROBLEMA_MAP[alias]!r}"
        )

    # Every type that has a "Ninguno" edge must resolve cleanly.
    tipos_con_ninguno = [
        t for t, node in TIPO_MAP.items() if kg.has_edge(node, "Ninguno")
    ]
    assert tipos_con_ninguno, "Expected at least one type with a 'Ninguno' edge"

    diag = {
        f"col_{i}": {
            "tipo_inferido": t,
            "problemas": ["ninguno"],
            "problema_principal": "ninguno",
        }
        for i, t in enumerate(tipos_con_ninguno)
    }
    out = node_consultar_grafo({"diagnostico": diag})
    for col, res in out["acciones_grafo"].items():
        assert not res["requiere_llm_fallback"], (
            f"{col} ({res['tipo_inferido']}) fell back to LLM for clean data"
        )
        assert res["acciones"][0]["accion"], f"{col} produced no action"
    print(f"✅ Clean-data edges activated for {len(tipos_con_ninguno)} types")


def test_esquema_general_fallback():
    """
    Structural problems (duplicados, valores constantes, ...) must resolve
    through the "Esquema_General" source node even when no direct edge from the
    column's type exists.
    """
    from datapulse_prod import node_consultar_grafo, ESQUEMA_GENERAL_TARGETS, PROBLEMA_MAP

    problemas_estructurales = [
        p for p, tgt in PROBLEMA_MAP.items() if tgt in ESQUEMA_GENERAL_TARGETS
    ]
    assert problemas_estructurales, "Expected structural problems in PROBLEMA_MAP"

    diag = {
        f"col_{i}": {
            "tipo_inferido": "texto_libre",
            "problemas": [p],
            "problema_principal": p,
        }
        for i, p in enumerate(problemas_estructurales)
    }
    out = node_consultar_grafo({"diagnostico": diag})
    for col, res in out["acciones_grafo"].items():
        assert not res["requiere_llm_fallback"], (
            f"{col} fell back to LLM instead of using Esquema_General: "
            f"{res['problemas_fallback']}"
        )
    print(f"✅ Esquema_General fallback resolved {len(problemas_estructurales)} problems")


def test_all_map_combinations_resolve():
    """
    Every (TIPO_MAP x PROBLEMA_MAP) pair that has an edge in the graph must be
    resolved by node_consultar_grafo without falling back to the LLM.
    """
    from datapulse_prod import TIPO_MAP, PROBLEMA_MAP, node_consultar_grafo, kg

    combos = []
    for tipo_alias, src in TIPO_MAP.items():
        for prob_alias, tgt in PROBLEMA_MAP.items():
            if tgt is None:
                continue
            if kg.has_edge(src, tgt):
                combos.append((tipo_alias, prob_alias))

    assert combos, "Expected at least one resolvable (type, problem) combination"

    diag = {
        f"col_{i}": {
            "tipo_inferido": t,
            "problemas": [p],
            "problema_principal": p,
        }
        for i, (t, p) in enumerate(combos)
    }
    out = node_consultar_grafo({"diagnostico": diag})
    for col, res in out["acciones_grafo"].items():
        assert not res["requiere_llm_fallback"], (
            f"{col} fell back to LLM: {res['problemas_fallback']}"
        )
    print(f"✅ All {len(combos)} graph-backed (type, problem) combinations resolved")


if __name__ == "__main__":
    print("=" * 60)
    print("🧪 DataPulse Test Suite")
    print("=" * 60)

    try:
        test_quality_checker_imports()
        test_knowledge_graph_construction()
        test_domain_rules_connected()
        test_pipeline_functions()
        test_type_maps()
        test_clean_data_edges_activated()
        test_esquema_general_fallback()
        test_all_map_combinations_resolve()

        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED")
        print("=" * 60)
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
