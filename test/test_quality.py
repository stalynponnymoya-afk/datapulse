"""
DataPulse Test Suite
====================

Unit tests for DataPulse quality checker and pipeline components.
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
    """Test that knowledge graph can be constructed."""
    from datapulse_pipeline import construir_grafo_conocimiento
    
    kg = construir_grafo_conocimiento()
    assert kg.number_of_nodes() > 0, "Graph should have nodes"
    assert kg.number_of_edges() > 0, "Graph should have edges"
    print(f"✅ Knowledge Graph: {kg.number_of_nodes()} nodes, {kg.number_of_edges()} edges")


def test_pipeline_functions():
    """Test that pipeline functions work correctly."""
    from datapulse_pipeline import (
        load_data,
        analyze_with_knowledge_graph,
        run_pipeline,
    )
    
    # Test that functions exist
    assert load_data is not None
    assert analyze_with_knowledge_graph is not None
    assert run_pipeline is not None
    print("✅ All pipeline functions imported")


def test_type_maps():
    """Test that type and problem maps are defined."""
    from datapulse_pipeline import TIPO_MAP, PROBLEMA_MAP
    
    assert len(TIPO_MAP) > 0, "TIPO_MAP should not be empty"
    assert len(PROBLEMA_MAP) > 0, "PROBLEMA_MAP should not be empty"
    assert "nulos_bajos" in PROBLEMA_MAP, "Should have nulos_bajos mapped"
    print(f"✅ Type maps: {len(TIPO_MAP)} types, {len(PROBLEMA_MAP)} problems")


if __name__ == "__main__":
    print("=" * 60)
    print("🧪 DataPulse Test Suite")
    print("=" * 60)
    
    try:
        test_quality_checker_imports()
        test_knowledge_graph_construction()
        test_pipeline_functions()
        test_type_maps()
        
        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED")
        print("=" * 60)
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)