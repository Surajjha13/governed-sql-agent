"""
Test suite for Visualization Recommendation Engine
"""
from app.services.visualization_service import VisualizationService


def test_kpi_card_recommendation():
    """Test KPI card for single numeric value"""
    results = {
        "columns": ["total_sales"],
        "rows": [{"total_sales": 125000}]
    }
    
    rec = VisualizationService.recommend_visualization(results)
    
    assert rec["recommended_chart"] == "kpi"
    assert rec["confidence"] == 100
    assert "Single numeric value" in rec["reason"]


def test_bar_chart_recommendation():
    """Test Bar chart for categorical + metric"""
    results = {
        "columns": ["category", "sales"],
        "rows": [
            {"category": "Electronics", "sales": 5000},
            {"category": "Clothing", "sales": 3000},
            {"category": "Food", "sales": 2000}
        ]
    }
    
    rec = VisualizationService.recommend_visualization(results)
    
    assert rec["recommended_chart"] == "bar"
    assert rec["confidence"] == 90
    assert "Categorical data" in rec["reason"]


def test_line_chart_recommendation():
    """Test Line chart for time-series data"""
    results = {
        "columns": ["date", "revenue"],
        "rows": [
            {"date": "2024-01-01", "revenue": 1000},
            {"date": "2024-01-02", "revenue": 1200},
            {"date": "2024-01-03", "revenue": 1100}
        ]
    }
    
    rec = VisualizationService.recommend_visualization(results)
    
    assert rec["recommended_chart"] == "line"
    assert rec["confidence"] == 95
    assert "Time-series" in rec["reason"]


def test_scatter_plot_recommendation():
    """Test Scatter plot for 2 numeric columns"""
    results = {
        "columns": ["price", "quantity"],
        "rows": [{"price": i * 10, "quantity": 100 - i} for i in range(20)]
    }
    
    rec = VisualizationService.recommend_visualization(results)
    
    assert rec["recommended_chart"] == "scatter"
    assert rec["confidence"] == 85
    assert "Two numeric variables" in rec["reason"]


def test_histogram_recommendation():
    """Test Histogram for single numeric distribution"""
    results = {
        "columns": ["age"],
        "rows": [{"age": i} for i in range(18, 65)]
    }
    
    rec = VisualizationService.recommend_visualization(results)
    
    assert rec["recommended_chart"] == "histogram"
    assert rec["confidence"] == 90
    assert "distribution" in rec["reason"]


def test_pie_chart_recommendation():
    """Test Pie chart for few categories with distribution keyword"""
    results = {
        "columns": ["region", "count"],
        "rows": [
            {"region": "North", "count": 100},
            {"region": "South", "count": 150},
            {"region": "East", "count": 120}
        ]
    }
    
    rec = VisualizationService.recommend_visualization(results, question="show distribution")
    
    assert rec["recommended_chart"] == "pie"
    assert rec["confidence"] == 80


def test_table_recommendation_fallback():
    """Test Table recommendation for complex data"""
    results = {
        "columns": ["id", "name", "description", "status"],
        "rows": [
            {"id": 1, "name": "Item1", "description": "Desc1", "status": "Active"},
            {"id": 2, "name": "Item2", "description": "Desc2", "status": "Pending"}
        ]
    }
    
    rec = VisualizationService.recommend_visualization(results)
    
    assert rec["recommended_chart"] == "table"
    assert rec["confidence"] == 100


def test_no_data_recommendation():
    """Test when no data is available"""
    results = {
        "columns": ["sales"],
        "rows": []
    }
    
    rec = VisualizationService.recommend_visualization(results)
    
    assert rec["recommended_chart"] == "none"
    assert rec["confidence"] == 0


def test_multi_metric_bar_recommendation():
    """Test multi-metric bar chart (stacked)"""
    results = {
        "columns": ["category", "q1_sales", "q2_sales"],
        "rows": [
            {"category": "A", "q1_sales": 100, "q2_sales": 120},
            {"category": "B", "q1_sales": 200, "q2_sales": 180}
        ]
    }
    
    rec = VisualizationService.recommend_visualization(results)
    
    assert rec["recommended_chart"] == "bar"
    assert "Multi-metric" in rec["reason"]


if __name__ == "__main__":
    print("Running Visualization Service Tests...")
    
    # Run each test
    tests = [
        ("KPI Card", test_kpi_card_recommendation),
        ("Bar Chart", test_bar_chart_recommendation),
        ("Line Chart", test_line_chart_recommendation),
        ("Scatter Plot", test_scatter_plot_recommendation),
        ("Histogram", test_histogram_recommendation),
        ("Pie Chart", test_pie_chart_recommendation),
        ("Table Fallback", test_table_recommendation_fallback),
        ("No Data", test_no_data_recommendation),
        ("Multi-metric Bar", test_multi_metric_bar_recommendation)
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            test_func()
            print(f"✓ {name} - PASSED")
            passed += 1
        except AssertionError as e:
            print(f"✗ {name} - FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"✗ {name} - ERROR: {e}")
            failed += 1
    
    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed")
    print(f"{'='*50}")
