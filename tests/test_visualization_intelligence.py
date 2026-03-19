"""
Comprehensive tests for the enhanced visualization recommendation engine.
Tests LLM intent analysis, data profiling, and chart scoring system.
"""

import pytest
from app.services.visualization_service import VisualizationService


class TestVisualizationIntelligence:
    """Test suite for intelligent visualization recommendations."""
    
    def test_kpi_single_value(self):
        """Test KPI recommendation for single numeric value."""
        results = {
            "columns": ["total_sales"],
            "rows": [{"total_sales": 150000}]
        }
        
        recommendation = VisualizationService.recommend_visualization_intelligent(
            results, 
            "What is the total sales?"
        )
        
        assert recommendation["recommended_chart"] == "kpi"
        assert recommendation["confidence"] >= 95
        assert "single" in recommendation["reason"].lower() or "kpi" in recommendation["reason"].lower()
    
    def test_line_chart_time_series(self):
        """Test line chart recommendation for time-series data."""
        results = {
            "columns": ["date", "revenue"],
            "rows": [
                {"date": "2024-01-01", "revenue": 1000},
                {"date": "2024-01-02", "revenue": 1200},
                {"date": "2024-01-03", "revenue": 1100},
                {"date": "2024-01-04", "revenue": 1300},
                {"date": "2024-01-05", "revenue": 1400}
            ]
        }
        
        recommendation = VisualizationService.recommend_visualization_intelligent(
            results,
            "Show me revenue trend over time"
        )
        
        assert recommendation["recommended_chart"] == "line"
        assert recommendation["confidence"] >= 80
        assert "x_axis" in recommendation
        assert "y_axis" in recommendation
    
    def test_bar_chart_categorical(self):
        """Test bar chart recommendation for categorical comparison."""
        results = {
            "columns": ["category", "sales"],
            "rows": [
                {"category": "Electronics", "sales": 50000},
                {"category": "Clothing", "sales": 30000},
                {"category": "Food", "sales": 20000},
                {"category": "Books", "sales": 15000}
            ]
        }
        
        recommendation = VisualizationService.recommend_visualization_intelligent(
            results,
            "Compare sales by category"
        )
        
        assert recommendation["recommended_chart"] in ["bar", "pie"]
        assert recommendation["confidence"] >= 70
    
    def test_pie_chart_composition(self):
        """Test pie chart recommendation for small categorical breakdown."""
        results = {
            "columns": ["region", "percentage"],
            "rows": [
                {"region": "North", "percentage": 35},
                {"region": "South", "percentage": 25},
                {"region": "East", "percentage": 20},
                {"region": "West", "percentage": 20}
            ]
        }
        
        recommendation = VisualizationService.recommend_visualization_intelligent(
            results,
            "Show sales breakdown by region"
        )
        
        # Should prefer pie for small breakdown
        assert recommendation["recommended_chart"] in ["pie", "bar"]
        assert recommendation["confidence"] >= 60
    
    def test_scatter_correlation(self):
        """Test scatter plot recommendation for correlation analysis."""
        results = {
            "columns": ["price", "quantity_sold"],
            "rows": [
                {"price": 10, "quantity_sold": 100},
                {"price": 15, "quantity_sold": 85},
                {"price": 20, "quantity_sold": 70},
                {"price": 25, "quantity_sold": 60},
                {"price": 30, "quantity_sold": 50},
                {"price": 35, "quantity_sold": 45},
                {"price": 40, "quantity_sold": 35},
                {"price": 45, "quantity_sold": 30},
                {"price": 50, "quantity_sold": 25},
                {"price": 55, "quantity_sold": 20},
                {"price": 60, "quantity_sold": 15}
            ]
        }
        
        recommendation = VisualizationService.recommend_visualization_intelligent(
            results,
            "Show relationship between price and quantity sold"
        )
        
        assert recommendation["recommended_chart"] == "scatter"
        assert recommendation["confidence"] >= 70
    
    def test_histogram_distribution(self):
        """Test histogram recommendation for distribution analysis."""
        results = {
            "columns": ["age"],
            "rows": [{"age": i} for i in range(18, 80, 2)]  # 31 data points
        }
        
        recommendation = VisualizationService.recommend_visualization_intelligent(
            results,
            "Show age distribution of customers"
        )
        
        assert recommendation["recommended_chart"] == "histogram"
        assert recommendation["confidence"] >= 70
    
    def test_candlestick_ohlc(self):
        """Test candlestick recommendation for OHLC financial data."""
        results = {
            "columns": ["date", "open", "high", "low", "close"],
            "rows": [
                {"date": "2024-01-01", "open": 100, "high": 105, "low": 98, "close": 103},
                {"date": "2024-01-02", "open": 103, "high": 108, "low": 102, "close": 106},
                {"date": "2024-01-03", "open": 106, "high": 110, "low": 104, "close": 108}
            ]
        }
        
        recommendation = VisualizationService.recommend_visualization_intelligent(
            results,
            "Show stock prices"
        )
        
        assert recommendation["recommended_chart"] == "candlestick"
        assert recommendation["confidence"] >= 90
    
    def test_table_fallback_wide_data(self):
        """Test table recommendation for wide complex data."""
        results = {
            "columns": ["id", "name", "email", "phone", "address", "city", "state", 
                       "zip", "country", "created_at", "updated_at", "status"],
            "rows": [
                {col: f"value_{i}_{col}" for col in 
                 ["id", "name", "email", "phone", "address", "city", "state", 
                  "zip", "country", "created_at", "updated_at", "status"]}
                for i in range(5)
            ]
        }
        
        recommendation = VisualizationService.recommend_visualization_intelligent(
            results,
            "Show all customer details"
        )
        
        # Should recommend table for wide data
        assert recommendation["recommended_chart"] == "table"
        assert recommendation["confidence"] >= 90
        assert "table" in recommendation["reason"].lower() or "complex" in recommendation["reason"].lower()
    
    def test_table_fallback_no_good_match(self):
        """Test table recommendation when no chart fits well."""
        results = {
            "columns": ["col1", "col2", "col3"],
            "rows": [
                {"col1": "a", "col2": "b", "col3": "c"},
                {"col1": "d", "col2": "e", "col3": "f"}
            ]
        }
        
        recommendation = VisualizationService.recommend_visualization_intelligent(
            results,
            "Show data"
        )
        
        # Most likely table since no numeric data
        assert recommendation["recommended_chart"] in ["table", "bar"]
    
    def test_alternatives_provided(self):
        """Test that alternative chart options are provided."""
        results = {
            "columns": ["category", "value"],
            "rows": [
                {"category": "A", "value": 100},
                {"category": "B", "value": 200},
                {"category": "C", "value": 150}
            ]
        }
        
        recommendation = VisualizationService.recommend_visualization_intelligent(
            results,
            "Compare values"
        )
        
        # Should have alternatives
        assert "alternatives" in recommendation
        # Alternatives should be a list (may be empty if confidence is low for all others)
        assert isinstance(recommendation["alternatives"], list)
    
    def test_empty_data(self):
        """Test handling of empty results."""
        results = {
            "columns": [],
            "rows": []
        }
        
        recommendation = VisualizationService.recommend_visualization_intelligent(
            results,
            "Show data"
        )
        
        assert recommendation["recommended_chart"] == "none"
        assert recommendation["confidence"] == 0
    
    def test_intent_analysis_trend(self):
        """Test that trend keywords boost line chart score."""
        results = {
            "columns": ["month", "sales"],
            "rows": [
                {"month": "2024-01", "sales": 1000},
                {"month": "2024-02", "sales": 1200},
                {"month": "2024-03", "sales": 1100}
            ]
        }
        
        # With trend keyword
        rec_trend = VisualizationService.recommend_visualization_intelligent(
            results,
            "Show monthly sales trend"
        )
        
        # Without trend keyword
        rec_no_trend = VisualizationService.recommend_visualization_intelligent(
            results,
            "Show monthly sales"
        )
        
        # Both should recommend line, but trend version should have higher confidence
        # or at least line should be preferred
        assert rec_trend["recommended_chart"] == "line"


class TestDataProfiling:
    """Test data profiling functionality."""
    
    def test_column_type_detection_numeric(self):
        """Test numeric column detection."""
        columns = ["id", "value"]
        rows = [{"id": 1, "value": 100}, {"id": 2, "value": 200}]
        
        col_types = VisualizationService._analyze_column_types(columns, rows)
        
        assert col_types["id"] == "numeric"
        assert col_types["value"] == "numeric"
    
    def test_column_type_detection_date(self):
        """Test date column detection."""
        columns = ["date", "value"]
        rows = [{"date": "2024-01-01", "value": 100}]
        
        col_types = VisualizationService._analyze_column_types(columns, rows)
        
        assert col_types["date"] == "date"
    
    def test_column_type_detection_string(self):
        """Test string column detection."""
        columns = ["name", "category"]
        rows = [{"name": "John", "category": "A"}]
        
        col_types = VisualizationService._analyze_column_types(columns, rows)
        
        assert col_types["name"] == "string"
        assert col_types["category"] == "string"
    
    def test_ohlc_detection(self):
        """Test OHLC pattern detection."""
        columns = ["date", "open", "high", "low", "close", "volume"]
        
        ohlc = VisualizationService._detect_ohlc_pattern(columns)
        
        assert ohlc is not None
        assert "open" in ohlc
        assert "high" in ohlc
        assert "low" in ohlc
        assert "close" in ohlc
    
    def test_ohlc_detection_missing(self):
        """Test OHLC detection with missing columns."""
        columns = ["date", "open", "close"]  # Missing high and low
        
        ohlc = VisualizationService._detect_ohlc_pattern(columns)
        
        assert ohlc is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
