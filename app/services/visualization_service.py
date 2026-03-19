import logging
from typing import Dict, Any, List, Optional, Tuple
import math
from datetime import datetime
import re

logger = logging.getLogger(__name__)

class VisualizationService:
    """
    Intelligent visualization recommendation service with LLM-powered intent analysis.
    """
    
    # Minimum confidence threshold for chart recommendation (otherwise fallback to table)
    MIN_CHART_CONFIDENCE = 50
    
    # Frontend-implemented chart types
    IMPLEMENTED_CHARTS = {
        "kpi",
        "candlestick",
        "line",
        "bar",
        "pie",
        "scatter",
        "histogram",
        "area",
        "table"
    }
    
    # Supported aliases from LLM responses to implemented charts
    CHART_ALIASES = {
        "column": "bar",
        "columns": "bar",
        "box": "histogram",
        "boxplot": "histogram",
        "box_plot": "histogram",
        "timeseries": "line",
        "time_series": "line",
        "donut": "pie",
        "doughnut": "pie",
        "metric": "kpi",
        "number": "kpi"
    }

    ID_LIKE_TERMS = {"id", "key", "code", "rank", "index", "no", "number"}
    TIME_LIKE_TERMS = {"date", "time", "year", "month", "day", "week", "quarter", "period"}
    NAME_LIKE_TERMS = {"name", "title", "label", "category", "type", "segment", "region", "city", "country"}
    
    @staticmethod
    def recommend_visualization_intelligent(
        results: Dict[str, Any], 
        question: str = "",
        llm_intent: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Intelligent visualization recommendation using LLM intent + data analysis.
        
        Args:
            results: Query results with columns and rows
            question: User's natural language query
            llm_intent: Optional pre-computed LLM intent analysis
            
        Returns:
            Dict containing:
            - recommended_chart: str (kpi, bar, line, scatter, histogram, pie, candlestick, table)
            - confidence: int (0-100)
            - reason: str
            - alternatives: List[Dict] (other suitable chart types with scores)
            - data_config: Dict (optional configuration for frontend)
        """
        columns = results.get("columns", [])
        rows = results.get("rows", [])
        
        if not rows:
            return {
                "recommended_chart": "none",
                "confidence": 0,
                "reason": "No data available.",
                "alternatives": []
            }
        
        # Analyze data characteristics
        data_profile = VisualizationService._profile_data(columns, rows)
        
        # Get LLM intent if not provided
        if llm_intent is None and question:
            try:
                from app.llm_service import analyze_visualization_intent
                llm_intent = analyze_visualization_intent(question)
            except Exception as e:
                logger.warning(f"Intent analysis failed: {e}")
                llm_intent = {"intent": "detail", "confidence": 0.5, "suggested_chart_types": []}
        
        # Score all chart types
        allowed_charts, constrained_by_llm = VisualizationService._resolve_allowed_charts(llm_intent or {})
        
        # If it's a single value, we favor KPI card even if LLM suggested table/detail
        if data_profile.get('is_single_value'):
            return {
                "recommended_chart": "kpi",
                "confidence": 100,
                "reason": "Single value detected - optimal for KPI card visualization.",
                "alternatives": []
            }

        # If LLM explicitly requests table-only, return table directly.
        if constrained_by_llm and allowed_charts == {"table"}:
            return {
                "recommended_chart": "table",
                "confidence": 100,
                "reason": "LLM intent requested table view for this question.",
                "alternatives": []
            }
        
        chart_scores = VisualizationService._score_all_charts(
            data_profile, 
            llm_intent or {},
            question,
            allowed_charts=allowed_charts
        )
        
        # If LLM constrained chart types but none are implementable/scorable, fallback to table.
        if not chart_scores:
            return {
                "recommended_chart": "table",
                "confidence": 100,
                "reason": "No implementable chart type available from LLM chart suggestions.",
                "alternatives": []
            }
        
        # Sort by score
        sorted_charts = sorted(chart_scores.items(), key=lambda x: x[1]['score'], reverse=True)
        
        # Get top recommendation
        best_chart, best_info = sorted_charts[0]
        
        # Hard safety gate: never return non-implementable chart.
        if not VisualizationService._is_chart_implementable(best_chart):
            return {
                "recommended_chart": "table",
                "confidence": 100,
                "reason": f"'{best_chart}' is not implemented in renderer. Falling back to table.",
                "alternatives": []
            }
        
        # Check if confidence meets threshold
        if best_info['score'] < VisualizationService.MIN_CHART_CONFIDENCE:
            return {
                "recommended_chart": "table",
                "confidence": 100,
                "reason": f"Data structure is complex or doesn't fit standard chart types well. Table view provides the clearest presentation.",
                "alternatives": [
                    {"chart": chart, "score": info['score'], "reason": info['reason']} 
                    for chart, info in sorted_charts[:5] if info['score'] > 30
                ],
                **best_info.get('config', {})
            }
        
        # Return best recommendation with alternatives
        return {
            "recommended_chart": best_chart,
            "confidence": best_info['score'],
            "reason": best_info['reason'],
            "alternatives": [
                {"chart": chart, "score": info['score'], "reason": info['reason']} 
                for chart, info in sorted_charts[1:4] if info['score'] > 50
            ],
            "profile_summary": {
                "row_count": data_profile.get("num_rows", 0),
                "column_count": data_profile.get("num_cols", 0),
                "dimension_candidates": data_profile.get("dimension_candidates", []),
                "metric_candidates": data_profile.get("metric_candidates", [])
            },
            **best_info.get('config', {})
        }
    
    @staticmethod
    def _profile_data(columns: List[str], rows: List[Dict]) -> Dict[str, Any]:
        """Advanced data profiling for visualization recommendations."""
        num_cols = len(columns)
        num_rows = len(rows)
        
        # Analyze column types
        col_types = VisualizationService._analyze_column_types(columns, rows)
        numeric_cols = [c for c, t in col_types.items() if t == "numeric"]
        date_cols = [c for c, t in col_types.items() if t == "date"]
        string_cols = [c for c, t in col_types.items() if t == "string"]
        
        # Cardinality analysis
        cardinalities = {}
        for col in columns:
            unique_vals = set(row.get(col) for row in rows if row.get(col) is not None)
            cardinalities[col] = len(unique_vals)
        
        # Detect special patterns
        has_ohlc = VisualizationService._detect_ohlc_pattern(columns)
        
        # Identify "Dimensional" numeric columns (e.g., Year, Month, ID)
        # These are numbers that should be used as labels/axis rather than metrics
        dimension_names = {'month', 'year', 'day', 'quarter', 'id', 'rank', 'period'}
        numeric_dimensions = []
        for col in numeric_cols:
            is_low_cardinality = cardinalities.get(col, 0) < 15 and num_rows > 5
            is_named_dimension = any(name in col.lower() for name in dimension_names)
            if is_named_dimension or is_low_cardinality:
                numeric_dimensions.append(col)

        likely_id_cols = [
            col for col in columns
            if any(term == token for token in re.split(r"[_\W]+", col.lower()) for term in VisualizationService.ID_LIKE_TERMS)
            or col.lower().endswith("_id")
        ]

        metric_candidates = [
            col for col in numeric_cols
            if col not in numeric_dimensions and col not in likely_id_cols
        ]
        if not metric_candidates:
            metric_candidates = [col for col in numeric_cols if col not in likely_id_cols]
        if not metric_candidates:
            metric_candidates = numeric_cols[:]

        dimension_candidates = []
        for col in columns:
            if col in date_cols:
                dimension_candidates.append(col)
            elif col in string_cols:
                dimension_candidates.append(col)
            elif col in numeric_dimensions:
                dimension_candidates.append(col)

        preferred_time_axis = VisualizationService._pick_time_axis(columns, date_cols, numeric_dimensions, cardinalities)
        preferred_category_axis = VisualizationService._pick_category_axis(
            columns=columns,
            string_cols=string_cols,
            numeric_dimensions=numeric_dimensions,
            cardinalities=cardinalities
        )
        
        return {
            "num_cols": num_cols,
            "num_rows": num_rows,
            "col_types": col_types,
            "numeric_cols": numeric_cols,
            "date_cols": date_cols,
            "string_cols": string_cols,
            "numeric_dimensions": numeric_dimensions, # NEW
            "likely_id_cols": likely_id_cols,
            "metric_candidates": metric_candidates,
            "dimension_candidates": dimension_candidates,
            "preferred_time_axis": preferred_time_axis,
            "preferred_category_axis": preferred_category_axis,
            "cardinalities": cardinalities,
            "has_ohlc": has_ohlc,
            "is_single_value": num_rows == 1 and num_cols == 1,
            "is_wide": num_cols > 10,
            "is_long": num_rows > 100
        }
    
    @staticmethod
    def _score_all_charts(
        profile: Dict[str, Any], 
        intent: Dict[str, Any],
        question: str,
        allowed_charts: Optional[set] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Score all chart types based on data profile and user intent."""
        
        scores = {}
        scorer_map = {
            "kpi": lambda: VisualizationService._score_kpi(profile, intent),
            "candlestick": lambda: VisualizationService._score_candlestick(profile, intent),
            "line": lambda: VisualizationService._score_line(profile, intent),
            "bar": lambda: VisualizationService._score_bar(profile, intent),
            "pie": lambda: VisualizationService._score_pie(profile, intent, question),
            "scatter": lambda: VisualizationService._score_scatter(profile, intent),
            "histogram": lambda: VisualizationService._score_histogram(profile, intent),
            "area": lambda: VisualizationService._score_area(profile, intent),
        }
        
        candidate_charts = set(scorer_map.keys()) if allowed_charts is None else {
            chart for chart in allowed_charts if chart in scorer_map
        }
        
        # Force KPI for single values even if not in LLM allowed list
        if profile.get('is_single_value'):
            candidate_charts.add("kpi")
        
        for chart in candidate_charts:
            scores[chart] = scorer_map[chart]()
        
        return scores
    
    # Individual chart scoring methods
    
    @staticmethod
    def _score_kpi(profile: Dict, intent: Dict) -> Dict[str, Any]:
        """Score KPI card suitability."""
        if profile['is_single_value']:
            return {
                'score': 100,
                'reason': 'Single numeric value - perfect for KPI display',
                'config': {'title': profile['numeric_cols'][0] if profile['numeric_cols'] else ''}
            }
        return {'score': 0, 'reason': 'Multiple rows/columns'}
    
    @staticmethod
    def _score_candlestick(profile: Dict, intent: Dict) -> Dict[str, Any]:
        """Score candlestick chart suitability."""
        if profile['has_ohlc'] and len(profile['date_cols']) >= 1:
            return {
                'score': 95,
                'reason': 'OHLC financial data detected',
                'config': {
                    'x_axis': profile['date_cols'][0],
                    'ohlc_cols': profile['has_ohlc']
                }
            }
        return {'score': 0, 'reason': 'Not OHLC data'}
    @staticmethod
    def _score_line(profile: Dict, intent: Dict) -> Dict[str, Any]:
        """Score line chart suitability."""
        score = 0
        reason = ""
        config = {}
        
        # Perfect for time-series
        time_axis = profile.get('preferred_time_axis')
        metrics = VisualizationService._pick_metric_columns(profile, exclude={time_axis} if time_axis else set())
        
        if time_axis and metrics:
            score = 85
            reason = "Time-series data detected"
            config = {
                'x_axis': time_axis,
                'x_axis_type': 'date' if time_axis in profile['date_cols'] else 'category',
                'y_axis': metrics[0],
                'all_metrics': metrics,
                'series_type': 'line'
            }
            
            # Boost for trend intent
            if intent.get('intent') == 'trend_analysis':
                score += 10
                reason = "Time-series trend analysis"
        else:
            score = 20
            reason = "No appropriate time axis or metric columns found"
        
        return {'score': min(score, 100), 'reason': reason, 'config': config}
    
    @staticmethod
    def _score_bar(profile: Dict, intent: Dict) -> Dict[str, Any]:
        """Score bar chart suitability."""
        score = 0
        reason = ""
        config = {}
        
        # Good for categorical comparisons
        # Labels can be strings OR numeric dimensions (like ID, Month number)
        cat_axis = profile.get('preferred_category_axis')
        metrics = VisualizationService._pick_metric_columns(profile, exclude={cat_axis} if cat_axis else set())

        if cat_axis and metrics:
            score = 75
            reason = "Categorical comparison data"
            config = {
                'x_axis': cat_axis,
                'x_axis_type': 'category',
                'y_axis': metrics[0],
                'all_metrics': metrics,
                'series_type': 'bar'
            }
            
            # Boost for comparison intent
            if intent.get('intent') == 'comparison':
                score += 15
                reason = "Categorical comparison analysis"
            
            # Penalize if too many categories
            cardinality = profile['cardinalities'].get(cat_axis, 0)
            if cardinality > 50:
                score -= 30
                reason += " (very many categories - will be cluttered)"
            elif cardinality > 20:
                score -= 10
                reason += " (many categories)"
        else:
            score = 30
            reason = "Missing categorical labels or numeric metrics"
        
        return {'score': min(score, 100), 'reason': reason, 'config': config}
    
    @staticmethod
    def _score_pie(profile: Dict, intent: Dict, question: str) -> Dict[str, Any]:
        """Score pie chart suitability."""
        score = 0
        reason = ""
        config = {}
        
        # Only good for categorical breakdowns
        if len(profile['string_cols']) >= 1 and len(profile['numeric_cols']) >= 1:
            cat_col = profile.get('preferred_category_axis') or profile['string_cols'][0]
            metric_col = VisualizationService._pick_metric_columns(profile, exclude={cat_col})[:1]
            cardinality = profile['cardinalities'].get(cat_col, 0)
            
            # Composition intent or "percentage" keywords allow for higher cardinality (up to 20)
            is_composition = intent.get('intent') == 'composition' or any(kw in question.lower() for kw in ['percent', 'percentage', 'share', 'proportion', 'breakdown'])
            max_pie_cats = 20 if is_composition else 7
            
            if cardinality <= max_pie_cats and profile['num_rows'] <= max_pie_cats:
                score = 70
                reason = "Categorical breakdown - suitable for pie"
                config = {
                    'x_axis': cat_col,
                    'y_axis': metric_col[0] if metric_col else profile['numeric_cols'][0],
                    'all_metrics': metric_col if metric_col else [profile['numeric_cols'][0]],
                    'series_type': 'pie'
                }
                
                # Boost for composition intent or breakdown keywords
                if is_composition:
                    score += 20
                    reason = "Part-to-whole composition analysis"
                    
                # Extra boost if "percent" is explicitly asked
                if 'percent' in question.lower():
                    score += 10
            else:
                score = 20
                reason = f"Too many categories ({cardinality}) for an effective pie chart"
        else:
            score = 10
            reason = "Missing categorical data"
        
        return {'score': min(score, 100), 'reason': reason, 'config': config}
    
    @staticmethod
    def _score_scatter(profile: Dict, intent: Dict) -> Dict[str, Any]:
        """Score scatter plot suitability."""
        score = 0
        reason = ""
        config = {}
        
        # Needs 2+ numeric columns and reasonable row count
        if len(profile['numeric_cols']) >= 2 and profile['num_rows'] >= 10:
            numeric_candidates = VisualizationService._pick_metric_columns(profile)
            if len(numeric_candidates) < 2:
                numeric_candidates = profile['numeric_cols'][:]
            score = 75
            reason = "Two numeric variables for correlation analysis"
            config = {
                'x_axis': numeric_candidates[0],
                'y_axis': numeric_candidates[1],
                'all_metrics': [numeric_candidates[1]],
                'series_type': 'scatter'
            }
            
            # Boost for correlation intent
            if intent.get('intent') == 'correlation':
                score += 20
                reason = "Correlation/relationship analysis"
            
            # Penalize if too few points
            if profile['num_rows'] < 20:
                score -= 15
        else:
            score = 10
            reason = "Insufficient numeric columns or data points"
        
        return {'score': min(score, 100), 'reason': reason, 'config': config}
    
    @staticmethod
    def _score_histogram(profile: Dict, intent: Dict) -> Dict[str, Any]:
        """Score histogram suitability."""
        score = 0
        reason = ""
        config = {}
        
        # Best for single numeric distribution
        if len(profile['numeric_cols']) >= 1 and profile['num_cols'] <= 2 and profile['num_rows'] > 10:
            metric_col = VisualizationService._pick_metric_columns(profile)[:1]
            score = 75
            reason = "Single variable distribution analysis"
            config = {
                'x_axis': metric_col[0] if metric_col else profile['numeric_cols'][0],
                'series_type': 'histogram'
            }
            
            # Boost for distribution intent
            if intent.get('intent') == 'distribution':
                score += 20
                reason = "Statistical distribution analysis"
        else:
            score = 20
            reason = "Not suitable for histogram"
        
        return {'score': min(score, 100), 'reason': reason, 'config': config}
    
    @staticmethod
    def _score_area(profile: Dict, intent: Dict) -> Dict[str, Any]:
        """Score area chart suitability."""
        # Similar to line but emphasizes magnitude
        line_score = VisualizationService._score_line(profile, intent)
        
        if line_score['score'] > 60:
            return {
                'score': line_score['score'] - 10,  # Slightly lower than line
                'reason': line_score['reason'].replace('line', 'area').replace('Line', 'Area'),
                'config': {
                    **line_score.get('config', {}),
                    'series_type': 'area'
                }
            }
        
        return {'score': 10, 'reason': 'Not suitable for area chart'}
    
    # Legacy method for backward compatibility
    @staticmethod
    def recommend_visualization(results: Dict[str, Any], question: str = "") -> Dict[str, Any]:
        """
        Legacy method - calls the intelligent recommendation engine.
        Kept for backward compatibility.
        """
        return VisualizationService.recommend_visualization_intelligent(results, question)
    
    # Helper methods (from original implementation)
    
    @staticmethod
    def _analyze_column_types(columns: List[str], rows: List[Dict]) -> Dict[str, str]:
        """Heuristic type inference for columns based on data."""
        types = {}
        if not rows:
            return {c: "string" for c in columns}
            
        for col in columns:
            col_type = "string"
            # Sample up to 10 rows for more robust detection
            sample_values = []
            for r in rows[:10]:
                val = r.get(col)
                if val is not None:
                    sample_values.append(val)
            
            if not sample_values:
                types[col] = "string"
                continue
            
            # Check if all sampled values are numeric or numeric strings
            is_numeric = True
            is_date = True
            
            for val in sample_values:
                # Numeric check
                if not isinstance(val, (int, float)):
                    try:
                        # Attempt to parse string as float
                        float(str(val).replace(',', '').replace('$', ''))
                    except (ValueError, TypeError):
                        is_numeric = False
                
                # Date check
                if not (isinstance(val, (datetime, str)) and VisualizationService._is_date(str(val))):
                    is_date = False
            
            if is_numeric:
                col_type = "numeric"
            elif is_date:
                col_type = "date"
            
            types[col] = col_type
                
        return types
    
    @staticmethod
    def _is_date(val: str) -> bool:
        """Simple regex check for common date formats."""
        if not val or not isinstance(val, str):
            return False
            
        date_patterns = [
            r"^\d{4}-\d{2}-\d{2}",  # ISO Date (2023-01-01)
            r"^\d{4}-\d{2}$",       # ISO Month (2023-01)
            r"^\d{4}$",              # ISO Year (2023)
            r"^\d{2}/\d{2}/\d{4}",  # US Date (01/01/2023)
            r"^\d{2}-\d{2}-\d{4}",  # Other Date (01-01-2023)
            r"^\d{4}/\d{2}/\d{2}"   # Alternative ISO (2023/01/01)
        ]
        for p in date_patterns:
            if re.match(p, val.strip()):
                return True
        return False
    
    @staticmethod
    def _detect_ohlc_pattern(columns: List[str]) -> dict:
        """Detect OHLC (Open, High, Low, Close) pattern in columns."""
        col_map = {c.lower(): c for c in columns}
        
        # Define required columns with aliases
        required_with_aliases = {
            'open': ['open', 'o'],
            'high': ['high', 'h'],
            'low': ['low', 'l'],
            'close': ['close', 'last', 'ltp', 'c']  # LAST, LTP (Last Traded Price) are common aliases
        }
        
        matched = {}
        for req, aliases in required_with_aliases.items():
            # Try exact match with any alias
            for alias in aliases:
                if alias in col_map:
                    matched[req] = col_map[alias]
                    break
            
            # If not found, try fuzzy matching (contains)
            if req not in matched:
                for col in columns:
                    col_lower = col.lower()
                    if any(alias in col_lower for alias in aliases):
                        matched[req] = col
                        break
        
        # Return matched columns only if all 4 are found
        if len(matched) == 4:
            return matched
        return None
    
    @staticmethod
    def _normalize_chart_name(chart: str) -> Optional[str]:
        """Normalize LLM-provided chart names to implemented chart identifiers."""
        if not chart:
            return None
        
        normalized = str(chart).strip().lower().replace("-", "_").replace(" ", "_")
        normalized = VisualizationService.CHART_ALIASES.get(normalized, normalized)
        
        if normalized in VisualizationService.IMPLEMENTED_CHARTS:
            return normalized
        return None
    
    @staticmethod
    def _resolve_allowed_charts(intent: Dict[str, Any]) -> Tuple[Optional[set], bool]:
        """
        Resolve LLM-specified chart constraints.
        
        Returns:
            (allowed_charts, constrained_by_llm)
            - allowed_charts=None means no LLM chart constraint (score all implemented charts)
            - allowed_charts=set(...) means score only those charts
        """
        if not intent:
            return None, False
        
        raw_candidates = []
        
        recommended_chart = intent.get("recommended_chart")
        if isinstance(recommended_chart, str) and recommended_chart.strip():
            raw_candidates.append(recommended_chart)
        
        for key in ("allowed_charts", "suggested_chart_types"):
            candidate_list = intent.get(key)
            if isinstance(candidate_list, list):
                raw_candidates.extend(candidate_list)
        
        if not raw_candidates:
            return None, False
        
        normalized = {
            chart for chart in
            (VisualizationService._normalize_chart_name(c) for c in raw_candidates)
            if chart
        }
        
        return normalized, True

    @staticmethod
    def _pick_time_axis(
        columns: List[str],
        date_cols: List[str],
        numeric_dimensions: List[str],
        cardinalities: Dict[str, int]
    ) -> Optional[str]:
        if date_cols:
            return sorted(date_cols, key=lambda c: cardinalities.get(c, 0), reverse=True)[0]

        candidates = [
            col for col in columns
            if any(term in col.lower() for term in VisualizationService.TIME_LIKE_TERMS)
        ]
        if candidates:
            return sorted(candidates, key=lambda c: cardinalities.get(c, 0), reverse=True)[0]

        if numeric_dimensions:
            timeish = [
                col for col in numeric_dimensions
                if any(term in col.lower() for term in VisualizationService.TIME_LIKE_TERMS)
            ]
            if timeish:
                return sorted(timeish, key=lambda c: cardinalities.get(c, 0), reverse=True)[0]

        return None

    @staticmethod
    def _pick_category_axis(
        columns: List[str],
        string_cols: List[str],
        numeric_dimensions: List[str],
        cardinalities: Dict[str, int]
    ) -> Optional[str]:
        preferred_strings = [
            col for col in string_cols
            if any(term in col.lower() for term in VisualizationService.NAME_LIKE_TERMS)
        ]
        if preferred_strings:
            return sorted(
                preferred_strings,
                key=lambda c: (cardinalities.get(c, 0) > 1, cardinalities.get(c, 0)),
                reverse=True
            )[0]

        if string_cols:
            viable = [col for col in string_cols if 1 < cardinalities.get(col, 0) <= 50]
            target = viable or string_cols
            return sorted(target, key=lambda c: cardinalities.get(c, 0), reverse=True)[0]

        if numeric_dimensions:
            return sorted(numeric_dimensions, key=lambda c: cardinalities.get(c, 0), reverse=True)[0]

        return None

    @staticmethod
    def _pick_metric_columns(profile: Dict[str, Any], exclude: Optional[set] = None) -> List[str]:
        exclude = exclude or set()
        metrics = [col for col in profile.get('metric_candidates', []) if col not in exclude]
        if metrics:
            return metrics

        numeric_cols = [col for col in profile.get('numeric_cols', []) if col not in exclude]
        likely_ids = set(profile.get('likely_id_cols', []))
        non_ids = [col for col in numeric_cols if col not in likely_ids]
        return non_ids or numeric_cols
    
    @staticmethod
    def _is_chart_implementable(chart: str) -> bool:
        """Verify chart type is implemented in renderer."""
        return chart in VisualizationService.IMPLEMENTED_CHARTS
