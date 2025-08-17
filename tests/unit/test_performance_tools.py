"""Unit tests for performance tools module."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.mcp_postgres.tools.performance_tools import (
    _generate_performance_recommendations,
    _generate_slow_query_recommendations,
    _generate_table_recommendations,
    analyze_query_performance,
    find_slow_queries,
    get_table_stats,
)


class TestAnalyzeQueryPerformance:
    """Test cases for analyze_query_performance tool."""

    @pytest.mark.asyncio
    async def test_analyze_query_performance_success(self):
        """Test successful query performance analysis."""
        mock_plan_data = {
            "Plan": {
                "Node Type": "Index Scan",
                "Total Cost": 100.5,
                "Actual Total Time": 25.3,
                "Actual Rows": 150,
                "Plan Rows": 140,
            }
        }

        with patch("src.mcp_postgres.tools.performance_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.performance_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.performance_tools.connection_manager") as mock_conn:

            mock_validate.return_value = (True, None)
            mock_sanitize.return_value = ["param1"]
            mock_conn.execute_query = AsyncMock(return_value=[mock_plan_data])

            result = await analyze_query_performance(
                query="SELECT * FROM users WHERE id = $1",
                parameters=["param1"]
            )

            assert result["success"] is True
            assert "execution_plan" in result["data"]
            assert "performance_metrics" in result["data"]
            assert result["data"]["performance_metrics"]["total_cost"] == 100.5
            assert result["data"]["performance_metrics"]["actual_execution_time_ms"] == 25.3
            assert result["data"]["performance_metrics"]["actual_rows"] == 150
            assert result["data"]["performance_metrics"]["planned_rows"] == 140

            # Verify the EXPLAIN ANALYZE query was constructed correctly
            mock_conn.execute_query.assert_called_once()
            call_args = mock_conn.execute_query.call_args
            assert "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)" in call_args[1]["query"]

    @pytest.mark.asyncio
    async def test_analyze_query_performance_empty_query(self):
        """Test query performance analysis with empty query."""
        result = await analyze_query_performance(query="")

        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "empty" in result["error"]["message"].lower()

    @pytest.mark.asyncio
    async def test_analyze_query_performance_security_failure(self):
        """Test query performance analysis with security validation failure."""
        with patch("src.mcp_postgres.tools.performance_tools.validate_query_permissions") as mock_validate:
            mock_validate.return_value = (False, "Dangerous query detected")

            result = await analyze_query_performance(
                query="DROP TABLE users"
            )

            assert "error" in result
            assert result["error"]["code"] == "SECURITY_ERROR"
            assert "security validation failed" in result["error"]["message"].lower()

    @pytest.mark.asyncio
    async def test_analyze_query_performance_no_plan_data(self):
        """Test query performance analysis when no plan data is returned."""
        with patch("src.mcp_postgres.tools.performance_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.performance_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.performance_tools.connection_manager") as mock_conn:

            mock_validate.return_value = (True, None)
            mock_sanitize.return_value = []
            mock_conn.execute_query = AsyncMock(return_value=[])

            result = await analyze_query_performance(
                query="SELECT 1"
            )

            assert result["success"] is True
            assert result["data"]["execution_plan"] is None
            assert result["data"]["metadata"]["analysis_failed"] is True

    @pytest.mark.asyncio
    async def test_analyze_query_performance_database_error(self):
        """Test query performance analysis with database error."""
        with patch("src.mcp_postgres.tools.performance_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.performance_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.performance_tools.connection_manager") as mock_conn, \
             patch("src.mcp_postgres.tools.performance_tools.handle_postgres_error") as mock_handle_error:

            mock_validate.return_value = (True, None)
            mock_sanitize.return_value = []
            mock_conn.execute_query = AsyncMock(side_effect=Exception("Database connection failed"))

            mock_error = MagicMock()
            mock_error.error_code = "DATABASE_ERROR"
            mock_error.details = None
            mock_handle_error.return_value = mock_error

            result = await analyze_query_performance(
                query="SELECT * FROM users"
            )

            assert "error" in result
            assert result["error"]["code"] == "DATABASE_ERROR"


class TestFindSlowQueries:
    """Test cases for find_slow_queries tool."""

    @pytest.mark.asyncio
    async def test_find_slow_queries_with_pg_stat_statements(self):
        """Test finding slow queries using pg_stat_statements."""
        mock_slow_queries = [
            {
                "query_preview": "SELECT * FROM large_table WHERE...",
                "calls": 1500,
                "total_exec_time": 45000,
                "mean_exec_time": 30,
                "max_exec_time": 120,
                "min_exec_time": 15,
                "stddev_exec_time": 25,
                "total_rows": 75000,
                "avg_time_per_call": 30,
                "percent_total_time": 15.5,
            }
        ]

        with patch("src.mcp_postgres.tools.performance_tools.connection_manager") as mock_conn:
            # First call checks for extension existence
            # Second call gets slow queries
            mock_conn.execute_query = AsyncMock(side_effect=[
                True,  # Extension exists
                [mock_slow_queries[0]]  # Slow queries result
            ])

            result = await find_slow_queries(min_duration_ms=1000, limit=10)

            assert result["success"] is True
            assert result["data"]["analysis_method"] == "pg_stat_statements"
            assert len(result["data"]["slow_queries"]) == 1
            assert result["data"]["slow_queries"][0]["calls"] == 1500
            assert result["data"]["slow_queries"][0]["mean_exec_time_ms"] == 30
            assert "recommendations" in result["data"]

    @pytest.mark.asyncio
    async def test_find_slow_queries_fallback_to_pg_stat_activity(self):
        """Test finding slow queries using pg_stat_activity fallback."""
        mock_active_queries = [
            {
                "pid": 12345,
                "username": "testuser",
                "database": "testdb",
                "state": "active",
                "query_start": datetime.now(),
                "duration_ms": 2500,
                "query_preview": "SELECT COUNT(*) FROM big_table",
            }
        ]

        with patch("src.mcp_postgres.tools.performance_tools.connection_manager") as mock_conn:
            # First call checks for extension existence (returns False)
            # Second call gets active queries
            mock_conn.execute_query = AsyncMock(side_effect=[
                False,  # Extension doesn't exist
                [mock_active_queries[0]]  # Active queries result
            ])

            result = await find_slow_queries(min_duration_ms=1000, limit=10)

            assert result["success"] is True
            assert result["data"]["analysis_method"] == "pg_stat_activity"
            assert "warning" in result["data"]
            assert "pg_stat_statements extension not available" in result["data"]["warning"]
            assert len(result["data"]["slow_queries"]) == 1
            assert result["data"]["slow_queries"][0]["source"] == "pg_stat_activity"

    @pytest.mark.asyncio
    async def test_find_slow_queries_invalid_parameters(self):
        """Test finding slow queries with invalid parameters."""
        # Test negative duration
        result = await find_slow_queries(min_duration_ms=-100)
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "non-negative" in result["error"]["message"]

        # Test invalid limit
        result = await find_slow_queries(limit=0)
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "between 1 and 100" in result["error"]["message"]

        result = await find_slow_queries(limit=150)
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "between 1 and 100" in result["error"]["message"]

    @pytest.mark.asyncio
    async def test_find_slow_queries_no_results(self):
        """Test finding slow queries when no slow queries exist."""
        with patch("src.mcp_postgres.tools.performance_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                True,  # Extension exists
                []  # No slow queries
            ])

            result = await find_slow_queries()

            assert result["success"] is True
            assert result["data"]["total_found"] == 0
            assert len(result["data"]["slow_queries"]) == 0


class TestGetTableStats:
    """Test cases for get_table_stats tool."""

    @pytest.mark.asyncio
    async def test_get_table_stats_success(self):
        """Test successful table statistics retrieval."""
        mock_basic_stats = [
            {
                "schemaname": "public",
                "tablename": "users",
                "column_name": "id",
                "n_distinct": -1,
                "most_common_vals": None,
                "most_common_freqs": None,
                "correlation": 1.0,
            },
            {
                "schemaname": "public",
                "tablename": "users",
                "column_name": "email",
                "n_distinct": 1000,
                "most_common_vals": ["test@example.com"],
                "most_common_freqs": [0.01],
                "correlation": 0.1,
            }
        ]

        mock_size_info = {
            "total_size": "1024 kB",
            "table_size": "800 kB",
            "index_size": "224 kB",
            "total_size_bytes": 1048576,
            "table_size_bytes": 819200,
        }

        mock_table_stats = {
            "inserts": 5000,
            "updates": 1200,
            "deletes": 100,
            "live_tuples": 4900,
            "dead_tuples": 50,
            "last_vacuum": datetime.now(),
            "last_autovacuum": None,
            "last_analyze": datetime.now(),
            "last_autoanalyze": datetime.now(),
            "vacuum_count": 5,
            "autovacuum_count": 10,
            "analyze_count": 3,
            "autoanalyze_count": 8,
        }

        mock_index_stats = [
            {
                "index_name": "users_pkey",
                "index_tuples_read": 15000,
                "index_tuples_fetched": 12000,
                "index_scans": 500,
                "index_size": "128 kB",
            }
        ]

        with patch("src.mcp_postgres.tools.performance_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                True,  # Table exists
                mock_basic_stats,  # Basic stats
                mock_size_info,  # Size info
                mock_table_stats,  # Table stats
                mock_index_stats,  # Index stats
            ])

            result = await get_table_stats(table_name="users")

            assert result["success"] is True
            assert result["data"]["table_name"] == "users"
            assert "size_information" in result["data"]
            assert "row_statistics" in result["data"]
            assert "maintenance_statistics" in result["data"]
            assert "column_statistics" in result["data"]
            assert "index_usage" in result["data"]
            assert "recommendations" in result["data"]

            # Check size information
            assert result["data"]["size_information"]["total_size"] == "1024 kB"
            assert result["data"]["size_information"]["total_size_bytes"] == 1048576

            # Check row statistics
            assert result["data"]["row_statistics"]["live_tuples"] == 4900
            assert result["data"]["row_statistics"]["dead_tuples"] == 50

            # Check column statistics
            assert len(result["data"]["column_statistics"]) == 2
            assert result["data"]["column_statistics"][0]["column_name"] == "id"

            # Check index usage
            assert len(result["data"]["index_usage"]) == 1
            assert result["data"]["index_usage"][0]["index_name"] == "users_pkey"

    @pytest.mark.asyncio
    async def test_get_table_stats_empty_table_name(self):
        """Test table statistics with empty table name."""
        result = await get_table_stats(table_name="")
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "empty" in result["error"]["message"].lower()

    @pytest.mark.asyncio
    async def test_get_table_stats_invalid_table_name(self):
        """Test table statistics with invalid table name."""
        result = await get_table_stats(table_name="users'; DROP TABLE users; --")
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "invalid characters" in result["error"]["message"]

    @pytest.mark.asyncio
    async def test_get_table_stats_table_not_exists(self):
        """Test table statistics for non-existent table."""
        with patch("src.mcp_postgres.tools.performance_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=False)  # Table doesn't exist

            result = await get_table_stats(table_name="nonexistent")

            assert "error" in result
            assert result["error"]["code"] == "VALIDATION_ERROR"
            assert "does not exist" in result["error"]["message"]

    @pytest.mark.asyncio
    async def test_get_table_stats_database_error(self):
        """Test table statistics with database error."""
        with patch("src.mcp_postgres.tools.performance_tools.connection_manager") as mock_conn, \
             patch("src.mcp_postgres.tools.performance_tools.handle_postgres_error") as mock_handle_error:

            mock_conn.execute_query = AsyncMock(side_effect=Exception("Connection failed"))

            mock_error = MagicMock()
            mock_error.error_code = "DATABASE_ERROR"
            mock_error.details = None
            mock_handle_error.return_value = mock_error

            result = await get_table_stats(table_name="users")

            assert "error" in result
            assert result["error"]["code"] == "DATABASE_ERROR"


class TestRecommendationFunctions:
    """Test cases for recommendation generation functions."""

    def test_generate_performance_recommendations_seq_scan(self):
        """Test performance recommendations for sequential scan."""
        plan = {"Node Type": "Seq Scan", "Total Cost": 500, "Actual Rows": 1000, "Plan Rows": 1000}
        recommendations = _generate_performance_recommendations(plan)

        assert any("index" in rec.lower() for rec in recommendations)
        assert any("sequential scan" in rec.lower() for rec in recommendations)

    def test_generate_performance_recommendations_high_cost(self):
        """Test performance recommendations for high cost query."""
        plan = {"Node Type": "Hash Join", "Total Cost": 5000, "Actual Rows": 100, "Plan Rows": 100}
        recommendations = _generate_performance_recommendations(plan)

        assert any("high query cost" in rec.lower() for rec in recommendations)

    def test_generate_performance_recommendations_inaccurate_estimates(self):
        """Test performance recommendations for inaccurate row estimates."""
        plan = {"Node Type": "Index Scan", "Total Cost": 100, "Actual Rows": 10000, "Plan Rows": 100}
        recommendations = _generate_performance_recommendations(plan)

        assert any("analyze" in rec.lower() for rec in recommendations)
        assert any("inaccurate" in rec.lower() for rec in recommendations)

    def test_generate_performance_recommendations_nested_loop(self):
        """Test performance recommendations for nested loop with high rows."""
        plan = {"Node Type": "Nested Loop", "Total Cost": 100, "Actual Rows": 5000, "Plan Rows": 5000}
        recommendations = _generate_performance_recommendations(plan)

        assert any("hash join" in rec.lower() or "merge join" in rec.lower() for rec in recommendations)

    def test_generate_performance_recommendations_optimal(self):
        """Test performance recommendations for optimal query."""
        plan = {"Node Type": "Index Scan", "Total Cost": 50, "Actual Rows": 100, "Plan Rows": 95}
        recommendations = _generate_performance_recommendations(plan)

        assert any("optimal" in rec.lower() for rec in recommendations)

    def test_generate_slow_query_recommendations_high_calls(self):
        """Test slow query recommendations for high call frequency."""
        slow_queries = [{"calls": 5000, "mean_exec_time_ms": 100, "stddev_exec_time_ms": 10}]
        recommendations = _generate_slow_query_recommendations(slow_queries)

        assert any("high call frequency" in rec.lower() for rec in recommendations)
        assert any("caching" in rec.lower() for rec in recommendations)

    def test_generate_slow_query_recommendations_high_variance(self):
        """Test slow query recommendations for high execution time variance."""
        slow_queries = [{"calls": 100, "mean_exec_time_ms": 100, "stddev_exec_time_ms": 80}]
        recommendations = _generate_slow_query_recommendations(slow_queries)

        assert any("variance" in rec.lower() for rec in recommendations)

    def test_generate_slow_query_recommendations_no_queries(self):
        """Test slow query recommendations when no slow queries found."""
        recommendations = _generate_slow_query_recommendations([])

        assert any("no slow queries" in rec.lower() for rec in recommendations)
        assert any("performance appears good" in rec.lower() for rec in recommendations)

    def test_generate_table_recommendations_high_dead_tuples(self):
        """Test table recommendations for high dead tuple ratio."""
        stats = {"live_tuples": 1000, "dead_tuples": 200}
        size = {"total_size_bytes": 1024 * 1024}
        indexes = []

        recommendations = _generate_table_recommendations(stats, size, indexes)

        assert any("dead tuple" in rec.lower() for rec in recommendations)
        assert any("vacuum" in rec.lower() for rec in recommendations)

    def test_generate_table_recommendations_large_table(self):
        """Test table recommendations for large table."""
        stats = {"live_tuples": 1000000, "dead_tuples": 1000}
        size = {"total_size_bytes": 2 * 1024 * 1024 * 1024}  # 2GB
        indexes = []

        recommendations = _generate_table_recommendations(stats, size, indexes)

        assert any("large table" in rec.lower() for rec in recommendations)
        assert any("partitioning" in rec.lower() for rec in recommendations)

    def test_generate_table_recommendations_unused_indexes(self):
        """Test table recommendations for unused indexes."""
        stats = {"live_tuples": 1000, "dead_tuples": 10}
        size = {"total_size_bytes": 1024 * 1024}
        indexes = [{"index_name": "unused_idx", "scans": 0}]

        recommendations = _generate_table_recommendations(stats, size, indexes)

        assert any("unused indexes" in rec.lower() for rec in recommendations)
        assert any("dropping" in rec.lower() for rec in recommendations)

    def test_generate_table_recommendations_never_analyzed(self):
        """Test table recommendations for table never analyzed."""
        stats = {"live_tuples": 1000, "dead_tuples": 10, "last_analyze": None}
        size = {"total_size_bytes": 1024 * 1024}
        indexes = []

        recommendations = _generate_table_recommendations(stats, size, indexes)

        assert any("never been analyzed" in rec.lower() for rec in recommendations)
        assert any("analyze" in rec.lower() for rec in recommendations)

    def test_generate_table_recommendations_healthy(self):
        """Test table recommendations for healthy table."""
        stats = {"live_tuples": 1000, "dead_tuples": 10, "last_analyze": datetime.now()}
        size = {"total_size_bytes": 1024 * 1024}
        indexes = [{"index_name": "active_idx", "scans": 100}]

        recommendations = _generate_table_recommendations(stats, size, indexes)

        assert any("healthy" in rec.lower() for rec in recommendations)
