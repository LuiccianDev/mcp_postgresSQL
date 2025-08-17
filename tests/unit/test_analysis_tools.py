"""Unit tests for analysis tools module."""

from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from src.mcp_postgres.tools.analysis_tools import (
    analyze_column,
    analyze_correlations,
    find_duplicates,
    profile_table,
)


class TestAnalyzeColumn:
    """Test cases for analyze_column function."""

    @pytest.mark.asyncio
    async def test_analyze_column_success_numeric(self):
        """Test successful column analysis for numeric column."""
        with patch('src.mcp_postgres.tools.analysis_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                {"column_name": "age", "data_type": "integer", "is_nullable": "NO"},
                {"total_rows": 1000, "non_null_count": 950, "null_count": 50, "distinct_count": 45},
                {"min_value": 18, "max_value": 65, "avg_value": Decimal("35.5"), "std_dev": Decimal("12.3"), "q1": Decimal("28.0"), "median": Decimal("35.0"), "q3": Decimal("42.0")},
                [{"value": 25, "frequency": 50}, {"value": 30, "frequency": 45}]
            ])

            result = await analyze_column("users", "age")

            assert result["analysis_type"] == "column_analysis"
            assert result["table_name"] == "users"
            assert result["column_name"] == "age"
            assert "results" in result

    @pytest.mark.asyncio
    async def test_analyze_column_invalid_table(self):
        """Test analyze_column with invalid table name."""
        result = await analyze_column("", "age")
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"

    @pytest.mark.asyncio
    async def test_analyze_column_column_not_found(self):
        """Test analyze_column with non-existent column."""
        with patch('src.mcp_postgres.tools.analysis_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=None)

            result = await analyze_column("users", "nonexistent")

            assert "error" in result
            assert result["error"]["code"] == "VALIDATION_ERROR"


class TestFindDuplicates:
    """Test cases for find_duplicates function."""

    @pytest.mark.asyncio
    async def test_find_duplicates_success(self):
        """Test successful duplicate detection."""
        with patch('src.mcp_postgres.tools.analysis_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                [{"column_name": "id"}, {"column_name": "name"}],
                [{"id": 1, "name": "John", "duplicate_count": 3}],
                {"total_duplicate_groups": 1, "total_duplicate_rows": 2}
            ])

            result = await find_duplicates("users")

            assert result["analysis_type"] == "duplicate_analysis"
            assert result["table_name"] == "users"

    @pytest.mark.asyncio
    async def test_find_duplicates_invalid_table(self):
        """Test find_duplicates with invalid table name."""
        result = await find_duplicates("", [])
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"


class TestProfileTable:
    """Test cases for profile_table function."""

    @pytest.mark.asyncio
    async def test_profile_table_success(self):
        """Test successful table profiling."""
        with patch('src.mcp_postgres.tools.analysis_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                [{"column_name": "id", "data_type": "integer", "is_nullable": "NO", "column_default": None, "character_maximum_length": None, "numeric_precision": 32, "numeric_scale": 0}],
                {"total_rows": 1000},
                {"sample_rows": 1000, "non_null_count": 1000, "null_count": 0, "distinct_count": 1000},
                {"min_value": 1, "max_value": 1000, "avg_value": Decimal("500.5")}
            ])

            result = await profile_table("users")

            assert result["analysis_type"] == "table_profile"
            assert result["table_name"] == "users"

    @pytest.mark.asyncio
    async def test_profile_table_invalid_table(self):
        """Test profile_table with invalid table name."""
        result = await profile_table("")
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"


class TestAnalyzeCorrelations:
    """Test cases for analyze_correlations function."""

    @pytest.mark.asyncio
    async def test_analyze_correlations_success(self):
        """Test successful correlation analysis."""
        with patch('src.mcp_postgres.tools.analysis_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                [{"column_name": "age"}, {"column_name": "salary"}],
                {"n": 1000, "correlation_coefficient": Decimal("0.75")}
            ])

            result = await analyze_correlations("employees")

            assert result["analysis_type"] == "correlation_analysis"
            assert result["table_name"] == "employees"

    @pytest.mark.asyncio
    async def test_analyze_correlations_invalid_table(self):
        """Test correlation analysis with invalid table name."""
        result = await analyze_correlations("")
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"

    @pytest.mark.asyncio
    async def test_analyze_correlations_unsupported_method(self):
        """Test correlation analysis with unsupported method."""
        result = await analyze_correlations("employees", method="spearman")
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
