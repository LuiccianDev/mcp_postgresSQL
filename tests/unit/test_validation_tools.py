"""Unit tests for validation tools module."""

from unittest.mock import AsyncMock, patch

import pytest

from src.mcp_postgres.tools.validation_tools import (
    check_data_integrity,
    validate_constraints,
    validate_data_types,
)


class TestValidateConstraints:
    """Test cases for validate_constraints function."""

    @pytest.mark.asyncio
    async def test_validate_constraints_success_no_violations(self):
        """Test successful constraint validation with no violations."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                {"table_name": "users", "table_type": "BASE TABLE"},  # Table exists
                [{"column_name": "email", "data_type": "varchar"}],  # NOT NULL columns
                {"null_count": 0},  # No NULL violations
                [{"constraint_name": "users_pkey", "columns": "id"}],  # Primary key
                [],  # No PK duplicates
                [{"constraint_name": "users_email_key", "columns": "email"}],  # Unique constraint
                [],  # No unique violations
                [{"constraint_name": "users_dept_fkey", "column_name": "dept_id", "foreign_table_name": "departments", "foreign_column_name": "id"}],  # Foreign key
                {"violation_count": 0},  # No FK violations
                [{"constraint_name": "users_age_check", "check_clause": "age >= 0"}],  # Check constraint
            ])

            result = await validate_constraints("users")

            assert result["analysis_type"] == "constraint_validation"
            assert result["table_name"] == "users"
            assert "results" in result
            assert result["results"]["validation_summary"]["violations_found"] == 0

    @pytest.mark.asyncio
    async def test_validate_constraints_with_violations(self):
        """Test constraint validation with violations found."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                {"table_name": "users", "table_type": "BASE TABLE"},  # Table exists
                [{"column_name": "email", "data_type": "varchar"}],  # NOT NULL columns
                {"null_count": 5},  # NULL violations found
                [],  # No primary key constraints
                [],  # No unique constraints
                [],  # No foreign key constraints
                [],  # No check constraints
            ])

            result = await validate_constraints("users")

            assert result["analysis_type"] == "constraint_validation"
            assert result["table_name"] == "users"
            assert result["results"]["validation_summary"]["violations_found"] == 1
            assert len(result["results"]["constraint_violations"]) == 1
            assert result["results"]["constraint_violations"][0]["constraint_type"] == "NOT NULL"

    @pytest.mark.asyncio
    async def test_validate_constraints_invalid_table(self):
        """Test validate_constraints with invalid table name."""
        result = await validate_constraints("")
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"

    @pytest.mark.asyncio
    async def test_validate_constraints_table_not_found(self):
        """Test validate_constraints with non-existent table."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=None)

            result = await validate_constraints("nonexistent")

            assert "error" in result
            assert result["error"]["code"] == "VALIDATION_ERROR"

    @pytest.mark.asyncio
    async def test_validate_constraints_primary_key_violations(self):
        """Test primary key constraint violations."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                {"table_name": "users", "table_type": "BASE TABLE"},  # Table exists
                [],  # No NOT NULL columns
                [{"constraint_name": "users_pkey", "columns": "id"}],  # Primary key
                [{"id": 1, "duplicate_count": 3}],  # PK duplicates found
                [],  # No unique constraints
                [],  # No foreign key constraints
                [],  # No check constraints
            ])

            result = await validate_constraints("users")

            assert result["results"]["validation_summary"]["violations_found"] == 1
            pk_violation = result["results"]["constraint_violations"][0]
            assert pk_violation["constraint_type"] == "PRIMARY KEY"
            assert pk_violation["violation_count"] == 1

    @pytest.mark.asyncio
    async def test_validate_constraints_foreign_key_violations(self):
        """Test foreign key constraint violations."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                {"table_name": "users", "table_type": "BASE TABLE"},  # Table exists
                [],  # No NOT NULL columns
                [],  # No primary key constraints
                [],  # No unique constraints
                [{"constraint_name": "users_dept_fkey", "column_name": "dept_id", "foreign_table_name": "departments", "foreign_column_name": "id"}],  # Foreign key
                {"violation_count": 3},  # FK violations found
                [{"orphaned_value": 999}, {"orphaned_value": 888}],  # Examples
                [],  # No check constraints
            ])

            result = await validate_constraints("users")

            assert result["results"]["validation_summary"]["violations_found"] == 1
            fk_violation = result["results"]["constraint_violations"][0]
            assert fk_violation["constraint_type"] == "FOREIGN KEY"
            assert fk_violation["violation_count"] == 3
            assert len(fk_violation["examples"]) == 2


class TestValidateDataTypes:
    """Test cases for validate_data_types function."""

    @pytest.mark.asyncio
    async def test_validate_data_types_success_no_violations(self):
        """Test successful data type validation with no violations."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                [{"column_name": "id", "data_type": "integer", "is_nullable": "NO", "column_default": None, "character_maximum_length": None, "numeric_precision": 32, "numeric_scale": 0}],
                {"null_count": 0},  # No NULL violations
            ])

            result = await validate_data_types("users")

            assert result["analysis_type"] == "data_type_validation"
            assert result["table_name"] == "users"
            assert result["results"]["validation_summary"]["total_violations"] == 0

    @pytest.mark.asyncio
    async def test_validate_data_types_single_column(self):
        """Test data type validation for a single column."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                [{"column_name": "email", "data_type": "varchar", "is_nullable": "YES", "column_default": None, "character_maximum_length": 255, "numeric_precision": None, "numeric_scale": None}],
                {"violation_count": 0},  # No length violations
            ])

            result = await validate_data_types("users", "email")

            assert result["analysis_type"] == "data_type_validation"
            assert result["table_name"] == "users"
            assert result["column_name"] == "email"

    @pytest.mark.asyncio
    async def test_validate_data_types_varchar_length_violations(self):
        """Test varchar length violations."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                [{"column_name": "name", "data_type": "varchar", "is_nullable": "YES", "column_default": None, "character_maximum_length": 50, "numeric_precision": None, "numeric_scale": None}],
                {"violation_count": 3},  # Length violations found
                [{"name": "This is a very long name that exceeds the limit", "actual_length": 55}],  # Examples
            ])

            result = await validate_data_types("users")

            assert result["results"]["validation_summary"]["total_violations"] == 3
            column_validation = result["results"]["column_validations"][0]
            assert column_validation["violation_count"] == 3
            assert column_validation["violations"][0]["violation_type"] == "length_exceeded"

    @pytest.mark.asyncio
    async def test_validate_data_types_smallint_range_violations(self):
        """Test smallint range violations."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                [{"column_name": "score", "data_type": "smallint", "is_nullable": "YES", "column_default": None, "character_maximum_length": None, "numeric_precision": 16, "numeric_scale": 0}],
                {"violation_count": 2},  # Range violations found
            ])

            result = await validate_data_types("users")

            assert result["results"]["validation_summary"]["total_violations"] == 2
            column_validation = result["results"]["column_validations"][0]
            assert column_validation["violations"][0]["violation_type"] == "out_of_range"

    @pytest.mark.asyncio
    async def test_validate_data_types_numeric_precision_violations(self):
        """Test numeric precision/scale violations."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                [{"column_name": "price", "data_type": "numeric", "is_nullable": "YES", "column_default": None, "character_maximum_length": None, "numeric_precision": 10, "numeric_scale": 2}],
                {"violation_count": 1},  # Precision violations found
            ])

            result = await validate_data_types("users")

            assert result["results"]["validation_summary"]["total_violations"] == 1
            column_validation = result["results"]["column_validations"][0]
            assert column_validation["violations"][0]["violation_type"] == "precision_scale_violation"

    @pytest.mark.asyncio
    async def test_validate_data_types_date_range_violations(self):
        """Test date range violations."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                [{"column_name": "birth_date", "data_type": "date", "is_nullable": "YES", "column_default": None, "character_maximum_length": None, "numeric_precision": None, "numeric_scale": None}],
                {"violation_count": 1},  # Date range violations found
            ])

            result = await validate_data_types("users")

            assert result["results"]["validation_summary"]["total_violations"] == 1
            column_validation = result["results"]["column_validations"][0]
            assert column_validation["violations"][0]["violation_type"] == "suspicious_date_range"

    @pytest.mark.asyncio
    async def test_validate_data_types_invalid_table(self):
        """Test validate_data_types with invalid table name."""
        result = await validate_data_types("")
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"

    @pytest.mark.asyncio
    async def test_validate_data_types_column_not_found(self):
        """Test validate_data_types with non-existent column."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=[])

            result = await validate_data_types("users", "nonexistent")

            assert "error" in result
            assert result["error"]["code"] == "VALIDATION_ERROR"


class TestCheckDataIntegrity:
    """Test cases for check_data_integrity function."""

    @pytest.mark.asyncio
    async def test_check_data_integrity_basic_success(self):
        """Test basic data integrity check with no issues."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                {"table_name": "users", "table_type": "BASE TABLE"},  # Table exists
                {"total_rows": 1000, "distinct_rows": 1000},  # Basic stats
            ])

            # Mock the other validation functions
            with patch('src.mcp_postgres.tools.validation_tools.validate_constraints') as mock_constraints:
                mock_constraints.return_value = {
                    "results": {
                        "validation_summary": {
                            "violations_found": 0,
                            "total_constraints_checked": 5,
                            "constraint_types_checked": ["NOT NULL", "PRIMARY KEY"]
                        }
                    }
                }

                with patch('src.mcp_postgres.tools.validation_tools.validate_data_types') as mock_datatypes:
                    mock_datatypes.return_value = {
                        "results": {
                            "validation_summary": {
                                "total_violations": 0,
                                "total_columns_checked": 3,
                                "columns_with_violations": 0
                            }
                        }
                    }

                    result = await check_data_integrity("users", comprehensive=False)

                    assert result["analysis_type"] == "data_integrity_check"
                    assert result["table_name"] == "users"
                    assert result["results"]["integrity_summary"]["overall_status"] == "PASS"
                    assert result["results"]["integrity_summary"]["total_issues"] == 0

    @pytest.mark.asyncio
    async def test_check_data_integrity_comprehensive_success(self):
        """Test comprehensive data integrity check."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                {"table_name": "users", "table_type": "BASE TABLE"},  # Table exists
                {"total_rows": 1000, "distinct_rows": 1000},  # Basic stats
                {"fk_count": 1},  # Has foreign keys
                [{"column_name": "id", "data_type": "integer"}, {"column_name": "name", "data_type": "varchar"}],  # Columns for distribution
                {"total_count": 1000, "non_null_count": 1000},  # id column stats
                {"distinct_count": 1000},  # id distinct count
                {"total_count": 1000, "non_null_count": 950},  # name column stats
                {"distinct_count": 800},  # name distinct count
                {  # Health stats
                    "schemaname": "public",
                    "tablename": "users",
                    "inserts": 1000,
                    "updates": 50,
                    "deletes": 10,
                    "live_tuples": 990,
                    "dead_tuples": 10,
                    "last_vacuum": None,
                    "last_autovacuum": "2024-01-01 10:00:00",
                    "last_analyze": None,
                    "last_autoanalyze": "2024-01-01 11:00:00"
                }
            ])

            # Mock the other validation functions
            with patch('src.mcp_postgres.tools.validation_tools.validate_constraints') as mock_constraints:
                mock_constraints.return_value = {
                    "results": {
                        "validation_summary": {
                            "violations_found": 0,
                            "total_constraints_checked": 5,
                            "constraint_types_checked": ["NOT NULL", "PRIMARY KEY"]
                        }
                    }
                }

                with patch('src.mcp_postgres.tools.validation_tools.validate_data_types') as mock_datatypes:
                    mock_datatypes.return_value = {
                        "results": {
                            "validation_summary": {
                                "total_violations": 0,
                                "total_columns_checked": 3,
                                "columns_with_violations": 0
                            }
                        }
                    }

                    result = await check_data_integrity("users", comprehensive=True)

                    assert result["analysis_type"] == "data_integrity_check"
                    assert result["results"]["check_type"] == "comprehensive"
                    assert "data_distribution" in result["results"]["detailed_results"]
                    assert "table_health" in result["results"]["detailed_results"]

    @pytest.mark.asyncio
    async def test_check_data_integrity_with_critical_issues(self):
        """Test data integrity check with critical issues."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                {"table_name": "users", "table_type": "BASE TABLE"},  # Table exists
                {"total_rows": 1000, "distinct_rows": 1000},  # Basic stats
            ])

            # Mock constraint validation with violations
            with patch('src.mcp_postgres.tools.validation_tools.validate_constraints') as mock_constraints:
                mock_constraints.return_value = {
                    "results": {
                        "validation_summary": {
                            "violations_found": 3,
                            "total_constraints_checked": 5,
                            "constraint_types_checked": ["NOT NULL", "PRIMARY KEY"]
                        }
                    }
                }

                with patch('src.mcp_postgres.tools.validation_tools.validate_data_types') as mock_datatypes:
                    mock_datatypes.return_value = {
                        "results": {
                            "validation_summary": {
                                "total_violations": 2,
                                "total_columns_checked": 3,
                                "columns_with_violations": 1
                            }
                        }
                    }

                    result = await check_data_integrity("users", comprehensive=False)

                    assert result["results"]["integrity_summary"]["overall_status"] == "CRITICAL"
                    assert result["results"]["integrity_summary"]["critical_issues"] == 3
                    assert result["results"]["integrity_summary"]["warning_issues"] == 2
                    assert result["results"]["integrity_summary"]["total_issues"] == 5

    @pytest.mark.asyncio
    async def test_check_data_integrity_with_distribution_issues(self):
        """Test data integrity check with data distribution issues."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                {"table_name": "users", "table_type": "BASE TABLE"},  # Table exists
                {"total_rows": 1000, "distinct_rows": 1000},  # Basic stats
                {"fk_count": 0},  # No foreign keys
                [{"column_name": "status", "data_type": "varchar"}],  # Columns for distribution
                {"total_count": 1000, "non_null_count": 0},  # All NULL values
            ])

            # Mock other validations as passing
            with patch('src.mcp_postgres.tools.validation_tools.validate_constraints') as mock_constraints:
                mock_constraints.return_value = {
                    "results": {
                        "validation_summary": {
                            "violations_found": 0,
                            "total_constraints_checked": 2,
                            "constraint_types_checked": ["NOT NULL"]
                        }
                    }
                }

                with patch('src.mcp_postgres.tools.validation_tools.validate_data_types') as mock_datatypes:
                    mock_datatypes.return_value = {
                        "results": {
                            "validation_summary": {
                                "total_violations": 0,
                                "total_columns_checked": 1,
                                "columns_with_violations": 0
                            }
                        }
                    }

                    result = await check_data_integrity("users", comprehensive=True)

                    assert result["results"]["integrity_summary"]["overall_status"] == "INFO"
                    assert result["results"]["integrity_summary"]["info_issues"] == 1
                    distribution_result = result["results"]["detailed_results"]["data_distribution"]
                    assert distribution_result["status"] == "WARNING"
                    assert len(distribution_result["issues"]) == 1
                    assert distribution_result["issues"][0]["issue"] == "all_null_values"

    @pytest.mark.asyncio
    async def test_check_data_integrity_invalid_table(self):
        """Test check_data_integrity with invalid table name."""
        result = await check_data_integrity("")
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"

    @pytest.mark.asyncio
    async def test_check_data_integrity_table_not_found(self):
        """Test check_data_integrity with non-existent table."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=None)

            result = await check_data_integrity("nonexistent")

            assert "error" in result
            assert result["error"]["code"] == "VALIDATION_ERROR"

    @pytest.mark.asyncio
    async def test_check_data_integrity_constraint_validation_error(self):
        """Test data integrity check when constraint validation fails."""
        with patch('src.mcp_postgres.tools.validation_tools.connection_manager') as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[
                {"table_name": "users", "table_type": "BASE TABLE"},  # Table exists
                {"total_rows": 1000, "distinct_rows": 1000},  # Basic stats
            ])

            # Mock constraint validation with error
            with patch('src.mcp_postgres.tools.validation_tools.validate_constraints') as mock_constraints:
                mock_constraints.return_value = {
                    "error": {
                        "code": "CONSTRAINT_VALIDATION_ERROR",
                        "message": "Database connection failed"
                    }
                }

                with patch('src.mcp_postgres.tools.validation_tools.validate_data_types') as mock_datatypes:
                    mock_datatypes.return_value = {
                        "results": {
                            "validation_summary": {
                                "total_violations": 0,
                                "total_columns_checked": 3,
                                "columns_with_violations": 0
                            }
                        }
                    }

                    result = await check_data_integrity("users", comprehensive=False)

                    assert result["results"]["integrity_summary"]["overall_status"] == "CRITICAL"
                    assert result["results"]["integrity_summary"]["critical_issues"] == 1
                    constraint_result = result["results"]["detailed_results"]["constraint_validation"]
                    assert constraint_result["status"] == "ERROR"
                    assert "Database connection failed" in constraint_result["error"]
