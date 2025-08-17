"""Integration tests for error handling scenarios."""

import asyncio
from unittest.mock import patch

import pytest

from src.mcp_postgres.core.connection import connection_manager
from src.mcp_postgres.tools.query_tools import (
    execute_query,
    execute_raw_query,
    execute_transaction,
)
from src.mcp_postgres.utils.error_handler import error_handler
from src.mcp_postgres.utils.exceptions import (
    ConnectionError,
    SecurityError,
)


class TestDatabaseErrorScenarios:
    """Test database-related error scenarios."""

    @pytest.fixture(autouse=True)
    async def setup_and_teardown(self):
        """Setup and teardown for each test."""
        # Clear error history before each test
        error_handler.clear_error_history()
        yield
        # Cleanup after test
        try:
            await connection_manager.close()
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_connection_failure_scenario(self):
        """Test behavior when database connection fails."""
        # Mock connection manager to simulate connection failure
        with patch.object(connection_manager, 'execute_query') as mock_execute:
            mock_execute.side_effect = ConnectionError("Connection refused")

            result = await execute_query("SELECT 1")

            assert result["success"] is False
            assert result["error"]["code"] == "CONNECTION_ERROR"

            # Check error statistics
            stats = error_handler.get_error_statistics()
            assert stats["total_errors"] > 0

    @pytest.mark.asyncio
    async def test_query_timeout_scenario(self):
        """Test behavior when query times out."""
        with patch.object(connection_manager, 'execute_query') as mock_execute:
            mock_execute.side_effect = TimeoutError("Query timed out")

            result = await execute_query("SELECT pg_sleep(100)")

            assert result["success"] is False
            assert "error" in result

    @pytest.mark.asyncio
    async def test_invalid_sql_syntax_scenario(self):
        """Test behavior with invalid SQL syntax."""
        with patch.object(connection_manager, 'execute_query') as mock_execute:
            # Simulate PostgreSQL syntax error
            class MockPGError(Exception):
                def __init__(self, message):
                    super().__init__(message)
                    self.sqlstate = "42601"  # Syntax error code

            mock_execute.side_effect = MockPGError("syntax error at or near 'SELCT'")

            result = await execute_query("SELCT * FROM users")

            assert result["success"] is False
            assert "error" in result

    @pytest.mark.asyncio
    async def test_table_not_found_scenario(self):
        """Test behavior when table doesn't exist."""
        with patch.object(connection_manager, 'execute_query') as mock_execute:
            class MockPGError(Exception):
                def __init__(self, message):
                    super().__init__(message)
                    self.sqlstate = "42P01"  # Undefined table

            mock_execute.side_effect = MockPGError("relation 'nonexistent_table' does not exist")

            result = await execute_query("SELECT * FROM nonexistent_table")

            assert result["success"] is False
            assert "error" in result

    @pytest.mark.asyncio
    async def test_permission_denied_scenario(self):
        """Test behavior when permission is denied."""
        with patch.object(connection_manager, 'execute_query') as mock_execute:
            mock_execute.side_effect = PermissionError("permission denied for table users")

            result = await execute_query("SELECT * FROM users")

            assert result["success"] is False
            assert result["error"]["code"] == "SECURITY_ERROR"

    @pytest.mark.asyncio
    async def test_constraint_violation_scenario(self):
        """Test behavior with constraint violations."""
        with patch.object(connection_manager, 'execute_query') as mock_execute:
            class MockPGError(Exception):
                def __init__(self, message):
                    super().__init__(message)
                    self.sqlstate = "23505"  # Unique violation

            mock_execute.side_effect = MockPGError("duplicate key value violates unique constraint")

            result = await execute_query("INSERT INTO users (email) VALUES ('test@example.com')")

            assert result["success"] is False
            assert "error" in result


class TestSecurityErrorScenarios:
    """Test security-related error scenarios."""

    @pytest.mark.asyncio
    async def test_sql_injection_detection(self):
        """Test SQL injection detection."""
        # Test various SQL injection patterns
        malicious_queries = [
            "SELECT * FROM users WHERE id = 1; DROP TABLE users; --",
            "SELECT * FROM users WHERE name = 'admin' OR '1'='1'",
            "SELECT * FROM users; DELETE FROM users WHERE 1=1; --",
        ]

        for query in malicious_queries:
            result = await execute_query(query)

            # Should be blocked by security validation
            assert result["success"] is False
            # Could be either security error or validation error depending on detection method

    @pytest.mark.asyncio
    async def test_dangerous_operations_blocked(self):
        """Test that dangerous operations are blocked."""
        dangerous_queries = [
            "DROP TABLE users",
            "TRUNCATE TABLE users",
            "ALTER TABLE users DROP COLUMN email",
            "DELETE FROM users",  # Without WHERE clause
        ]

        for query in dangerous_queries:
            result = await execute_query(query)

            assert result["success"] is False
            # Should be blocked by security validation

    @pytest.mark.asyncio
    async def test_raw_query_security_warnings(self):
        """Test that raw queries generate security warnings."""
        with patch.object(connection_manager, 'execute_raw_query') as mock_execute:
            mock_execute.return_value = [{"result": "test"}]

            result = await execute_raw_query("SELECT 1")

            # Should succeed but include security warning
            if result["success"]:
                assert "security_warning" in result["data"]


class TestValidationErrorScenarios:
    """Test input validation error scenarios."""

    @pytest.mark.asyncio
    async def test_empty_query_validation(self):
        """Test validation of empty queries."""
        empty_queries = ["", "   ", "\n\t  ", None]

        for query in empty_queries:
            if query is None:
                # None should cause TypeError before reaching our validation
                with pytest.raises(TypeError):
                    await execute_query(query)
            else:
                result = await execute_query(query)
                assert result["success"] is False
                assert result["error"]["code"] == "VALIDATION_ERROR"

    @pytest.mark.asyncio
    async def test_invalid_fetch_mode_validation(self):
        """Test validation of invalid fetch modes."""
        invalid_modes = ["invalid", "ALL", "One", 123, None]

        for mode in invalid_modes:
            result = await execute_query("SELECT 1", fetch_mode=mode)
            assert result["success"] is False
            assert result["error"]["code"] == "VALIDATION_ERROR"

    @pytest.mark.asyncio
    async def test_invalid_transaction_format(self):
        """Test validation of invalid transaction formats."""
        invalid_transactions = [
            [],  # Empty list
            [{"invalid": "format"}],  # Missing query
            [{"query": ""}],  # Empty query
            [{"query": "SELECT 1", "fetch_mode": "invalid"}],  # Invalid fetch mode
            "not_a_list",  # Not a list
            None,  # None
        ]

        for transaction in invalid_transactions:
            if transaction is None or isinstance(transaction, str):
                with pytest.raises(TypeError):
                    await execute_transaction(transaction)
            else:
                result = await execute_transaction(transaction)
                assert result["success"] is False
                assert result["error"]["code"] == "VALIDATION_ERROR"


class TestErrorRecoveryScenarios:
    """Test error recovery and resilience scenarios."""

    @pytest.mark.asyncio
    async def test_partial_transaction_failure(self):
        """Test behavior when part of a transaction fails."""
        with patch.object(connection_manager, 'execute_transaction') as mock_execute:
            # Simulate partial failure
            mock_execute.side_effect = Exception("Transaction failed at query 2")

            queries = [
                {"query": "SELECT 1", "fetch_mode": "val"},
                {"query": "SELECT 2", "fetch_mode": "val"},
                {"query": "SELECT 3", "fetch_mode": "val"},
            ]

            result = await execute_transaction(queries)

            assert result["success"] is False
            assert "error" in result

    @pytest.mark.asyncio
    async def test_connection_pool_exhaustion(self):
        """Test behavior when connection pool is exhausted."""
        with patch.object(connection_manager, 'execute_query') as mock_execute:
            mock_execute.side_effect = ConnectionError("Connection pool exhausted")

            # Try multiple concurrent queries
            tasks = [execute_query("SELECT 1") for _ in range(10)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # All should fail gracefully
            for result in results:
                if isinstance(result, dict):
                    assert result["success"] is False
                    assert result["error"]["code"] == "CONNECTION_ERROR"

    @pytest.mark.asyncio
    async def test_memory_pressure_scenario(self):
        """Test behavior under memory pressure."""
        with patch.object(connection_manager, 'execute_query') as mock_execute:
            mock_execute.side_effect = MemoryError("Out of memory")

            result = await execute_query("SELECT * FROM large_table")

            assert result["success"] is False
            assert "error" in result

    @pytest.mark.asyncio
    async def test_network_interruption_scenario(self):
        """Test behavior when network is interrupted."""
        with patch.object(connection_manager, 'execute_query') as mock_execute:
            mock_execute.side_effect = OSError("Network is unreachable")

            result = await execute_query("SELECT 1")

            assert result["success"] is False
            assert result["error"]["code"] == "CONNECTION_ERROR"


class TestErrorLoggingScenarios:
    """Test error logging and monitoring scenarios."""

    @pytest.mark.asyncio
    async def test_error_statistics_accumulation(self):
        """Test that error statistics accumulate correctly."""
        # Clear existing stats
        error_handler.clear_error_history()

        # Generate various types of errors
        with patch.object(connection_manager, 'execute_query') as mock_execute:
            # Connection errors
            mock_execute.side_effect = ConnectionError("Connection failed")
            await execute_query("SELECT 1")
            await execute_query("SELECT 2")

            # Validation errors
            await execute_query("")  # Empty query
            await execute_query("", fetch_mode="invalid")

            # Security errors (if not caught by validation first)
            mock_execute.side_effect = SecurityError("Dangerous query")
            await execute_query("DROP TABLE test")

        stats = error_handler.get_error_statistics()
        assert stats["total_errors"] >= 3  # At least the errors we generated
        assert stats["failed_executions"] >= 3

    @pytest.mark.asyncio
    async def test_error_context_preservation(self):
        """Test that error context is preserved in logs."""
        with patch.object(connection_manager, 'execute_query') as mock_execute:
            mock_execute.side_effect = Exception("Test error with context")

            result = await execute_query("SELECT 1", parameters=["test_param"])

            assert result["success"] is False

            # Check that recent errors contain context
            recent_errors = error_handler.get_recent_errors(1)
            assert len(recent_errors) > 0
            assert recent_errors[0]["tool_name"] == "execute_query"

    @pytest.mark.asyncio
    async def test_concurrent_error_handling(self):
        """Test error handling under concurrent load."""
        with patch.object(connection_manager, 'execute_query') as mock_execute:
            mock_execute.side_effect = Exception("Concurrent error")

            # Run multiple queries concurrently
            tasks = [
                execute_query(f"SELECT {i}")
                for i in range(20)
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # All should fail gracefully
            failed_count = sum(
                1 for result in results
                if isinstance(result, dict) and not result.get("success", True)
            )

            assert failed_count == 20

            # Error statistics should be consistent
            stats = error_handler.get_error_statistics()
            assert stats["total_errors"] >= 20


if __name__ == "__main__":
    pytest.main([__file__])
