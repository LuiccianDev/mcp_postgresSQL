"""Tests for comprehensive error handling and logging functionality."""

import asyncio
import json
from unittest.mock import patch

import pytest

from src.mcp_postgres.tools.query_tools import (
    execute_query,
    execute_transaction,
)
from src.mcp_postgres.utils.error_handler import (
    ErrorHandler,
    handle_tool_errors,
    log_and_reraise,
    validate_and_handle_errors,
)
from src.mcp_postgres.utils.exceptions import (
    ConnectionError,
    ValidationError,
    handle_postgres_error,
)
from src.mcp_postgres.utils.logging import (
    LogContext,
    LoggerFactory,
    PerformanceMetrics,
    get_logger,
)


class TestErrorHandler:
    """Test cases for ErrorHandler class."""

    def setup_method(self):
        """Setup test environment."""
        self.error_handler = ErrorHandler()

    def test_handle_error_basic(self):
        """Test basic error handling."""
        error = ValueError("Test error")
        result = self.error_handler.handle_error(
            error=error,
            tool_name="test_tool",
            operation="test_operation"
        )

        assert result["success"] is False
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "Test error" in result["error"]["message"]

    def test_handle_error_with_context(self):
        """Test error handling with log context."""
        error = ConnectionError("Database connection failed")
        context = LogContext(tool_name="test_tool", operation="connect")

        result = self.error_handler.handle_error(
            error=error,
            tool_name="test_tool",
            operation="connect",
            context=context,
            parameters={"host": "localhost"}
        )

        assert result["success"] is False
        assert result["error"]["code"] == "CONNECTION_ERROR"

    def test_error_statistics(self):
        """Test error statistics tracking."""
        # Generate some errors
        self.error_handler.handle_error(ValueError("Error 1"), "tool1")
        self.error_handler.handle_error(ValueError("Error 2"), "tool1")
        self.error_handler.handle_error(ConnectionError("Error 3"), "tool2")

        stats = self.error_handler.get_error_statistics()

        assert stats["total_errors"] == 3
        assert stats["error_counts_by_type"]["ValidationError"] == 2
        assert stats["error_counts_by_type"]["ConnectionError"] == 1
        assert stats["most_common_error"] == "ValidationError"

    def test_recent_errors(self):
        """Test recent errors tracking."""
        for i in range(15):  # More than the default limit
            self.error_handler.handle_error(ValueError(f"Error {i}"), f"tool{i}")

        recent = self.error_handler.get_recent_errors(5)
        assert len(recent) == 5
        assert recent[-1]["error_message"] == "Error 14"

    def test_clear_error_history(self):
        """Test clearing error history."""
        self.error_handler.handle_error(ValueError("Test"), "tool")
        self.error_handler.clear_error_history()

        stats = self.error_handler.get_error_statistics()
        assert stats["total_errors"] == 0
        assert len(self.error_handler.get_recent_errors()) == 0


class TestErrorDecorators:
    """Test cases for error handling decorators."""

    def test_handle_tool_errors_async(self):
        """Test handle_tool_errors decorator with async function."""
        @handle_tool_errors(tool_name="test_tool", operation="test_op")
        async def failing_function():
            raise ValueError("Test error")

        result = asyncio.run(failing_function())

        assert result["success"] is False
        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"

    def test_handle_tool_errors_sync(self):
        """Test handle_tool_errors decorator with sync function."""
        @handle_tool_errors(tool_name="test_tool", operation="test_op")
        def failing_function():
            raise ValueError("Test error")

        result = failing_function()

        assert result["success"] is False
        assert "error" in result

    def test_handle_tool_errors_success(self):
        """Test handle_tool_errors decorator with successful function."""
        @handle_tool_errors(tool_name="test_tool", operation="test_op")
        async def successful_function():
            return {"success": True, "data": "test"}

        result = asyncio.run(successful_function())

        assert result["success"] is True
        assert result["data"] == "test"

    def test_validate_and_handle_errors(self):
        """Test validate_and_handle_errors decorator."""
        def validation_func(kwargs):
            return kwargs.get("valid", False)

        @validate_and_handle_errors(validation_func, "Validation failed")
        def test_function(valid=False):
            return {"success": True}

        # Test validation failure
        with pytest.raises(ValidationError):
            test_function(valid=False)

        # Test validation success
        result = test_function(valid=True)
        assert result["success"] is True

    def test_log_and_reraise(self):
        """Test log_and_reraise decorator."""
        @log_and_reraise("Function failed", log_level="error")
        def failing_function():
            raise ValueError("Test error")

        with pytest.raises(ValueError):
            failing_function()


class TestStructuredLogging:
    """Test cases for structured logging functionality."""

    def setup_method(self):
        """Setup test environment."""
        self.logger = get_logger("test_logger")

    def test_log_context_creation(self):
        """Test log context creation."""
        context = LogContext(
            tool_name="test_tool",
            operation="test_op",
            user_id="user123"
        )

        assert context.tool_name == "test_tool"
        assert context.operation == "test_op"
        assert context.user_id == "user123"
        assert context.request_id is not None

    def test_performance_metrics(self):
        """Test performance metrics creation."""
        metrics = PerformanceMetrics(
            execution_time_ms=1500.0,
            query_count=3,
            result_size=100
        )

        metrics_dict = metrics.to_dict()
        assert metrics_dict["execution_time_ms"] == 1500.0
        assert metrics_dict["query_count"] == 3
        assert metrics_dict["result_size"] == 100

    @patch('src.mcp_postgres.config.settings.server_config')
    def test_structured_logger_formatting(self, mock_config):
        """Test structured logger message formatting."""
        mock_config.enable_structured_logging = True

        context = LogContext(tool_name="test_tool")

        with patch.object(self.logger.logger, 'info') as mock_info:
            self.logger.info("Test message", context, {"extra": "data"})

            # Verify that structured logging was called
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]

            # Should be JSON formatted
            try:
                parsed = json.loads(call_args)
                assert parsed["message"] == "Test message"
                assert parsed["tool_name"] == "test_tool"
                assert parsed["data"]["extra"] == "data"
            except json.JSONDecodeError:
                pytest.fail("Expected JSON formatted log message")

    def test_logger_factory(self):
        """Test logger factory functionality."""
        logger1 = LoggerFactory.get_logger("test1")
        logger2 = LoggerFactory.get_logger("test1")  # Same name
        logger3 = LoggerFactory.get_logger("test2")  # Different name

        assert logger1 is logger2  # Should be same instance
        assert logger1 is not logger3  # Should be different instances

    def test_log_context_manager(self):
        """Test log context manager."""
        context = LogContext(tool_name="test_tool")

        with self.logger.log_context(context) as ctx:
            assert ctx is context
            assert len(self.logger._context_stack) == 1

        assert len(self.logger._context_stack) == 0


class TestExceptionHandling:
    """Test cases for exception handling and conversion."""

    def test_handle_postgres_error_connection(self):
        """Test PostgreSQL connection error handling."""
        class MockPGError(Exception):
            def __init__(self, message, sqlstate=None):
                super().__init__(message)
                self.sqlstate = sqlstate

        pg_error = MockPGError("connection failed", "08001")
        mcp_error = handle_postgres_error(pg_error)

        assert isinstance(mcp_error, ConnectionError)
        assert "connection failed" in str(mcp_error)

    def test_handle_postgres_error_syntax(self):
        """Test PostgreSQL syntax error handling."""
        class MockPGError(Exception):
            def __init__(self, message, sqlstate=None):
                super().__init__(message)
                self.sqlstate = sqlstate

        pg_error = MockPGError("syntax error", "42601")
        mcp_error = handle_postgres_error(pg_error)

        assert mcp_error.error_code == "QUERY_SYNTAX_ERROR"

    def test_mcp_postgres_error_to_dict(self):
        """Test MCPPostgresError to_dict conversion."""
        error = ValidationError(
            message="Invalid input",
            field_name="test_field",
            field_value="invalid_value"
        )

        error_dict = error.to_dict()

        assert error_dict["code"] == "VALIDATION_ERROR"
        assert error_dict["message"] == "Invalid input"
        assert error_dict["details"]["field_name"] == "test_field"


class TestToolErrorHandling:
    """Test cases for tool-specific error handling."""

    @patch('src.mcp_postgres.core.connection.connection_manager')
    @patch('src.mcp_postgres.core.security.validate_query_permissions')
    @patch('src.mcp_postgres.core.security.sanitize_parameters')
    def test_execute_query_validation_error(self, mock_sanitize, mock_validate, mock_conn):
        """Test execute_query with validation error."""
        result = asyncio.run(execute_query(""))  # Empty query

        assert result["success"] is False
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "empty" in result["error"]["message"].lower()

    @patch('src.mcp_postgres.core.connection.connection_manager')
    @patch('src.mcp_postgres.core.security.validate_query_permissions')
    @patch('src.mcp_postgres.core.security.sanitize_parameters')
    def test_execute_query_security_error(self, mock_sanitize, mock_validate, mock_conn):
        """Test execute_query with security error."""
        mock_validate.return_value = (False, "Dangerous query detected")

        result = asyncio.run(execute_query("DROP TABLE users"))

        assert result["success"] is False
        assert result["error"]["code"] == "SECURITY_ERROR"

    @patch('src.mcp_postgres.core.connection.connection_manager')
    @patch('src.mcp_postgres.core.security.validate_query_permissions')
    @patch('src.mcp_postgres.core.security.sanitize_parameters')
    def test_execute_query_database_error(self, mock_sanitize, mock_validate, mock_conn):
        """Test execute_query with database error."""
        mock_validate.return_value = (True, "")
        mock_sanitize.return_value = []
        mock_conn.execute_query.side_effect = Exception("Database connection failed")

        result = asyncio.run(execute_query("SELECT 1"))

        assert result["success"] is False
        assert "error" in result

    def test_execute_transaction_empty_queries(self):
        """Test execute_transaction with empty queries list."""
        result = asyncio.run(execute_transaction([]))

        assert result["success"] is False
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "empty" in result["error"]["message"].lower()

    def test_execute_transaction_invalid_query_format(self):
        """Test execute_transaction with invalid query format."""
        result = asyncio.run(execute_transaction([
            "invalid_query_format"  # Should be dict
        ]))

        assert result["success"] is False
        assert result["error"]["code"] == "VALIDATION_ERROR"


class TestErrorRecovery:
    """Test cases for error recovery mechanisms."""

    def test_error_handler_resilience(self):
        """Test error handler resilience to various error types."""
        handler = ErrorHandler()

        # Test with various error types
        errors = [
            ValueError("Value error"),
            ConnectionError("Connection error"),
            KeyError("Key error"),
            TypeError("Type error"),
            RuntimeError("Runtime error"),
        ]

        for error in errors:
            result = handler.handle_error(error, "test_tool")
            assert result["success"] is False
            assert "error" in result

    def test_logging_with_serialization_errors(self):
        """Test logging behavior with non-serializable data."""
        logger = get_logger("test")

        # Create context with non-serializable data
        context = LogContext(tool_name="test")
        context.metadata = {"non_serializable": lambda x: x}  # Function is not JSON serializable

        # Should not raise exception
        try:
            logger.info("Test message", context)
        except Exception as e:
            pytest.fail(f"Logging should handle serialization errors gracefully: {e}")

    @patch('src.mcp_postgres.config.settings.server_config')
    def test_logging_fallback_when_structured_disabled(self, mock_config):
        """Test logging fallback when structured logging is disabled."""
        mock_config.enable_structured_logging = False

        logger = get_logger("test")
        context = LogContext(tool_name="test")

        with patch.object(logger.logger, 'info') as mock_info:
            logger.info("Test message", context)

            # Should use simple message format
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert call_args == "Test message"


if __name__ == "__main__":
    pytest.main([__file__])
