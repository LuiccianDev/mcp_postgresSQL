"""Comprehensive error handling utilities for MCP Postgres server.

This module provides standardized error handling, response formatting,
and error recovery mechanisms for all MCP tools.
"""

import inspect
import traceback
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

from ..utils.exceptions import (
    ConnectionError,
    MCPPostgresError,
    QueryError,
    SecurityError,
    ToolError,
    ValidationError,
    handle_postgres_error,
)
from ..utils.formatters import format_error_response
from ..utils.logging import LogContext, get_logger


logger = get_logger(__name__)

# Type variable for decorated functions
F = TypeVar("F", bound=Callable[..., Any])

class ErrorHandler:
    """Centralized error handling for MCP tools."""

    def __init__(self) -> None:
        """Initialize error handler."""
        self._error_counts: dict[str, int] = {}
        self._recent_errors: list[dict[str, Any]] = []
        self._max_recent_errors = 100

    def handle_error(
        self,
        error: Exception,
        tool_name: str | None = None,
        operation: str | None = None,
        context: LogContext | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Handle and format errors consistently.

        Args:
            error: Exception that occurred
            tool_name: Name of the tool where error occurred
            operation: Operation that failed
            context: Log context
            parameters: Tool parameters that caused the error

        Returns:
            Formatted error response dictionary
        """
        # Track error statistics
        error_type = type(error).__name__
        self._error_counts[error_type] = self._error_counts.get(error_type, 0) + 1

        # Create error record
        error_record = {
            "error_type": error_type,
            "error_message": str(error),
            "tool_name": tool_name,
            "operation": operation,
            "timestamp": context.start_time if context else None,
            "parameters": parameters,
        }

        # Add to recent errors (with size limit)
        self._recent_errors.append(error_record)
        if len(self._recent_errors) > self._max_recent_errors:
            self._recent_errors.pop(0)

        # Log the error with context
        logger.log_error(
            error=error,
            operation=f"{tool_name}.{operation}"
            if tool_name and operation
            else operation,
            context=context,
            additional_data={"parameters": parameters} if parameters else None,
        )

        # Convert to MCP Postgres error if needed
        if isinstance(error, MCPPostgresError):
            mcp_error = error
        else:
            mcp_error = self._convert_to_mcp_error(error, tool_name, operation)

        # Format error response
        return format_error_response(
            error_code=mcp_error.error_code,
            message=mcp_error.message,
            details=mcp_error.details,
        )

    def _convert_to_mcp_error(
        self,
        error: Exception,
        tool_name: str | None = None,
        operation: str | None = None,
    ) -> MCPPostgresError:
        """Convert generic exceptions to MCP Postgres errors.

        Args:
            error: Original exception
            tool_name: Tool name for context
            operation: Operation for context

        Returns:
            MCPPostgresError instance
        """
        error_message = str(error)

        # Handle specific exception types
        if (
            isinstance(error, ConnectionRefusedError | OSError)
            and "connection" in error_message.lower()
        ):
            return ConnectionError(
                message="Database connection failed",
                details={
                    "original_error": error_message,
                    "tool_name": tool_name,
                    "operation": operation,
                },
            )

        elif isinstance(error, ValueError):
            return ValidationError(
                message=f"Invalid input: {error_message}",
                field_name="unknown",
                field_value=None,
                validation_rule="value_error",
            )

        elif isinstance(error, PermissionError):
            return SecurityError(
                message=f"Permission denied: {error_message}",
                security_rule="permission_check",
                attempted_operation=operation,
            )

        elif isinstance(error, TimeoutError):
            return QueryError(
                message=f"Operation timed out: {error_message}",
                query=None,
                parameters=None,
                details={"timeout_error": True, "operation": operation},
            )

        # Try to handle as PostgreSQL error
        try:
            return handle_postgres_error(error)
        except Exception:
            # Fallback to generic tool error
            return ToolError(
                message=f"Tool execution failed: {error_message}",
                tool_name=tool_name,
                tool_parameters=None,
            )

    def get_error_statistics(self) -> dict[str, Any]:
        """Get error statistics for monitoring.

        Returns:
            Dictionary containing error statistics
        """
        total_errors = sum(self._error_counts.values())

        return {
            "total_errors": total_errors,
            "error_counts_by_type": self._error_counts.copy(),
            "recent_errors_count": len(self._recent_errors),
            "most_common_error": max(self._error_counts.items(), key=lambda x: x[1])[0]
            if self._error_counts
            else None,
            "success_rate": 0.0,  # This would need to be calculated with successful operations
        }

    def get_recent_errors(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get recent errors for debugging.

        Args:
            limit: Maximum number of recent errors to return

        Returns:
            List of recent error records
        """
        return self._recent_errors[-limit:] if self._recent_errors else []

    def clear_error_history(self) -> None:
        """Clear error history and statistics."""
        self._error_counts.clear()
        self._recent_errors.clear()
        logger.info("Error history cleared")


# Global error handler instance
error_handler = ErrorHandler()

def handle_tool_errors(
    tool_name: str | None = None,
    operation: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator for consistent error handling in MCP tools."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:

        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            context = LogContext(
                tool_name=tool_name or func.__module__,
                operation=operation or func.__name__,
            )
            try:
                with logger.log_context(context):
                    result = await func(*args, **kwargs)
                    return result
            except Exception as e:
                return error_handler.handle_error(
                    error=e,
                    tool_name=tool_name or func.__module__,
                    operation=operation or func.__name__,
                    context=context,
                    parameters=kwargs,
                )

        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            context = LogContext(
                tool_name=tool_name or func.__module__,
                operation=operation or func.__name__,
            )
            try:
                with logger.log_context(context):
                    result = func(*args, **kwargs)
                    return result
            except Exception as e:
                return error_handler.handle_error(
                    error=e,
                    tool_name=tool_name or func.__module__,
                    operation=operation or func.__name__,
                    context=context,
                    parameters=kwargs,
                )

        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def validate_and_handle_errors(
    validation_func: Callable[[Any], bool],
    error_message: str,
    error_code: str = "VALIDATION_ERROR",
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator for input validation with error handling."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        import inspect
        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                if not validation_func(kwargs):
                    raise ValidationError(
                        message=error_message,
                        field_name="input_parameters",
                        validation_rule="custom_validation",
                    )
                return await func(*args, **kwargs)
            except ValidationError:
                raise
            except Exception as e:
                logger.error(f"Validation wrapper error in {func.__name__}: {e}")
                raise

        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                if not validation_func(kwargs):
                    raise ValidationError(
                        message=error_message,
                        field_name="input_parameters",
                        validation_rule="custom_validation",
                    )
                return func(*args, **kwargs)
            except ValidationError:
                raise
            except Exception as e:
                logger.error(f"Validation wrapper error in {func.__name__}: {e}")
                raise

        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def log_and_reraise(
    error_message: str,
    log_level: str = "error",
    include_traceback: bool = True,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to log errors and re-raise them.

    Args:
        error_message: Message to log
        log_level: Logging level to use
        include_traceback: Whether to include traceback in logs

    Returns:
        Decorated function with logging
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        import inspect

        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                log_func = getattr(logger, log_level, logger.error)

                extra_data = {
                    "function": func.__name__,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                }

                if include_traceback:
                    extra_data["traceback"] = traceback.format_exc()

                log_func(error_message, extra_data=extra_data)
                raise

        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                log_func = getattr(logger, log_level, logger.error)

                extra_data = {
                    "function": func.__name__,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                }

                if include_traceback:
                    extra_data["traceback"] = traceback.format_exc()

                log_func(error_message, extra_data=extra_data)
                raise

        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator
