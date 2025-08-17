"""Manual testing script for error handling and logging functionality.

This script can be run to manually test various error scenarios
and verify that logging and error handling work correctly.
"""

import asyncio
import sys
from pathlib import Path


# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from mcp_postgres.utils.error_handler import error_handler, handle_tool_errors
from mcp_postgres.utils.exceptions import (
    ConnectionError,
    QueryError,
    SecurityError,
    ValidationError,
)
from mcp_postgres.utils.logging import LogContext, get_logger, setup_enhanced_logging


def setup_test_environment():
    """Setup test environment with enhanced logging."""
    # Set environment variables for testing
    import os
    os.environ["LOG_LEVEL"] = "DEBUG"
    os.environ["ENABLE_STRUCTURED_LOGGING"] = "true"
    os.environ["LOG_QUERY_PARAMETERS"] = "true"

    # Setup enhanced logging
    setup_enhanced_logging()

    print("Enhanced logging configured for testing")


@handle_tool_errors(tool_name="test_tool", operation="test_operation")
async def test_function_with_error():
    """Test function that raises an error."""
    raise ValidationError("This is a test validation error", field_name="test_field")


@handle_tool_errors(tool_name="test_tool", operation="successful_operation")
async def test_function_success():
    """Test function that succeeds."""
    return {"success": True, "data": "Test successful"}


async def test_error_handling():
    """Test various error handling scenarios."""
    logger = get_logger(__name__)

    print("\n=== Testing Error Handling ===")

    # Test 1: Validation Error
    print("\n1. Testing validation error...")
    result = await test_function_with_error()
    print(f"Result: {result}")

    # Test 2: Successful operation
    print("\n2. Testing successful operation...")
    result = await test_function_success()
    print(f"Result: {result}")

    # Test 3: Direct error handling
    print("\n3. Testing direct error handling...")
    try:
        raise ConnectionError("Test connection error", {"host": "localhost"})
    except Exception as e:
        result = error_handler.handle_error(
            error=e,
            tool_name="manual_test",
            operation="connection_test",
            parameters={"host": "localhost", "port": 5432}
        )
        print(f"Handled error result: {result}")

    # Test 4: Multiple errors for statistics
    print("\n4. Testing error statistics...")
    for i in range(5):
        try:
            if i % 2 == 0:
                raise ValidationError(f"Validation error {i}")
            else:
                raise SecurityError(f"Security error {i}")
        except Exception as e:
            error_handler.handle_error(e, "stats_test", f"operation_{i}")

    stats = error_handler.get_error_statistics()
    print(f"Error statistics: {stats}")

    recent_errors = error_handler.get_recent_errors(3)
    print(f"Recent errors (last 3): {recent_errors}")


def test_logging_features():
    """Test various logging features."""
    logger = get_logger(__name__)

    print("\n=== Testing Logging Features ===")

    # Test 1: Basic structured logging
    print("\n1. Testing basic structured logging...")
    context = LogContext(
        tool_name="test_logging",
        operation="basic_test",
        user_id="test_user"
    )

    logger.info("This is a test info message", context, {"extra_data": "test_value"})
    logger.warning("This is a test warning", context)
    logger.error("This is a test error", context, exc_info=False)

    # Test 2: Performance logging
    print("\n2. Testing performance logging...")
    from mcp_postgres.utils.logging import PerformanceMetrics

    metrics = PerformanceMetrics(
        execution_time_ms=1500.0,
        query_count=3,
        result_size=100
    )

    logger.log_performance("test_operation", metrics, context)

    # Test 3: Query logging
    print("\n3. Testing query logging...")
    logger.log_query(
        query="SELECT * FROM users WHERE id = $1",
        parameters=["test_user_id"],
        execution_time_ms=250.5,
        result_count=1,
        context=context
    )

    # Test 4: Error logging
    print("\n4. Testing error logging...")
    try:
        raise ValueError("Test error for logging")
    except Exception as e:
        logger.log_error(
            error=e,
            operation="test_error_logging",
            context=context,
            additional_data={"test_context": "manual_testing"}
        )

    # Test 5: Context manager
    print("\n5. Testing log context manager...")
    with logger.log_context(context) as ctx:
        logger.info("Message within context manager")
        logger.debug("Debug message within context")


def test_exception_hierarchy():
    """Test the exception hierarchy and conversion."""
    print("\n=== Testing Exception Hierarchy ===")

    # Test various exception types
    exceptions_to_test = [
        ValidationError("Test validation", field_name="test"),
        SecurityError("Test security", security_rule="test_rule"),
        ConnectionError("Test connection"),
        QueryError("Test query", query="SELECT 1"),
    ]

    for i, exc in enumerate(exceptions_to_test, 1):
        print(f"\n{i}. Testing {type(exc).__name__}...")
        error_dict = exc.to_dict()
        print(f"   Error dict: {error_dict}")

        # Test error handler conversion
        result = error_handler.handle_error(exc, "test_tool", "test_operation")
        print(f"   Handled result: {result['error']['code']}")


async def main():
    """Main test function."""
    print("Starting manual error handling and logging tests...")

    # Setup test environment
    setup_test_environment()

    # Run tests
    await test_error_handling()
    test_logging_features()
    test_exception_hierarchy()

    print("\n=== Final Statistics ===")
    final_stats = error_handler.get_error_statistics()
    print(f"Total errors handled: {final_stats['total_errors']}")
    print(f"Error types: {list(final_stats['error_counts_by_type'].keys())}")
    print(f"Success rate: {final_stats['success_rate']:.2%}")

    print("\nManual testing completed!")


if __name__ == "__main__":
    asyncio.run(main())
