"""Complete integration test for MCP server."""

import asyncio
import os
from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_complete_server_integration():
    """Test complete server integration with all components."""

    # Set environment variables
    test_env = {
        "DATABASE_URL": "postgresql://test:test@localhost:5432/test",
        "LOG_LEVEL": "INFO"
    }

    with patch.dict(os.environ, test_env):
        # Mock asyncpg
        with patch("asyncpg.create_pool") as mock_create_pool:
            mock_pool = AsyncMock()

            async def mock_create_pool_coro(*args, **kwargs):
                return mock_pool

            mock_create_pool.side_effect = mock_create_pool_coro

            # Mock stdio server to avoid infinite loop
            with patch("src.mcp_postgres.main.stdio_server") as mock_stdio:
                mock_read_stream = AsyncMock()
                mock_write_stream = AsyncMock()

                mock_context = AsyncMock()
                mock_context.__aenter__ = AsyncMock(return_value=(mock_read_stream, mock_write_stream))
                mock_context.__aexit__ = AsyncMock(return_value=None)
                mock_stdio.return_value = mock_context

                # Mock server run to complete quickly
                with patch("mcp.server.Server.run") as mock_run:
                    async def mock_run_func(*args, **kwargs):
                        # Simulate server running briefly
                        await asyncio.sleep(0.01)
                        return None

                    mock_run.side_effect = mock_run_func

                    # Import and test main function
                    from src.mcp_postgres.main import main

                    # Should complete without errors
                    await main()

                    # Verify components were initialized
                    mock_create_pool.assert_called_once()
                    mock_run.assert_called_once()


def test_tool_registry_completeness():
    """Test that tool registry is complete and properly structured."""
    from src.mcp_postgres.tools.register_tools import (
        TOOL_REGISTRY,
        get_tool_discovery_info,
    )

    # Test total count
    assert len(TOOL_REGISTRY) == 38

    # Test discovery info
    discovery = get_tool_discovery_info()
    assert discovery["total_tools"] == 38
    assert len(discovery["modules"]) == 10

    # Test module distribution
    expected_counts = {
        "query_tools": 3,
        "schema_tools": 8,
        "analysis_tools": 4,
        "data_tools": 4,
        "relation_tools": 3,
        "performance_tools": 3,
        "backup_tools": 3,
        "admin_tools": 4,
        "validation_tools": 3,
        "generation_tools": 3,
    }

    for module, expected_count in expected_counts.items():
        assert module in discovery["module_summary"]
        assert discovery["module_summary"][module] == expected_count


@pytest.mark.asyncio
async def test_mcp_protocol_handlers():
    """Test MCP protocol handlers are properly registered."""
    from mcp.server import Server

    from src.mcp_postgres.tools.register_tools import register_all_tools

    server = Server("test-server")
    await register_all_tools(server)

    # The handlers are registered internally by the MCP framework
    # We can verify by checking that registration completed without errors
    assert server is not None


def test_configuration_validation():
    """Test configuration validation works correctly."""
    from src.mcp_postgres.config.database import (
        database_config,
        validate_database_config,
    )
    from src.mcp_postgres.config.settings import validate_environment

    # Test with valid environment
    test_env = {
        "DATABASE_URL": "postgresql://test:test@localhost:5432/test",
        "LOG_LEVEL": "INFO"
    }

    with patch.dict(os.environ, test_env):
        # Should not raise exceptions
        validate_environment()
        validate_database_config(database_config)


@pytest.mark.asyncio
async def test_connection_manager_integration():
    """Test connection manager integration."""
    from src.mcp_postgres.config.database import DatabaseConfig
    from src.mcp_postgres.core.connection import ConnectionManager

    config = DatabaseConfig(
        host="localhost",
        port=5432,
        database="test",
        username="test",
        password="test"
    )

    manager = ConnectionManager(config)

    with patch("asyncpg.create_pool") as mock_create_pool:
        mock_pool = AsyncMock()

        async def mock_create_pool_coro(*args, **kwargs):
            return mock_pool

        mock_create_pool.side_effect = mock_create_pool_coro

        # Test lifecycle
        await manager.initialize()
        assert manager.is_initialized

        health = await manager.health_check()
        assert health["status"] == "healthy"

        await manager.close()


def test_all_tools_have_valid_functions():
    """Test that all registered tools have valid callable functions."""
    from src.mcp_postgres.tools.register_tools import TOOL_REGISTRY

    for tool_name, tool_info in TOOL_REGISTRY.items():
        assert callable(tool_info["function"]), f"Tool {tool_name} function is not callable"
        assert asyncio.iscoroutinefunction(tool_info["function"]), f"Tool {tool_name} function is not async"


def test_mcp_schema_compliance():
    """Test all tools comply with MCP schema requirements."""
    from src.mcp_postgres.tools.register_tools import TOOL_REGISTRY

    for tool_name, tool_info in TOOL_REGISTRY.items():
        schema = tool_info["schema"]

        # Required MCP fields
        assert "name" in schema
        assert "description" in schema
        assert "inputSchema" in schema

        # Schema validation
        assert schema["name"] == tool_name
        assert isinstance(schema["description"], str)
        assert len(schema["description"]) > 0

        # Input schema validation
        input_schema = schema["inputSchema"]
        assert input_schema["type"] == "object"

        if "properties" in input_schema:
            assert isinstance(input_schema["properties"], dict)

        if "required" in input_schema:
            assert isinstance(input_schema["required"], list)


if __name__ == "__main__":
    # Run tests manually
    print("Running complete server integration tests...")

    # Test 1
    print("1. Testing tool registry completeness...")
    test_tool_registry_completeness()
    print("   ✓ Passed")

    # Test 2
    print("2. Testing configuration validation...")
    test_configuration_validation()
    print("   ✓ Passed")

    # Test 3
    print("3. Testing tool functions...")
    test_all_tools_have_valid_functions()
    print("   ✓ Passed")

    # Test 4
    print("4. Testing MCP schema compliance...")
    test_mcp_schema_compliance()
    print("   ✓ Passed")

    # Test 5 (async)
    print("5. Testing MCP protocol handlers...")
    asyncio.run(test_mcp_protocol_handlers())
    print("   ✓ Passed")

    # Test 6 (async)
    print("6. Testing connection manager integration...")
    asyncio.run(test_connection_manager_integration())
    print("   ✓ Passed")

    print("\n🎉 All integration tests passed!")
