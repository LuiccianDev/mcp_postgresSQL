"""Basic server startup test."""

import asyncio
import os
from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_server_can_start():
    """Test that the server can start with mocked dependencies."""

    # Set required environment variables
    test_env = {
        "DATABASE_URL": "postgresql://test:test@localhost:5432/test",
        "LOG_LEVEL": "INFO"
    }

    with patch.dict(os.environ, test_env):
        # Mock all external dependencies
        with patch("asyncpg.create_pool") as mock_create_pool:
            # Mock the pool creation
            mock_pool = AsyncMock()

            async def mock_create_pool_coro(*args, **kwargs):
                return mock_pool

            mock_create_pool.side_effect = mock_create_pool_coro

            # Mock stdio_server
            with patch("src.mcp_postgres.main.stdio_server") as mock_stdio:
                mock_read_stream = AsyncMock()
                mock_write_stream = AsyncMock()

                mock_context = AsyncMock()
                mock_context.__aenter__ = AsyncMock(return_value=(mock_read_stream, mock_write_stream))
                mock_context.__aexit__ = AsyncMock(return_value=None)
                mock_stdio.return_value = mock_context

                # Mock the server run method to avoid infinite loop
                with patch("mcp.server.Server.run") as mock_run:
                    mock_run.return_value = None

                    # Import and run main
                    from src.mcp_postgres.main import main

                    # Should complete without errors
                    await main()

                    # Verify key components were called
                    mock_create_pool.assert_called_once()
                    mock_run.assert_called_once()


@pytest.mark.asyncio
async def test_tool_registration():
    """Test that tools are properly registered."""
    from mcp.server import Server

    from src.mcp_postgres.tools.register_tools import TOOL_REGISTRY, register_all_tools

    # Create server
    server = Server("test-server")

    # Register tools
    await register_all_tools(server)

    # Verify tools were registered
    assert len(TOOL_REGISTRY) == 38  # Expected tool count

    # Verify server has handlers
    assert hasattr(server, '_list_tools_handler')
    assert hasattr(server, '_call_tool_handler')


def test_tool_registry_integrity():
    """Test that the tool registry is properly structured."""
    from src.mcp_postgres.tools.register_tools import TOOL_REGISTRY

    # Verify all tools have required fields
    for tool_name, tool_info in TOOL_REGISTRY.items():
        assert "function" in tool_info
        assert "schema" in tool_info
        assert "module" in tool_info
        assert callable(tool_info["function"])

        # Verify schema structure
        schema = tool_info["schema"]
        assert "name" in schema
        assert "description" in schema
        assert "inputSchema" in schema
        assert schema["name"] == tool_name


if __name__ == "__main__":
    # Run basic test
    asyncio.run(test_server_can_start())
    print("✓ Server startup test passed")

    asyncio.run(test_tool_registration())
    print("✓ Tool registration test passed")

    test_tool_registry_integrity()
    print("✓ Tool registry integrity test passed")

    print("All basic tests passed!")
