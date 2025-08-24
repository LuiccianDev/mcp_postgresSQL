"""Integration tests for MCP server functionality."""

import asyncio
import os
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from src.mcp_postgres.core.connection import connection_manager
from mcp_postgres.server import main
from src.mcp_postgres.tools.register_tools import TOOL_REGISTRY, get_tool_discovery_info


class TestMCPServerIntegration:
    """Integration tests for MCP server functionality."""

    @pytest_asyncio.fixture
    async def mock_database_config(self):
        """Mock database configuration for testing."""
        with patch("src.mcp_postgres.config.database.database_config") as mock_config:
            mock_config.host = "localhost"
            mock_config.port = 5432
            mock_config.database = "test_db"
            mock_config.username = "test_user"
            mock_config.password = "test_pass"
            mock_config.pool_size = 5
            mock_config.pool_kwargs = {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "user": "test_user",
                "password": "test_pass",
                "min_size": 1,
                "max_size": 5,
            }
            yield mock_config

    @pytest_asyncio.fixture
    async def mock_connection_manager(self, mock_database_config):
        """Mock connection manager for testing."""
        with patch("src.mcp_postgres.core.connection.connection_manager") as mock_manager:
            # Mock the connection manager methods
            mock_manager.initialize = AsyncMock()
            mock_manager.close = AsyncMock()
            mock_manager.health_check = AsyncMock(return_value={
                "status": "healthy",
                "test_query_result": 1,
                "pool_stats": {"size": 1, "max_size": 5, "min_size": 1},
                "database": "test_db",
                "host": "localhost",
                "port": 5432,
            })
            mock_manager.is_initialized = True
            yield mock_manager

    @pytest_asyncio.fixture
    async def mock_stdio_server(self):
        """Mock stdio server for testing."""
        with patch("src.mcp_postgres.main.stdio_server") as mock_stdio:
            # Create mock streams
            mock_read_stream = AsyncMock()
            mock_write_stream = AsyncMock()

            # Mock the context manager
            mock_context = AsyncMock()
            mock_context.__aenter__ = AsyncMock(return_value=(mock_read_stream, mock_write_stream))
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_stdio.return_value = mock_context

            yield mock_stdio, mock_read_stream, mock_write_stream

    def test_tool_registry_completeness(self):
        """Test that all expected tools are registered."""
        # Verify total tool count (50+ tools as per requirements)
        assert len(TOOL_REGISTRY) >= 50, f"Expected at least 50 tools, got {len(TOOL_REGISTRY)}"

        # Verify tools by module
        expected_modules = {
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

        actual_modules = {}
        for tool_info in TOOL_REGISTRY.values():
            module = tool_info["module"]
            actual_modules[module] = actual_modules.get(module, 0) + 1

        for module, expected_count in expected_modules.items():
            assert module in actual_modules, f"Module {module} not found in registry"
            assert actual_modules[module] == expected_count, \
                f"Module {module} has {actual_modules[module]} tools, expected {expected_count}"

    def test_tool_schema_validation(self):
        """Test that all tools have valid schemas."""
        for tool_name, tool_info in TOOL_REGISTRY.items():
            # Check required fields
            assert "function" in tool_info, f"Tool {tool_name} missing function"
            assert "schema" in tool_info, f"Tool {tool_name} missing schema"
            assert "module" in tool_info, f"Tool {tool_name} missing module"

            # Check schema structure
            schema = tool_info["schema"]
            assert "name" in schema, f"Tool {tool_name} schema missing name"
            assert "description" in schema, f"Tool {tool_name} schema missing description"
            assert "inputSchema" in schema, f"Tool {tool_name} schema missing inputSchema"

            # Check input schema structure
            input_schema = schema["inputSchema"]
            assert "type" in input_schema, f"Tool {tool_name} inputSchema missing type"
            assert input_schema["type"] == "object", f"Tool {tool_name} inputSchema type should be object"

            # Verify function is callable
            assert callable(tool_info["function"]), f"Tool {tool_name} function is not callable"

    def test_tool_discovery_info(self):
        """Test tool discovery information generation."""
        discovery_info = get_tool_discovery_info()

        # Check structure
        assert "total_tools" in discovery_info
        assert "modules" in discovery_info
        assert "module_summary" in discovery_info

        # Verify counts
        assert discovery_info["total_tools"] == len(TOOL_REGISTRY)
        assert len(discovery_info["modules"]) == 10  # 10 modules

        # Check module summary
        total_from_summary = sum(discovery_info["module_summary"].values())
        assert total_from_summary == discovery_info["total_tools"]

    @pytest.mark.asyncio
    async def test_connection_manager_initialization(self, mock_database_config):
        """Test connection manager initialization."""
        # Test with real connection manager (but mocked asyncpg)
        with patch("asyncpg.create_pool") as mock_create_pool:
            mock_pool = AsyncMock()
            mock_create_pool.return_value = mock_pool

            # Initialize connection manager
            await connection_manager.initialize()

            # Verify pool creation was called
            mock_create_pool.assert_called_once()
            assert connection_manager.is_initialized

    @pytest.mark.asyncio
    async def test_server_startup_sequence(self, mock_connection_manager, mock_stdio_server):
        """Test the server startup sequence."""
        mock_stdio, mock_read_stream, mock_write_stream = mock_stdio_server

        with patch("src.mcp_postgres.main.Server") as mock_server_class:
            mock_server = AsyncMock()
            mock_server_class.return_value = mock_server
            mock_server.create_initialization_options.return_value = {}
            mock_server.run = AsyncMock()

            with patch("src.mcp_postgres.main.register_all_tools") as mock_register:
                mock_register.return_value = None

                # Mock environment validation
                with patch("src.mcp_postgres.main.validate_environment"):
                    with patch("src.mcp_postgres.main.validate_database_config"):
                        # This should complete the startup sequence without running indefinitely
                        with patch("src.mcp_postgres.main.signal.signal"):
                            try:
                                # Run main with a timeout to prevent hanging
                                await asyncio.wait_for(main(), timeout=1.0)
                            except TimeoutError:
                                # Expected - the server would run indefinitely in real usage
                                pass

                # Verify initialization sequence
                mock_connection_manager.initialize.assert_called_once()
                mock_connection_manager.health_check.assert_called_once()
                mock_register.assert_called_once_with(mock_server)
                mock_server.run.assert_called_once()

    @pytest.mark.asyncio
    async def test_server_shutdown_handling(self, mock_connection_manager):
        """Test server shutdown handling."""
        from mcp_postgres.server import shutdown_handler

        mock_server = AsyncMock()

        # Test shutdown handler
        await shutdown_handler(mock_server)

        # Verify connection manager close was called
        mock_connection_manager.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_server_error_handling(self, mock_connection_manager):
        """Test server error handling during startup."""
        # Mock connection manager to raise an error during initialization
        mock_connection_manager.initialize.side_effect = Exception("Connection failed")

        with patch("src.mcp_postgres.main.validate_environment"):
            with patch("src.mcp_postgres.main.validate_database_config"):
                with patch("sys.exit") as mock_exit:
                    await main()

                    # Verify error handling
                    mock_exit.assert_called_once_with(1)
                    mock_connection_manager.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_health_check_failure_handling(self, mock_connection_manager):
        """Test handling of health check failures."""
        # Mock health check to return unhealthy status
        mock_connection_manager.health_check.return_value = {
            "status": "unhealthy",
            "error": "Connection timeout"
        }

        with patch("src.mcp_postgres.main.validate_environment"):
            with patch("src.mcp_postgres.main.validate_database_config"):
                with patch("sys.exit") as mock_exit:
                    await main()

                    # Verify error handling for unhealthy database
                    mock_exit.assert_called_once_with(1)

    def test_signal_handler_setup(self):
        """Test that signal handlers are properly configured."""
        with patch("src.mcp_postgres.main.signal.signal") as mock_signal:
            with patch("src.mcp_postgres.main.validate_environment"):
                with patch("src.mcp_postgres.main.validate_database_config"):
                    with patch("src.mcp_postgres.main.connection_manager") as mock_manager:
                        mock_manager.initialize = AsyncMock()
                        mock_manager.health_check = AsyncMock(return_value={"status": "healthy", "pool_stats": {}})

                        with patch("src.mcp_postgres.main.Server"):
                            with patch("src.mcp_postgres.main.register_all_tools"):
                                with patch("src.mcp_postgres.main.stdio_server") as mock_stdio:
                                    mock_context = AsyncMock()
                                    mock_context.__aenter__ = AsyncMock(return_value=(AsyncMock(), AsyncMock()))
                                    mock_context.__aexit__ = AsyncMock(return_value=None)
                                    mock_stdio.return_value = mock_context

                                    try:
                                        asyncio.run(asyncio.wait_for(main(), timeout=0.1))
                                    except TimeoutError:
                                        pass

                                    # Verify signal handlers were set up
                                    assert mock_signal.call_count >= 2  # SIGINT and SIGTERM

    @pytest.mark.asyncio
    async def test_mcp_protocol_compliance(self):
        """Test MCP protocol compliance."""
        from mcp.server import Server

        from src.mcp_postgres.tools.register_tools import register_all_tools

        # Create a real server instance
        server = Server("test-mcp-postgres")

        # Register tools
        await register_all_tools(server)

        # Verify server has the required handlers
        # Note: This is a basic test - full MCP protocol testing would require
        # a complete MCP client implementation
        assert hasattr(server, '_tools')
        assert hasattr(server, '_call_tool_handler')
        assert hasattr(server, '_list_tools_handler')

    def test_environment_variable_handling(self):
        """Test environment variable handling and validation."""
        # Test with missing required environment variables
        with patch.dict(os.environ, {}, clear=True):
            with patch("src.mcp_postgres.main.validate_environment") as mock_validate:
                mock_validate.side_effect = ValueError("Missing DATABASE_URL")

                with patch("sys.exit") as mock_exit:
                    asyncio.run(main())
                    mock_exit.assert_called_once_with(1)

    @pytest.mark.asyncio
    async def test_concurrent_tool_execution(self, mock_connection_manager):
        """Test that multiple tools can be executed concurrently."""
        from src.mcp_postgres.tools.register_tools import validate_tool_parameters

        # Test parameter validation for multiple tools concurrently
        tasks = []
        for tool_name in list(TOOL_REGISTRY.keys())[:5]:  # Test first 5 tools
            task = asyncio.create_task(
                asyncio.coroutine(lambda tn=tool_name: validate_tool_parameters(tn, {}))()
            )
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # All should complete (though may have validation errors)
        assert len(results) == 5
        for result in results:
            assert not isinstance(result, Exception) or isinstance(result, tuple)
