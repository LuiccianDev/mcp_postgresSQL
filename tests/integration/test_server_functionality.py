"""Functional tests for MCP server basic functionality."""

from unittest.mock import AsyncMock, patch

import pytest

from src.mcp_postgres.config.database import DatabaseConfig
from src.mcp_postgres.core.connection import ConnectionManager
from src.mcp_postgres.tools.register_tools import (
    TOOL_REGISTRY,
    get_all_tools,
    get_tool_by_name,
    get_tools_by_module,
    validate_tool_parameters,
)


class TestServerFunctionality:
    """Test basic server functionality without requiring a real database."""

    def test_tool_registry_structure(self):
        """Test that the tool registry has the expected structure."""
        tools = get_all_tools()

        # Should have all 38 tools as per design
        assert len(tools) >= 38

        # Each tool should have required fields
        for tool_name, tool_info in tools.items():
            assert isinstance(tool_name, str)
            assert "function" in tool_info
            assert "schema" in tool_info
            assert "module" in tool_info
            assert callable(tool_info["function"])

    def test_tool_retrieval_by_name(self):
        """Test retrieving tools by name."""
        # Test existing tool
        tool = get_tool_by_name("execute_query")
        assert tool is not None
        assert tool["module"] == "query_tools"
        assert "function" in tool

        # Test non-existing tool
        tool = get_tool_by_name("nonexistent_tool")
        assert tool is None

    def test_tools_by_module(self):
        """Test retrieving tools by module."""
        # Test query tools module
        query_tools = get_tools_by_module("query_tools")
        assert len(query_tools) == 3
        assert "execute_query" in query_tools
        assert "execute_raw_query" in query_tools
        assert "execute_transaction" in query_tools

        # Test schema tools module
        schema_tools = get_tools_by_module("schema_tools")
        assert len(schema_tools) == 8

        # Test non-existing module
        empty_tools = get_tools_by_module("nonexistent_module")
        assert len(empty_tools) == 0

    def test_parameter_validation(self):
        """Test tool parameter validation."""
        # Test valid parameters for execute_query
        is_valid, error = validate_tool_parameters("execute_query", {
            "query": "SELECT * FROM users WHERE id = $1",
            "parameters": [1]
        })
        assert is_valid
        assert error is None

        # Test missing required parameter
        is_valid, error = validate_tool_parameters("execute_query", {})
        assert not is_valid
        assert "Required parameter 'query' is missing" in error

        # Test invalid tool name
        is_valid, error = validate_tool_parameters("nonexistent_tool", {})
        assert not is_valid
        assert "Tool 'nonexistent_tool' not found" in error

    def test_parameter_type_validation(self):
        """Test parameter type validation."""
        # Test string parameter validation
        is_valid, error = validate_tool_parameters("list_tables", {
            "schema": "public"  # Should be string
        })
        assert is_valid

        # Test invalid type
        is_valid, error = validate_tool_parameters("list_tables", {
            "schema": 123  # Should be string, not int
        })
        assert not is_valid
        assert "must be a string" in error

    @pytest.mark.asyncio
    async def test_connection_manager_lifecycle(self):
        """Test connection manager initialization and cleanup."""
        # Create test configuration
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="test_db",
            username="test_user",
            password="test_pass",
            pool_size=2
        )

        manager = ConnectionManager(config)

        # Initially not initialized
        assert not manager.is_initialized

        # Mock asyncpg.create_pool to return a coroutine
        with patch("asyncpg.create_pool") as mock_create_pool:
            mock_pool = AsyncMock()

            async def mock_create_pool_coro(*args, **kwargs):
                return mock_pool

            mock_create_pool.side_effect = mock_create_pool_coro

            # Initialize
            await manager.initialize()
            assert manager.is_initialized
            mock_create_pool.assert_called_once()

            # Close
            await manager.close()
            mock_pool.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_manager_error_handling(self):
        """Test connection manager error handling."""
        config = DatabaseConfig(
            host="invalid_host",
            port=5432,
            database="test_db",
            username="test_user",
            password="test_pass"
        )

        manager = ConnectionManager(config)

        # Mock asyncpg.create_pool to raise an exception
        with patch("asyncpg.create_pool") as mock_create_pool:
            mock_create_pool.side_effect = Exception("Connection failed")

            # Should raise ConnectionError
            with pytest.raises(ConnectionError, match="Failed to initialize database connection pool"):
                await manager.initialize()

    def test_all_tool_schemas_valid(self):
        """Test that all tool schemas are valid MCP schemas."""
        for tool_name, tool_info in TOOL_REGISTRY.items():
            schema = tool_info["schema"]

            # Required schema fields
            assert "name" in schema, f"Tool {tool_name} missing name in schema"
            assert "description" in schema, f"Tool {tool_name} missing description in schema"
            assert "inputSchema" in schema, f"Tool {tool_name} missing inputSchema"

            # Schema name should match tool name
            assert schema["name"] == tool_name, f"Tool {tool_name} schema name mismatch"

            # Description should be non-empty
            assert len(schema["description"]) > 0, f"Tool {tool_name} has empty description"

            # Input schema should be object type
            input_schema = schema["inputSchema"]
            assert input_schema["type"] == "object", f"Tool {tool_name} inputSchema should be object type"

    def test_tool_module_organization(self):
        """Test that tools are properly organized by module."""
        expected_modules = {
            "query_tools": ["execute_query", "execute_raw_query", "execute_transaction"],
            "schema_tools": ["list_tables", "describe_table", "list_indexes", "list_constraints",
                           "list_views", "list_functions", "list_triggers", "list_sequences"],
            "analysis_tools": ["analyze_column", "find_duplicates", "profile_table", "analyze_correlations"],
            "data_tools": ["insert_data", "update_data", "delete_data", "bulk_insert"],
            "relation_tools": ["get_foreign_keys", "get_table_relationships", "validate_referential_integrity"],
            "performance_tools": ["analyze_query_performance", "find_slow_queries", "get_table_stats"],
            "backup_tools": ["export_table_csv", "import_csv_data", "backup_table"],
            "admin_tools": ["get_database_info", "monitor_connections", "vacuum_table", "reindex_table"],
            "validation_tools": ["validate_constraints", "validate_data_types", "check_data_integrity"],
            "generation_tools": ["generate_ddl", "generate_insert_template", "generate_orm_model"],
        }

        for module, expected_tools in expected_modules.items():
            module_tools = get_tools_by_module(module)
            assert len(module_tools) == len(expected_tools), \
                f"Module {module} has {len(module_tools)} tools, expected {len(expected_tools)}"

            for tool_name in expected_tools:
                assert tool_name in module_tools, f"Tool {tool_name} not found in module {module}"

    @pytest.mark.asyncio
    async def test_server_initialization_sequence(self):
        """Test the server initialization sequence with mocked dependencies."""
        from mcp.server import Server

        from src.mcp_postgres.main import setup_logging

        # Test logging setup
        setup_logging()

        # Test server creation
        server = Server("test-mcp-postgres")
        assert server is not None

        # Test tool registration
        from src.mcp_postgres.tools.register_tools import register_all_tools

        # Mock the connection manager to avoid database dependency
        with patch("src.mcp_postgres.tools.register_tools.connection_manager"):
            await register_all_tools(server)

            # Verify handlers are registered
            assert hasattr(server, '_list_tools_handler')
            assert hasattr(server, '_call_tool_handler')

    def test_environment_configuration(self):
        """Test environment configuration handling."""
        from src.mcp_postgres.config.database import database_config
        from src.mcp_postgres.config.settings import server_config

        # Test that configuration objects exist and have expected attributes
        assert hasattr(server_config, 'log_level')
        assert hasattr(database_config, 'host')
        assert hasattr(database_config, 'port')
        assert hasattr(database_config, 'database')
        assert hasattr(database_config, 'username')

    @pytest.mark.asyncio
    async def test_tool_execution_mock(self):
        """Test tool execution with mocked database operations."""
        from src.mcp_postgres.tools.query_tools import execute_query

        # Mock the connection manager
        with patch("src.mcp_postgres.tools.query_tools.connection_manager") as mock_manager:
            mock_manager.execute_query = AsyncMock(return_value=[{"id": 1, "name": "test"}])

            # Execute a tool
            result = await execute_query(
                query="SELECT * FROM users WHERE id = $1",
                parameters=[1]
            )

            # Verify the result structure
            assert "success" in result
            assert "data" in result
            mock_manager.execute_query.assert_called_once()

    def test_tool_count_requirements(self):
        """Test that we meet the requirement of 38 tools as per design."""
        total_tools = len(TOOL_REGISTRY)
        assert total_tools >= 38, f"Requirement not met: need 38+ tools, have {total_tools}"

        # Verify we have exactly the expected count based on design
        expected_total = 3 + 8 + 4 + 4 + 3 + 3 + 3 + 4 + 3 + 3  # Sum of all modules
        assert total_tools == expected_total, f"Expected exactly {expected_total} tools, got {total_tools}"

    def test_mcp_schema_compliance(self):
        """Test that all schemas comply with MCP specification."""
        for tool_name, tool_info in TOOL_REGISTRY.items():
            schema = tool_info["schema"]

            # MCP tool schema requirements
            assert isinstance(schema["name"], str)
            assert isinstance(schema["description"], str)
            assert isinstance(schema["inputSchema"], dict)

            input_schema = schema["inputSchema"]
            assert input_schema["type"] == "object"

            # If properties exist, they should be properly formatted
            if "properties" in input_schema:
                assert isinstance(input_schema["properties"], dict)
                for prop_name, prop_schema in input_schema["properties"].items():
                    assert isinstance(prop_name, str)
                    assert isinstance(prop_schema, dict)
                    assert "type" in prop_schema or "anyOf" in prop_schema or "$ref" in prop_schema

            # If required exists, it should be a list
            if "required" in input_schema:
                assert isinstance(input_schema["required"], list)
                for req_field in input_schema["required"]:
                    assert isinstance(req_field, str)
