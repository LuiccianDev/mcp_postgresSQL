"""Integration tests for tool registration system."""

import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from unittest.mock import AsyncMock, MagicMock

import pytest
from mcp.server import Server
from mcp.types import Tool

from mcp_postgres.tools.register_tools import (
    TOOL_REGISTRY,
    get_all_tools,
    get_tool_by_name,
    get_tool_discovery_info,
    get_tools_by_module,
    register_all_tools,
    validate_tool_parameters,
)


class TestToolRegistry:
    """Test the tool registry functionality."""

    def test_tool_registry_completeness(self):
        """Test that all expected tools are registered."""
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

        # Check total tool count
        assert len(TOOL_REGISTRY) >= 38, f"Expected at least 38 tools, got {len(TOOL_REGISTRY)}"

        # Check tools by module
        for module, expected_count in expected_modules.items():
            module_tools = get_tools_by_module(module)
            assert len(module_tools) >= expected_count, f"Module {module} should have at least {expected_count} tools, got {len(module_tools)}"

    def test_tool_registry_structure(self):
        """Test that all tools have required structure."""
        for tool_name, tool_info in TOOL_REGISTRY.items():
            # Check required keys
            assert "function" in tool_info, f"Tool {tool_name} missing 'function'"
            assert "schema" in tool_info, f"Tool {tool_name} missing 'schema'"
            assert "module" in tool_info, f"Tool {tool_name} missing 'module'"

            # Check schema structure
            schema = tool_info["schema"]
            assert "name" in schema, f"Tool {tool_name} schema missing 'name'"
            assert "description" in schema, f"Tool {tool_name} schema missing 'description'"
            assert "inputSchema" in schema, f"Tool {tool_name} schema missing 'inputSchema'"

            # Check that schema name matches tool name
            assert schema["name"] == tool_name, f"Tool {tool_name} schema name mismatch"

            # Check that function is callable
            assert callable(tool_info["function"]), f"Tool {tool_name} function is not callable"

    def test_get_all_tools(self):
        """Test getting all tools."""
        all_tools = get_all_tools()
        assert isinstance(all_tools, dict)
        assert len(all_tools) == len(TOOL_REGISTRY)

        # Ensure it's a copy, not the original
        all_tools["test_key"] = "test_value"
        assert "test_key" not in TOOL_REGISTRY

    def test_get_tool_by_name(self):
        """Test getting specific tools by name."""
        # Test existing tool
        tool = get_tool_by_name("execute_query")
        assert tool is not None
        assert tool["schema"]["name"] == "execute_query"
        assert tool["module"] == "query_tools"

        # Test non-existing tool
        tool = get_tool_by_name("non_existent_tool")
        assert tool is None

    def test_get_tools_by_module(self):
        """Test getting tools by module."""
        # Test existing module
        query_tools = get_tools_by_module("query_tools")
        assert len(query_tools) >= 3
        for tool_name, tool_info in query_tools.items():
            assert tool_info["module"] == "query_tools"

        # Test non-existing module
        empty_tools = get_tools_by_module("non_existent_module")
        assert len(empty_tools) == 0


class TestParameterValidation:
    """Test parameter validation functionality."""

    def test_validate_required_parameters(self):
        """Test validation of required parameters."""
        # Test with missing required parameter
        is_valid, error = validate_tool_parameters("execute_query", {})
        assert not is_valid
        assert "Required parameter 'query' is missing" in error

        # Test with required parameter present
        is_valid, error = validate_tool_parameters("execute_query", {"query": "SELECT 1"})
        assert is_valid
        assert error is None

    def test_validate_parameter_types(self):
        """Test validation of parameter types."""
        # Test string parameter
        is_valid, error = validate_tool_parameters("execute_query", {"query": 123})
        assert not is_valid
        assert "must be a string" in error

        # Test array parameter
        is_valid, error = validate_tool_parameters("execute_query", {
            "query": "SELECT 1",
            "parameters": "not_an_array"
        })
        assert not is_valid
        assert "must be an array" in error

    def test_validate_enum_parameters(self):
        """Test validation of enum parameters."""
        # Test valid enum value
        is_valid, error = validate_tool_parameters("execute_query", {
            "query": "SELECT 1",
            "fetch_mode": "all"
        })
        assert is_valid
        assert error is None

        # Test invalid enum value
        is_valid, error = validate_tool_parameters("execute_query", {
            "query": "SELECT 1",
            "fetch_mode": "invalid_mode"
        })
        assert not is_valid
        assert "must be one of" in error

    def test_validate_unknown_tool(self):
        """Test validation with unknown tool."""
        is_valid, error = validate_tool_parameters("unknown_tool", {})
        assert not is_valid
        assert "Tool 'unknown_tool' not found" in error


class TestToolDiscovery:
    """Test tool discovery functionality."""

    def test_get_tool_discovery_info(self):
        """Test getting tool discovery information."""
        discovery_info = get_tool_discovery_info()

        # Check structure
        assert "total_tools" in discovery_info
        assert "modules" in discovery_info
        assert "module_summary" in discovery_info

        # Check total tools count
        assert discovery_info["total_tools"] == len(TOOL_REGISTRY)

        # Check modules structure
        modules = discovery_info["modules"]
        for module_name, module_info in modules.items():
            assert "tool_count" in module_info
            assert "tools" in module_info
            assert isinstance(module_info["tools"], list)

            # Check tool info structure
            for tool in module_info["tools"]:
                assert "name" in tool
                assert "description" in tool
                assert "required_params" in tool
                assert "optional_params" in tool

    def test_module_summary_consistency(self):
        """Test that module summary matches actual tool counts."""
        discovery_info = get_tool_discovery_info()

        for module_name, tool_count in discovery_info["module_summary"].items():
            actual_tools = get_tools_by_module(module_name)
            assert len(actual_tools) == tool_count


@pytest.mark.asyncio
class TestToolRegistration:
    """Test tool registration with MCP server."""

    async def test_register_all_tools(self):
        """Test registering all tools with MCP server."""
        # Create mock server
        server = MagicMock(spec=Server)
        server.list_tools = MagicMock()
        server.call_tool = MagicMock()

        # Register tools
        await register_all_tools(server)

        # Verify handlers were registered
        server.list_tools.assert_called_once()
        server.call_tool.assert_called_once()

    async def test_list_tools_handler(self):
        """Test the list_tools handler functionality."""
        server = MagicMock(spec=Server)
        list_tools_handler = None
        call_tool_handler = None

        # Capture the handlers
        def mock_list_tools():
            def decorator(func):
                nonlocal list_tools_handler
                list_tools_handler = func
                return func
            return decorator

        def mock_call_tool():
            def decorator(func):
                nonlocal call_tool_handler
                call_tool_handler = func
                return func
            return decorator

        server.list_tools = mock_list_tools
        server.call_tool = mock_call_tool

        # Register tools
        await register_all_tools(server)

        # Test list_tools handler
        assert list_tools_handler is not None
        tools = await list_tools_handler()

        assert isinstance(tools, list)
        assert len(tools) == len(TOOL_REGISTRY)

        for tool in tools:
            assert isinstance(tool, Tool)
            assert hasattr(tool, 'name')
            assert hasattr(tool, 'description')
            assert hasattr(tool, 'inputSchema')

    async def test_call_tool_handler_success(self):
        """Test successful tool call handling."""
        server = MagicMock(spec=Server)
        call_tool_handler = None
        list_tools_handler = None

        def mock_call_tool():
            def decorator(func):
                nonlocal call_tool_handler
                call_tool_handler = func
                return func
            return decorator

        def mock_list_tools():
            def decorator(func):
                nonlocal list_tools_handler
                list_tools_handler = func
                return func
            return decorator

        server.call_tool = mock_call_tool
        server.list_tools = mock_list_tools

        # Mock a tool function
        mock_tool_function = AsyncMock(return_value={"result": "success"})
        original_function = TOOL_REGISTRY["execute_query"]["function"]
        TOOL_REGISTRY["execute_query"]["function"] = mock_tool_function

        try:
            # Register tools
            await register_all_tools(server)

            # Test call_tool handler
            assert call_tool_handler is not None
            result = await call_tool_handler("execute_query", {"query": "SELECT 1"})

            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0]["type"] == "text"
            assert "success" in result[0]["text"]

            # Verify the tool function was called
            mock_tool_function.assert_called_once_with(query="SELECT 1")

        finally:
            # Restore original function
            TOOL_REGISTRY["execute_query"]["function"] = original_function

    async def test_call_tool_handler_validation_error(self):
        """Test tool call handling with validation errors."""
        server = MagicMock(spec=Server)
        call_tool_handler = None
        list_tools_handler = None

        def mock_call_tool():
            def decorator(func):
                nonlocal call_tool_handler
                call_tool_handler = func
                return func
            return decorator

        def mock_list_tools():
            def decorator(func):
                nonlocal list_tools_handler
                list_tools_handler = func
                return func
            return decorator

        server.call_tool = mock_call_tool
        server.list_tools = mock_list_tools

        # Register tools
        await register_all_tools(server)

        # Test with missing required parameter
        with pytest.raises(ValueError, match="Parameter validation failed"):
            await call_tool_handler("execute_query", {})

    async def test_call_tool_handler_unknown_tool(self):
        """Test tool call handling with unknown tool."""
        server = MagicMock(spec=Server)
        call_tool_handler = None
        list_tools_handler = None

        def mock_call_tool():
            def decorator(func):
                nonlocal call_tool_handler
                call_tool_handler = func
                return func
            return decorator

        def mock_list_tools():
            def decorator(func):
                nonlocal list_tools_handler
                list_tools_handler = func
                return func
            return decorator

        server.call_tool = mock_call_tool
        server.list_tools = mock_list_tools

        # Register tools
        await register_all_tools(server)

        # Test with unknown tool
        with pytest.raises(ValueError, match="Unknown tool"):
            await call_tool_handler("unknown_tool", {})


class TestToolSchemas:
    """Test tool schema definitions."""

    def test_all_tools_have_valid_schemas(self):
        """Test that all tools have valid schema definitions."""
        for tool_name, tool_info in TOOL_REGISTRY.items():
            schema = tool_info["schema"]

            # Check required schema fields
            assert isinstance(schema["name"], str), f"Tool {tool_name} name must be string"
            assert isinstance(schema["description"], str), f"Tool {tool_name} description must be string"
            assert isinstance(schema["inputSchema"], dict), f"Tool {tool_name} inputSchema must be dict"

            # Check inputSchema structure
            input_schema = schema["inputSchema"]
            assert input_schema.get("type") == "object", f"Tool {tool_name} inputSchema type must be 'object'"

            if "properties" in input_schema:
                assert isinstance(input_schema["properties"], dict), f"Tool {tool_name} properties must be dict"

            if "required" in input_schema:
                assert isinstance(input_schema["required"], list), f"Tool {tool_name} required must be list"

    def test_schema_parameter_consistency(self):
        """Test that schema parameters are consistent with function signatures."""
        # This is a basic test - in a real implementation, you might want to
        # inspect function signatures and compare with schema parameters
        for tool_name, tool_info in TOOL_REGISTRY.items():
            schema = tool_info["schema"]
            input_schema = schema["inputSchema"]

            # Check that required parameters exist in properties
            if "required" in input_schema and "properties" in input_schema:
                required_params = input_schema["required"]
                properties = input_schema["properties"]

                for param in required_params:
                    assert param in properties, f"Tool {tool_name} required param '{param}' not in properties"


if __name__ == "__main__":
    pytest.main([__file__])
