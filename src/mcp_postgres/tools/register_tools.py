"""Tool registration system for MCP Postgres server.

This module provides the central registry for all MCP tools, including
schema definitions, parameter validation, and tool discovery functionality.
"""

import logging
from typing import Any

from mcp.server import Server
from mcp.types import Tool

# Import all tool modules and their schemas
from .admin_tools import (
    GET_DATABASE_INFO_SCHEMA,
    MONITOR_CONNECTIONS_SCHEMA,
    REINDEX_TABLE_SCHEMA,
    VACUUM_TABLE_SCHEMA,
    get_database_info,
    monitor_connections,
    reindex_table,
    vacuum_table,
)
from .analysis_tools import (
    ANALYZE_COLUMN_SCHEMA,
    ANALYZE_CORRELATIONS_SCHEMA,
    FIND_DUPLICATES_SCHEMA,
    PROFILE_TABLE_SCHEMA,
    analyze_column,
    analyze_correlations,
    find_duplicates,
    profile_table,
)
from .backup_tools import (
    BACKUP_TABLE_SCHEMA,
    EXPORT_TABLE_CSV_SCHEMA,
    IMPORT_CSV_DATA_SCHEMA,
    backup_table,
    export_table_csv,
    import_csv_data,
)
from .data_tools import (
    BULK_INSERT_SCHEMA,
    DELETE_DATA_SCHEMA,
    INSERT_DATA_SCHEMA,
    UPDATE_DATA_SCHEMA,
    bulk_insert,
    delete_data,
    insert_data,
    update_data,
)
from .generation_tools import (
    GENERATE_DDL_SCHEMA,
    GENERATE_INSERT_TEMPLATE_SCHEMA,
    GENERATE_ORM_MODEL_SCHEMA,
    generate_ddl,
    generate_insert_template,
    generate_orm_model,
)
from .performance_tools import (
    ANALYZE_QUERY_PERFORMANCE_SCHEMA,
    FIND_SLOW_QUERIES_SCHEMA,
    GET_TABLE_STATS_SCHEMA,
    analyze_query_performance,
    find_slow_queries,
    get_table_stats,
)
from .query_tools import (
    EXECUTE_QUERY_SCHEMA,
    EXECUTE_RAW_QUERY_SCHEMA,
    EXECUTE_TRANSACTION_SCHEMA,
    execute_query,
    execute_raw_query,
    execute_transaction,
)
from .relation_tools import (
    GET_FOREIGN_KEYS_SCHEMA,
    GET_TABLE_RELATIONSHIPS_SCHEMA,
    VALIDATE_REFERENTIAL_INTEGRITY_SCHEMA,
    get_foreign_keys,
    get_table_relationships,
    validate_referential_integrity,
)
from .schema_tools import (
    DESCRIBE_TABLE_SCHEMA,
    LIST_CONSTRAINTS_SCHEMA,
    LIST_FUNCTIONS_SCHEMA,
    LIST_INDEXES_SCHEMA,
    LIST_SEQUENCES_SCHEMA,
    LIST_TABLES_SCHEMA,
    LIST_TRIGGERS_SCHEMA,
    LIST_VIEWS_SCHEMA,
    describe_table,
    list_constraints,
    list_functions,
    list_indexes,
    list_sequences,
    list_tables,
    list_triggers,
    list_views,
)
from .validation_tools import (
    CHECK_DATA_INTEGRITY_SCHEMA,
    VALIDATE_CONSTRAINTS_SCHEMA,
    VALIDATE_DATA_TYPES_SCHEMA,
    check_data_integrity,
    validate_constraints,
    validate_data_types,
)


logger = logging.getLogger(__name__)


# Tool registry mapping tool names to their implementations and schemas
TOOL_REGISTRY: dict[str, dict[str, Any]] = {
    # Query Tools (3 tools)
    "execute_query": {
        "function": execute_query,
        "schema": EXECUTE_QUERY_SCHEMA,
        "module": "query_tools",
    },
    "execute_raw_query": {
        "function": execute_raw_query,
        "schema": EXECUTE_RAW_QUERY_SCHEMA,
        "module": "query_tools",
    },
    "execute_transaction": {
        "function": execute_transaction,
        "schema": EXECUTE_TRANSACTION_SCHEMA,
        "module": "query_tools",
    },
    # Schema Tools (8 tools)
    "list_tables": {
        "function": list_tables,
        "schema": LIST_TABLES_SCHEMA,
        "module": "schema_tools",
    },
    "describe_table": {
        "function": describe_table,
        "schema": DESCRIBE_TABLE_SCHEMA,
        "module": "schema_tools",
    },
    "list_indexes": {
        "function": list_indexes,
        "schema": LIST_INDEXES_SCHEMA,
        "module": "schema_tools",
    },
    "list_constraints": {
        "function": list_constraints,
        "schema": LIST_CONSTRAINTS_SCHEMA,
        "module": "schema_tools",
    },
    "list_views": {
        "function": list_views,
        "schema": LIST_VIEWS_SCHEMA,
        "module": "schema_tools",
    },
    "list_functions": {
        "function": list_functions,
        "schema": LIST_FUNCTIONS_SCHEMA,
        "module": "schema_tools",
    },
    "list_triggers": {
        "function": list_triggers,
        "schema": LIST_TRIGGERS_SCHEMA,
        "module": "schema_tools",
    },
    "list_sequences": {
        "function": list_sequences,
        "schema": LIST_SEQUENCES_SCHEMA,
        "module": "schema_tools",
    },
    # Analysis Tools (4 tools)
    "analyze_column": {
        "function": analyze_column,
        "schema": ANALYZE_COLUMN_SCHEMA,
        "module": "analysis_tools",
    },
    "find_duplicates": {
        "function": find_duplicates,
        "schema": FIND_DUPLICATES_SCHEMA,
        "module": "analysis_tools",
    },
    "profile_table": {
        "function": profile_table,
        "schema": PROFILE_TABLE_SCHEMA,
        "module": "analysis_tools",
    },
    "analyze_correlations": {
        "function": analyze_correlations,
        "schema": ANALYZE_CORRELATIONS_SCHEMA,
        "module": "analysis_tools",
    },
    # Data Tools (4 tools)
    "insert_data": {
        "function": insert_data,
        "schema": INSERT_DATA_SCHEMA,
        "module": "data_tools",
    },
    "update_data": {
        "function": update_data,
        "schema": UPDATE_DATA_SCHEMA,
        "module": "data_tools",
    },
    "delete_data": {
        "function": delete_data,
        "schema": DELETE_DATA_SCHEMA,
        "module": "data_tools",
    },
    "bulk_insert": {
        "function": bulk_insert,
        "schema": BULK_INSERT_SCHEMA,
        "module": "data_tools",
    },
    # Relation Tools (3 tools)
    "get_foreign_keys": {
        "function": get_foreign_keys,
        "schema": GET_FOREIGN_KEYS_SCHEMA,
        "module": "relation_tools",
    },
    "get_table_relationships": {
        "function": get_table_relationships,
        "schema": GET_TABLE_RELATIONSHIPS_SCHEMA,
        "module": "relation_tools",
    },
    "validate_referential_integrity": {
        "function": validate_referential_integrity,
        "schema": VALIDATE_REFERENTIAL_INTEGRITY_SCHEMA,
        "module": "relation_tools",
    },
    # Performance Tools (3 tools)
    "analyze_query_performance": {
        "function": analyze_query_performance,
        "schema": ANALYZE_QUERY_PERFORMANCE_SCHEMA,
        "module": "performance_tools",
    },
    "find_slow_queries": {
        "function": find_slow_queries,
        "schema": FIND_SLOW_QUERIES_SCHEMA,
        "module": "performance_tools",
    },
    "get_table_stats": {
        "function": get_table_stats,
        "schema": GET_TABLE_STATS_SCHEMA,
        "module": "performance_tools",
    },
    # Backup Tools (3 tools)
    "export_table_csv": {
        "function": export_table_csv,
        "schema": EXPORT_TABLE_CSV_SCHEMA,
        "module": "backup_tools",
    },
    "import_csv_data": {
        "function": import_csv_data,
        "schema": IMPORT_CSV_DATA_SCHEMA,
        "module": "backup_tools",
    },
    "backup_table": {
        "function": backup_table,
        "schema": BACKUP_TABLE_SCHEMA,
        "module": "backup_tools",
    },
    # Admin Tools (4 tools)
    "get_database_info": {
        "function": get_database_info,
        "schema": GET_DATABASE_INFO_SCHEMA,
        "module": "admin_tools",
    },
    "monitor_connections": {
        "function": monitor_connections,
        "schema": MONITOR_CONNECTIONS_SCHEMA,
        "module": "admin_tools",
    },
    "vacuum_table": {
        "function": vacuum_table,
        "schema": VACUUM_TABLE_SCHEMA,
        "module": "admin_tools",
    },
    "reindex_table": {
        "function": reindex_table,
        "schema": REINDEX_TABLE_SCHEMA,
        "module": "admin_tools",
    },
    # Validation Tools (3 tools)
    "validate_constraints": {
        "function": validate_constraints,
        "schema": VALIDATE_CONSTRAINTS_SCHEMA,
        "module": "validation_tools",
    },
    "validate_data_types": {
        "function": validate_data_types,
        "schema": VALIDATE_DATA_TYPES_SCHEMA,
        "module": "validation_tools",
    },
    "check_data_integrity": {
        "function": check_data_integrity,
        "schema": CHECK_DATA_INTEGRITY_SCHEMA,
        "module": "validation_tools",
    },
    # Generation Tools (3 tools)
    "generate_ddl": {
        "function": generate_ddl,
        "schema": GENERATE_DDL_SCHEMA,
        "module": "generation_tools",
    },
    "generate_insert_template": {
        "function": generate_insert_template,
        "schema": GENERATE_INSERT_TEMPLATE_SCHEMA,
        "module": "generation_tools",
    },
    "generate_orm_model": {
        "function": generate_orm_model,
        "schema": GENERATE_ORM_MODEL_SCHEMA,
        "module": "generation_tools",
    },
}


def get_all_tools() -> dict[str, dict[str, Any]]:
    """Get all registered tools with their metadata.
    Returns:
        Dictionary mapping tool names to their metadata including function,
        schema, and module information.
    """
    return TOOL_REGISTRY.copy()


def get_tool_by_name(tool_name: str) -> dict[str, Any] | None:
    """Get a specific tool by name.
    Args:
        tool_name: Name of the tool to retrieve
    Returns:
        Tool metadata dictionary or None if not found
    """
    return TOOL_REGISTRY.get(tool_name)


def get_tools_by_module(module_name: str) -> dict[str, dict[str, Any]]:
    """Get all tools from a specific module.
    Args:
        module_name: Name of the module (e.g., 'query_tools', 'schema_tools')
    Returns:
        Dictionary of tools from the specified module
    """
    return {
        name: tool_info
        for name, tool_info in TOOL_REGISTRY.items()
        if tool_info["module"] == module_name
    }


def validate_tool_parameters(
    tool_name: str, parameters: dict[str, Any]
) -> tuple[bool, str | None]:
    """Validate parameters against a tool's schema.
    Args:
        tool_name: Name of the tool
        parameters: Parameters to validate
    Returns:
        Tuple of (is_valid, error_message)
    """
    tool_info = get_tool_by_name(tool_name)
    if not tool_info:
        return False, f"Tool '{tool_name}' not found"

    schema = tool_info["schema"]
    input_schema = schema.get("inputSchema", {})

    # Basic validation - check required parameters
    required_params = input_schema.get("required", [])
    for param in required_params:
        if param not in parameters:
            return False, f"Required parameter '{param}' is missing"

    # Type validation for properties
    properties = input_schema.get("properties", {})
    for param_name, param_value in parameters.items():
        if param_name in properties:
            param_schema = properties[param_name]
            param_type = param_schema.get("type")

            # Basic type checking
            if param_type == "string" and not isinstance(param_value, str):
                return False, f"Parameter '{param_name}' must be a string"
            elif param_type == "integer" and not isinstance(param_value, int):
                return False, f"Parameter '{param_name}' must be an integer"
            elif param_type == "number" and not isinstance(param_value, int | float):
                return False, f"Parameter '{param_name}' must be a number"
            elif param_type == "boolean" and not isinstance(param_value, bool):
                return False, f"Parameter '{param_name}' must be a boolean"
            elif param_type == "array" and not isinstance(param_value, list):
                return False, f"Parameter '{param_name}' must be an array"
            elif param_type == "object" and not isinstance(param_value, dict):
                return False, f"Parameter '{param_name}' must be an object"

            # Enum validation
            if "enum" in param_schema and param_value not in param_schema["enum"]:
                return (
                    False,
                    f"Parameter '{param_name}' must be one of: {param_schema['enum']}",
                )

    return True, None


async def register_all_tools(server: Server) -> None:
    """Register all tools with the MCP server.
    Args:
        server: MCP Server instance to register tools with
    """
    logger.info("Registering MCP tools...")

    registered_count = 0
    """ failed_registrations = [] # noqa: F841 """

    # Register list_tools handler
    @server.list_tools()  # type: ignore[misc]
    async def handle_list_tools() -> list[Tool]:
        """Handle list_tools requests from MCP clients."""
        return [
            Tool(
                name=info["schema"]["name"],
                description=info["schema"]["description"],
                inputSchema=info["schema"]["inputSchema"],
            )
            for info in TOOL_REGISTRY.values()
        ]

    # Register call_tool handler
    @server.call_tool()  # type: ignore[misc]
    async def handle_call_tool(
        name: str, arguments: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Handle tool calls from MCP clients."""
        if name not in TOOL_REGISTRY:
            raise ValueError(f"Unknown tool: {name}")

        tool_info = TOOL_REGISTRY[name]
        tool_function = tool_info["function"]

        # Validate parameters
        params = arguments or {}
        is_valid, error_msg = validate_tool_parameters(name, params)
        if not is_valid:
            raise ValueError(f"Parameter validation failed: {error_msg}")

        # Call the tool function
        try:
            logger.debug(f"Executing tool '{name}' with params: {params}")
            result = await tool_function(**params)

            # Format result for MCP response
            if isinstance(result, dict):
                # If result is already a structured response, convert to text
                import json

                result_text = json.dumps(result, indent=2, default=str)
            else:
                result_text = str(result)

            return [{"type": "text", "text": result_text}]

        except Exception as e:
            logger.error(f"Error executing tool '{name}': {e}")
            raise

    # Count successful registrations
    registered_count = len(TOOL_REGISTRY)

    logger.info(f"Successfully registered {registered_count} tools")

    # Log summary by module
    module_counts: dict[str, Any] = {}
    for tool_info in TOOL_REGISTRY.values():
        module = tool_info["module"]
        module_counts[module] = module_counts.get(module, 0) + 1

    logger.info("Tools registered by module:")
    for module, count in sorted(module_counts.items()):
        logger.info(f"  - {module}: {count} tools")


def get_tool_discovery_info() -> dict[str, Any]:
    """Get comprehensive tool discovery information.
    Returns:
        Dictionary containing tool discovery metadata
    """
    tools_by_module: dict[str, Any] = {}
    total_tools = len(TOOL_REGISTRY)

    for tool_name, tool_info in TOOL_REGISTRY.items():
        module = tool_info["module"]
        if module not in tools_by_module:
            tools_by_module[module] = []

        tools_by_module[module].append(
            {
                "name": tool_name,
                "description": tool_info["schema"]["description"],
                "required_params": tool_info["schema"]["inputSchema"].get(
                    "required", []
                ),
                "optional_params": [
                    param
                    for param in tool_info["schema"]["inputSchema"]
                    .get("properties", {})
                    .keys()
                    if param
                    not in tool_info["schema"]["inputSchema"].get("required", [])
                ],
            }
        )

    return {
        "total_tools": total_tools,
        "modules": {
            module: {"tool_count": len(tools), "tools": tools}
            for module, tools in tools_by_module.items()
        },
        "module_summary": {
            module: len(tools) for module, tools in tools_by_module.items()
        },
    }


# Export main functions
__all__ = [
    "TOOL_REGISTRY",
    "get_all_tools",
    "get_tool_by_name",
    "get_tools_by_module",
    "validate_tool_parameters",
    "register_all_tools",
    "get_tool_discovery_info",
]
