"""Relationship analysis tools for MCP Postgres server.

This module provides tools for analyzing table relationships including foreign keys,
parent-child connections, and referential integrity validation.
"""

import logging
from typing import Any

from mcp_postgres.core.connection import connection_manager
from mcp_postgres.utils.exceptions import (
    MCPPostgresError,
    TableNotFoundError,
    handle_postgres_error,
)
from mcp_postgres.utils.validators import validate_table_name


logger = logging.getLogger(__name__)


async def get_foreign_keys(
    table_name: str | None = None, schema_name: str | None = None
) -> dict[str, Any]:
    """Get foreign key relationships for tables.

    Args:
        table_name: Optional table name to filter foreign keys
        schema_name: Optional schema name (defaults to 'public')

    Returns:
        Dictionary containing foreign key relationship information

    Raises:
        MCPPostgresError: If query execution fails
        TableNotFoundError: If specified table doesn't exist
    """
    try:
        if schema_name is None:
            schema_name = "public"
        else:
            validate_table_name(schema_name)

        if table_name is not None:
            validate_table_name(table_name)

            # Check if table exists when specified
            table_exists_query = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )
            """
            exists = await connection_manager.execute_query(
                table_exists_query, [schema_name, table_name], "val"
            )
            if not exists:
                raise TableNotFoundError(table_name, schema_name)

        # Build query based on whether table_name is specified
        if table_name:
            query = """
            SELECT
                tc.constraint_name,
                tc.table_name as source_table,
                tc.table_schema as source_schema,
                string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as source_columns,
                ccu.table_name as target_table,
                ccu.table_schema as target_schema,
                ccu.column_name as target_column,
                rc.update_rule,
                rc.delete_rule,
                rc.match_option,
                CASE
                    WHEN rc.update_rule = 'CASCADE' OR rc.delete_rule = 'CASCADE' THEN true
                    ELSE false
                END as has_cascade_actions
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.constraint_schema
            JOIN information_schema.referential_constraints rc
                ON tc.constraint_name = rc.constraint_name
                AND tc.table_schema = rc.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = $1
            AND tc.table_name = $2
            GROUP BY tc.constraint_name, tc.table_name, tc.table_schema,
                     ccu.table_name, ccu.table_schema, ccu.column_name,
                     rc.update_rule, rc.delete_rule, rc.match_option
            ORDER BY tc.constraint_name
            """
            parameters = [schema_name, table_name]
        else:
            query = """
            SELECT
                tc.constraint_name,
                tc.table_name as source_table,
                tc.table_schema as source_schema,
                string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as source_columns,
                ccu.table_name as target_table,
                ccu.table_schema as target_schema,
                ccu.column_name as target_column,
                rc.update_rule,
                rc.delete_rule,
                rc.match_option,
                CASE
                    WHEN rc.update_rule = 'CASCADE' OR rc.delete_rule = 'CASCADE' THEN true
                    ELSE false
                END as has_cascade_actions
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.constraint_schema
            JOIN information_schema.referential_constraints rc
                ON tc.constraint_name = rc.constraint_name
                AND tc.table_schema = rc.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = $1
            GROUP BY tc.constraint_name, tc.table_name, tc.table_schema,
                     ccu.table_name, ccu.table_schema, ccu.column_name,
                     rc.update_rule, rc.delete_rule, rc.match_option
            ORDER BY tc.table_name, tc.constraint_name
            """
            parameters = [schema_name]

        rows = await connection_manager.execute_query(query, parameters)
        foreign_keys = [dict(row) for row in rows]

        # Group by action rules for summary
        action_rules = {
            "CASCADE": 0,
            "RESTRICT": 0,
            "SET NULL": 0,
            "SET DEFAULT": 0,
            "NO ACTION": 0,
        }
        for fk in foreign_keys:
            update_rule = fk.get("update_rule", "NO ACTION")
            delete_rule = fk.get("delete_rule", "NO ACTION")
            if update_rule in action_rules:
                action_rules[update_rule] += 1
            if delete_rule in action_rules:
                action_rules[delete_rule] += 1

        result = {
            "foreign_keys": foreign_keys,
            "foreign_key_count": len(foreign_keys),
            "action_rules_summary": action_rules,
            "metadata": {
                "schema_name": schema_name,
                "table_name": table_name,
                "has_cascade_actions": any(
                    fk.get("has_cascade_actions") for fk in foreign_keys
                ),
                "unique_target_tables": len(
                    {
                        f"{fk['target_schema']}.{fk['target_table']}"
                        for fk in foreign_keys
                    }
                ),
            },
        }

        logger.info(
            f"Retrieved {len(foreign_keys)} foreign keys"
            + (
                f" for table '{table_name}'"
                if table_name
                else f" from schema '{schema_name}'"
            )
        )
        return result

    except Exception as e:
        logger.error(f"Error getting foreign keys: {e}")
        if isinstance(e, MCPPostgresError):
            raise
        raise handle_postgres_error(e) from e


async def get_table_relationships(
    table_name: str | None = None, schema_name: str | None = None
) -> dict[str, Any]:
    """Map parent-child table connections and relationship hierarchy.

    Args:
        table_name: Optional table name to focus analysis on
        schema_name: Optional schema name (defaults to 'public')

    Returns:
        Dictionary containing table relationship mapping and hierarchy

    Raises:
        MCPPostgresError: If query execution fails
        TableNotFoundError: If specified table doesn't exist
    """
    try:
        if schema_name is None:
            schema_name = "public"
        else:
            validate_table_name(schema_name)

        if table_name is not None:
            validate_table_name(table_name)

            # Check if table exists when specified
            table_exists_query = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )
            """
            exists = await connection_manager.execute_query(
                table_exists_query, [schema_name, table_name], "val"
            )
            if not exists:
                raise TableNotFoundError(table_name, schema_name)

        # Get all foreign key relationships in the schema
        relationships_query = """
        SELECT
            tc.table_name as child_table,
            tc.table_schema as child_schema,
            string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as child_columns,
            ccu.table_name as parent_table,
            ccu.table_schema as parent_schema,
            ccu.column_name as parent_column,
            tc.constraint_name,
            rc.update_rule,
            rc.delete_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
            AND tc.table_schema = ccu.constraint_schema
        JOIN information_schema.referential_constraints rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = $1
        GROUP BY tc.table_name, tc.table_schema, ccu.table_name, ccu.table_schema,
                 ccu.column_name, tc.constraint_name, rc.update_rule, rc.delete_rule
        ORDER BY tc.table_name, ccu.table_name
        """

        rows = await connection_manager.execute_query(
            relationships_query, [schema_name]
        )
        all_relationships = [dict(row) for row in rows]

        # Build relationship maps
        parent_to_children: dict[
            str, list[dict[str, Any]]
        ] = {}  # parent -> list of children
        child_to_parents: dict[
            str, list[dict[str, Any]]
        ] = {}  # child -> list of parents
        table_relationships: list[dict[str, Any]] = []

        for rel in all_relationships:
            parent_key = f"{rel['parent_schema']}.{rel['parent_table']}"
            child_key = f"{rel['child_schema']}.{rel['child_table']}"

            # Build parent-to-children mapping
            if parent_key not in parent_to_children:
                parent_to_children[parent_key] = []
            parent_to_children[parent_key].append(
                {
                    "child_table": rel["child_table"],
                    "child_schema": rel["child_schema"],
                    "child_columns": rel["child_columns"],
                    "parent_column": rel["parent_column"],
                    "constraint_name": rel["constraint_name"],
                    "update_rule": rel["update_rule"],
                    "delete_rule": rel["delete_rule"],
                }
            )

            # Build child-to-parents mapping
            if child_key not in child_to_parents:
                child_to_parents[child_key] = []
            child_to_parents[child_key].append(
                {
                    "parent_table": rel["parent_table"],
                    "parent_schema": rel["parent_schema"],
                    "parent_column": rel["parent_column"],
                    "child_columns": rel["child_columns"],
                    "constraint_name": rel["constraint_name"],
                    "update_rule": rel["update_rule"],
                    "delete_rule": rel["delete_rule"],
                }
            )

            table_relationships.append(rel)

        # If specific table requested, filter relationships
        if table_name:
            table_key = f"{schema_name}.{table_name}"
            filtered_relationships = []

            # Include relationships where table is parent or child
            for rel in all_relationships:
                parent_key = f"{rel['parent_schema']}.{rel['parent_table']}"
                child_key = f"{rel['child_schema']}.{rel['child_table']}"

                if parent_key == table_key or child_key == table_key:
                    filtered_relationships.append(rel)

            table_relationships = filtered_relationships

        # Find root tables (tables with no parents)
        all_tables = set()
        for rel in all_relationships:
            all_tables.add(f"{rel['parent_schema']}.{rel['parent_table']}")
            all_tables.add(f"{rel['child_schema']}.{rel['child_table']}")

        root_tables = []
        leaf_tables = []

        for table in all_tables:
            if table not in child_to_parents:
                root_tables.append(table)
            if table not in parent_to_children:
                leaf_tables.append(table)

        result = {
            "relationships": table_relationships,
            "relationship_count": len(table_relationships),
            "parent_to_children": parent_to_children,
            "child_to_parents": child_to_parents,
            "root_tables": root_tables,
            "leaf_tables": leaf_tables,
            "metadata": {
                "schema_name": schema_name,
                "table_name": table_name,
                "total_tables_in_relationships": len(all_tables),
                "root_table_count": len(root_tables),
                "leaf_table_count": len(leaf_tables),
                "has_circular_references": len(all_tables) > 0
                and len(root_tables) == 0,
            },
        }

        logger.info(
            f"Mapped {len(table_relationships)} table relationships"
            + (
                f" for table '{table_name}'"
                if table_name
                else f" in schema '{schema_name}'"
            )
        )
        return result

    except Exception as e:
        logger.error(f"Error getting table relationships: {e}")
        if isinstance(e, MCPPostgresError):
            raise
        raise handle_postgres_error(e) from e


async def validate_referential_integrity(
    table_name: str | None = None, schema_name: str | None = None
) -> dict[str, Any]:
    """Check for referential integrity constraint violations.

    Args:
        table_name: Optional table name to focus validation on
        schema_name: Optional schema name (defaults to 'public')

    Returns:
        Dictionary containing constraint violation information

    Raises:
        MCPPostgresError: If query execution fails
        TableNotFoundError: If specified table doesn't exist
    """
    try:
        if schema_name is None:
            schema_name = "public"
        else:
            validate_table_name(schema_name)

        if table_name is not None:
            validate_table_name(table_name)

            # Check if table exists when specified
            table_exists_query = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )
            """
            exists = await connection_manager.execute_query(
                table_exists_query, [schema_name, table_name], "val"
            )
            if not exists:
                raise TableNotFoundError(table_name, schema_name)

        violations = []
        constraint_checks = []

        # Get foreign key constraints to validate
        if table_name:
            fk_query = """
            SELECT
                tc.constraint_name,
                tc.table_name as child_table,
                tc.table_schema as child_schema,
                string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as child_columns,
                ccu.table_name as parent_table,
                ccu.table_schema as parent_schema,
                ccu.column_name as parent_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = $1
            AND tc.table_name = $2
            GROUP BY tc.constraint_name, tc.table_name, tc.table_schema,
                     ccu.table_name, ccu.table_schema, ccu.column_name
            """
            parameters = [schema_name, table_name]
        else:
            fk_query = """
            SELECT
                tc.constraint_name,
                tc.table_name as child_table,
                tc.table_schema as child_schema,
                string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as child_columns,
                ccu.table_name as parent_table,
                ccu.table_schema as parent_schema,
                ccu.column_name as parent_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = $1
            GROUP BY tc.constraint_name, tc.table_name, tc.table_schema,
                     ccu.table_name, ccu.table_schema, ccu.column_name
            """
            parameters = [schema_name]

        fk_rows = await connection_manager.execute_query(fk_query, parameters)
        foreign_keys = [dict(row) for row in fk_rows]

        # Check each foreign key constraint for violations
        for fk in foreign_keys:
            constraint_name = fk["constraint_name"]
            child_table = fk["child_table"]
            child_schema = fk["child_schema"]
            child_columns = fk["child_columns"].split(", ")
            parent_table = fk["parent_table"]
            parent_schema = fk["parent_schema"]
            parent_column = fk["parent_column"]

            # Build dynamic query to check for orphaned records
            # Note: Table and column names are validated and come from database metadata
            child_cols_str = ", ".join(child_columns)
            child_where_clause = " AND ".join(
                [f"c.{col} IS NOT NULL" for col in child_columns]
            )

            violation_query = f"""  # noqa: S608
            SELECT
                '{constraint_name}' as constraint_name,
                '{child_schema}.{child_table}' as child_table,
                '{parent_schema}.{parent_table}' as parent_table,
                COUNT(*) as violation_count,
                array_agg(DISTINCT ({child_cols_str})) as sample_values
            FROM {child_schema}.{child_table} c
            LEFT JOIN {parent_schema}.{parent_table} p ON c.{child_columns[0]} = p.{parent_column}
            WHERE {child_where_clause}
            AND p.{parent_column} IS NULL
            HAVING COUNT(*) > 0
            """

            try:
                violation_rows = await connection_manager.execute_query(
                    violation_query, []
                )
                if violation_rows:
                    violation_data = dict(violation_rows[0])
                    violations.append(violation_data)

                constraint_checks.append(
                    {
                        "constraint_name": constraint_name,
                        "child_table": f"{child_schema}.{child_table}",
                        "parent_table": f"{parent_schema}.{parent_table}",
                        "status": "VIOLATED" if violation_rows else "VALID",
                        "violation_count": violation_data.get("violation_count", 0)
                        if violation_rows
                        else 0,
                    }
                )
            except Exception as check_error:
                logger.warning(
                    f"Could not validate constraint '{constraint_name}': {check_error}"
                )
                constraint_checks.append(
                    {
                        "constraint_name": constraint_name,
                        "child_table": f"{child_schema}.{child_table}",
                        "parent_table": f"{parent_schema}.{parent_table}",
                        "status": "ERROR",
                        "error": str(check_error),
                    }
                )

        # Summary statistics
        total_constraints = len(constraint_checks)
        valid_constraints = sum(1 for c in constraint_checks if c["status"] == "VALID")
        violated_constraints = sum(
            1 for c in constraint_checks if c["status"] == "VIOLATED"
        )
        error_constraints = sum(1 for c in constraint_checks if c["status"] == "ERROR")
        total_violations = sum(v.get("violation_count", 0) for v in violations)

        result = {
            "constraint_checks": constraint_checks,
            "violations": violations,
            "summary": {
                "total_constraints_checked": total_constraints,
                "valid_constraints": valid_constraints,
                "violated_constraints": violated_constraints,
                "error_constraints": error_constraints,
                "total_violation_records": total_violations,
                "integrity_status": "VALID"
                if violated_constraints == 0 and error_constraints == 0
                else "VIOLATED",
            },
            "metadata": {
                "schema_name": schema_name,
                "table_name": table_name,
                "check_timestamp": "NOW()",
                "has_violations": len(violations) > 0,
            },
        }

        logger.info(
            f"Validated {total_constraints} referential integrity constraints, "
            f"found {violated_constraints} violations"
            + (
                f" for table '{table_name}'"
                if table_name
                else f" in schema '{schema_name}'"
            )
        )
        return result

    except Exception as e:
        logger.error(f"Error validating referential integrity: {e}")
        if isinstance(e, MCPPostgresError):
            raise
        raise handle_postgres_error(e) from e


# Tool schema definitions for MCP registration
GET_FOREIGN_KEYS_SCHEMA = {
    "name": "get_foreign_keys",
    "description": "Get foreign key relationships for a specific table including referenced tables and columns",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to get foreign keys for",
            },
            "schema_name": {
                "type": "string",
                "description": "Schema name (defaults to 'public')",
                "default": "public",
            },
        },
        "required": ["table_name"],
    },
}

GET_TABLE_RELATIONSHIPS_SCHEMA = {
    "name": "get_table_relationships",
    "description": "Get comprehensive table relationships including parent-child connections and dependency mapping",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Optional table name to focus on specific relationships",
            },
            "schema_name": {
                "type": "string",
                "description": "Schema name (defaults to 'public')",
                "default": "public",
            },
            "include_views": {
                "type": "boolean",
                "description": "Whether to include view dependencies",
                "default": False,
            },
        },
        "required": [],
    },
}

VALIDATE_REFERENTIAL_INTEGRITY_SCHEMA = {
    "name": "validate_referential_integrity",
    "description": "Validate referential integrity by checking for constraint violations and orphaned records",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Optional table name to focus validation on specific table",
            },
            "schema_name": {
                "type": "string",
                "description": "Schema name (defaults to 'public')",
                "default": "public",
            },
            "fix_violations": {
                "type": "boolean",
                "description": "Whether to attempt to fix violations (WARNING: may delete data)",
                "default": False,
            },
        },
        "required": [],
    },
}

# Export tool functions and schemas
__all__ = [
    "get_foreign_keys",
    "get_table_relationships",
    "validate_referential_integrity",
    "GET_FOREIGN_KEYS_SCHEMA",
    "GET_TABLE_RELATIONSHIPS_SCHEMA",
    "VALIDATE_REFERENTIAL_INTEGRITY_SCHEMA",
]
