"""Unit tests for relation tools module."""

from unittest.mock import AsyncMock, patch

import pytest

from src.mcp_postgres.tools.relation_tools import (
    get_foreign_keys,
    get_table_relationships,
    validate_referential_integrity,
)
from src.mcp_postgres.utils.exceptions import (
    MCPPostgresError,
    TableNotFoundError,
)


class TestGetForeignKeys:
    """Test cases for get_foreign_keys function."""

    @pytest.fixture
    def mock_foreign_key_rows(self):
        """Mock foreign key data from database query."""
        return [
            {
                "constraint_name": "fk_orders_user_id",
                "source_table": "orders",
                "source_schema": "public",
                "source_columns": "user_id",
                "target_table": "users",
                "target_schema": "public",
                "target_column": "id",
                "update_rule": "NO ACTION",
                "delete_rule": "CASCADE",
                "match_option": "NONE",
                "has_cascade_actions": True,
            },
            {
                "constraint_name": "fk_order_items_order_id",
                "source_table": "order_items",
                "source_schema": "public",
                "source_columns": "order_id",
                "target_table": "orders",
                "target_schema": "public",
                "target_column": "id",
                "update_rule": "RESTRICT",
                "delete_rule": "RESTRICT",
                "match_option": "NONE",
                "has_cascade_actions": False,
            },
        ]

    @pytest.mark.asyncio
    async def test_get_foreign_keys_all_tables(self, mock_foreign_key_rows):
        """Test getting all foreign keys from schema."""
        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=mock_foreign_key_rows)

            result = await get_foreign_keys()

            assert result["foreign_key_count"] == 2
            assert len(result["foreign_keys"]) == 2
            assert result["metadata"]["schema_name"] == "public"
            assert result["metadata"]["table_name"] is None
            assert result["metadata"]["has_cascade_actions"] is True
            assert result["metadata"]["unique_target_tables"] == 2

            # Check action rules summary
            assert "CASCADE" in result["action_rules_summary"]
            assert "RESTRICT" in result["action_rules_summary"]

    @pytest.mark.asyncio
    async def test_get_foreign_keys_specific_table(self, mock_foreign_key_rows):
        """Test getting foreign keys for specific table."""
        # Mock table existence check
        mock_filtered_rows = [mock_foreign_key_rows[0]]  # Only orders table FK

        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[True, mock_filtered_rows])  # exists, then FKs

            result = await get_foreign_keys("orders", "public")

            assert result["foreign_key_count"] == 1
            assert result["foreign_keys"][0]["source_table"] == "orders"
            assert result["metadata"]["table_name"] == "orders"

            # Verify table existence was checked
            assert mock_conn.execute_query.call_count == 2

    @pytest.mark.asyncio
    async def test_get_foreign_keys_table_not_found(self):
        """Test error when specified table doesn't exist."""
        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=False)  # Table doesn't exist

            with pytest.raises(TableNotFoundError):
                await get_foreign_keys("nonexistent", "public")

    @pytest.mark.asyncio
    async def test_get_foreign_keys_database_error(self):
        """Test handling of database errors."""
        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=Exception("Database connection failed"))

            with pytest.raises(MCPPostgresError):
                await get_foreign_keys()

    @pytest.mark.asyncio
    async def test_get_foreign_keys_custom_schema(self, mock_foreign_key_rows):
        """Test getting foreign keys from custom schema."""
        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=mock_foreign_key_rows)

            result = await get_foreign_keys(schema_name="custom_schema")

            assert result["metadata"]["schema_name"] == "custom_schema"


class TestGetTableRelationships:
    """Test cases for get_table_relationships function."""

    @pytest.fixture
    def mock_relationship_rows(self):
        """Mock relationship data from database query."""
        return [
            {
                "child_table": "orders",
                "child_schema": "public",
                "child_columns": "user_id",
                "parent_table": "users",
                "parent_schema": "public",
                "parent_column": "id",
                "constraint_name": "fk_orders_user_id",
                "update_rule": "NO ACTION",
                "delete_rule": "CASCADE",
            },
            {
                "child_table": "order_items",
                "child_schema": "public",
                "child_columns": "order_id",
                "parent_table": "orders",
                "parent_schema": "public",
                "parent_column": "id",
                "constraint_name": "fk_order_items_order_id",
                "update_rule": "RESTRICT",
                "delete_rule": "RESTRICT",
            },
            {
                "child_table": "order_items",
                "child_schema": "public",
                "child_columns": "product_id",
                "parent_table": "products",
                "parent_schema": "public",
                "parent_column": "id",
                "constraint_name": "fk_order_items_product_id",
                "update_rule": "NO ACTION",
                "delete_rule": "NO ACTION",
            },
        ]

    @pytest.mark.asyncio
    async def test_get_table_relationships_all_tables(self, mock_relationship_rows):
        """Test getting all table relationships from schema."""
        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=mock_relationship_rows)

            result = await get_table_relationships()

            assert result["relationship_count"] == 3
            assert len(result["relationships"]) == 3
            assert result["metadata"]["schema_name"] == "public"
            assert result["metadata"]["table_name"] is None

            # Check parent-to-children mapping
            assert "public.users" in result["parent_to_children"]
            assert "public.orders" in result["parent_to_children"]
            assert "public.products" in result["parent_to_children"]

            # Check child-to-parents mapping
            assert "public.orders" in result["child_to_parents"]
            assert "public.order_items" in result["child_to_parents"]

            # Check root and leaf tables
            assert "public.users" in result["root_tables"]
            assert "public.products" in result["root_tables"]
            assert "public.order_items" in result["leaf_tables"]

            assert result["metadata"]["total_tables_in_relationships"] == 4
            assert result["metadata"]["root_table_count"] == 2
            assert result["metadata"]["leaf_table_count"] == 1

    @pytest.mark.asyncio
    async def test_get_table_relationships_specific_table(self, mock_relationship_rows):
        """Test getting relationships for specific table."""
        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[True, mock_relationship_rows])  # exists, then relationships

            result = await get_table_relationships("orders", "public")

            assert result["metadata"]["table_name"] == "orders"
            # Should include relationships where orders is parent or child
            orders_relationships = [
                rel for rel in result["relationships"]
                if rel["parent_table"] == "orders" or rel["child_table"] == "orders"
            ]
            assert len(orders_relationships) >= 1

    @pytest.mark.asyncio
    async def test_get_table_relationships_table_not_found(self):
        """Test error when specified table doesn't exist."""
        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=False)  # Table doesn't exist

            with pytest.raises(TableNotFoundError):
                await get_table_relationships("nonexistent", "public")

    @pytest.mark.asyncio
    async def test_get_table_relationships_circular_reference_detection(self):
        """Test detection of circular references."""
        # Mock data with circular reference (no root tables)
        circular_rows = [
            {
                "child_table": "table_a",
                "child_schema": "public",
                "child_columns": "b_id",
                "parent_table": "table_b",
                "parent_schema": "public",
                "parent_column": "id",
                "constraint_name": "fk_a_b",
                "update_rule": "NO ACTION",
                "delete_rule": "NO ACTION",
            },
            {
                "child_table": "table_b",
                "child_schema": "public",
                "child_columns": "a_id",
                "parent_table": "table_a",
                "parent_schema": "public",
                "parent_column": "id",
                "constraint_name": "fk_b_a",
                "update_rule": "NO ACTION",
                "delete_rule": "NO ACTION",
            },
        ]

        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=circular_rows)

            result = await get_table_relationships()

            assert result["metadata"]["has_circular_references"] is True
            assert len(result["root_tables"]) == 0


class TestValidateReferentialIntegrity:
    """Test cases for validate_referential_integrity function."""

    @pytest.fixture
    def mock_fk_constraints(self):
        """Mock foreign key constraint data."""
        return [
            {
                "constraint_name": "fk_orders_user_id",
                "child_table": "orders",
                "child_schema": "public",
                "child_columns": "user_id",
                "parent_table": "users",
                "parent_schema": "public",
                "parent_column": "id",
            },
            {
                "constraint_name": "fk_order_items_order_id",
                "child_table": "order_items",
                "child_schema": "public",
                "child_columns": "order_id",
                "parent_table": "orders",
                "parent_schema": "public",
                "parent_column": "id",
            },
        ]

    @pytest.fixture
    def mock_violation_data(self):
        """Mock constraint violation data."""
        return [
            {
                "constraint_name": "fk_orders_user_id",
                "child_table": "public.orders",
                "parent_table": "public.users",
                "violation_count": 5,
                "sample_values": [999, 1001, 1002],
            }
        ]

    @pytest.mark.asyncio
    async def test_validate_referential_integrity_all_valid(self, mock_fk_constraints):
        """Test validation when all constraints are valid."""
        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            # First call returns FK constraints, subsequent calls return no violations
            mock_conn.execute_query = AsyncMock(side_effect=[mock_fk_constraints] + [[] for _ in mock_fk_constraints])

            result = await validate_referential_integrity()

            assert result["summary"]["total_constraints_checked"] == 2
            assert result["summary"]["valid_constraints"] == 2
            assert result["summary"]["violated_constraints"] == 0
            assert result["summary"]["integrity_status"] == "VALID"
            assert len(result["violations"]) == 0

    @pytest.mark.asyncio
    async def test_validate_referential_integrity_with_violations(
        self, mock_fk_constraints, mock_violation_data
    ):
        """Test validation when constraints are violated."""
        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            # First call returns FK constraints
            # Second call returns violation for first constraint
            # Third call returns no violation for second constraint
            mock_conn.execute_query = AsyncMock(side_effect=[
                mock_fk_constraints,
                mock_violation_data,  # Violation for first constraint
                [],  # No violation for second constraint
            ])

            result = await validate_referential_integrity()

            assert result["summary"]["total_constraints_checked"] == 2
            assert result["summary"]["valid_constraints"] == 1
            assert result["summary"]["violated_constraints"] == 1
            assert result["summary"]["integrity_status"] == "VIOLATED"
            assert len(result["violations"]) == 1
            assert result["violations"][0]["violation_count"] == 5

    @pytest.mark.asyncio
    async def test_validate_referential_integrity_specific_table(self, mock_fk_constraints):
        """Test validation for specific table."""
        filtered_constraints = [mock_fk_constraints[0]]  # Only orders table constraint

        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[True, filtered_constraints, []])  # exists, FKs, no violations

            result = await validate_referential_integrity("orders", "public")

            assert result["metadata"]["table_name"] == "orders"
            assert result["summary"]["total_constraints_checked"] == 1

    @pytest.mark.asyncio
    async def test_validate_referential_integrity_table_not_found(self):
        """Test error when specified table doesn't exist."""
        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=False)  # Table doesn't exist

            with pytest.raises(TableNotFoundError):
                await validate_referential_integrity("nonexistent", "public")

    @pytest.mark.asyncio
    async def test_validate_referential_integrity_constraint_check_error(self, mock_fk_constraints):
        """Test handling of errors during constraint validation."""
        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            # First call returns FK constraints
            # Second call raises exception during validation
            mock_conn.execute_query = AsyncMock(side_effect=[
                mock_fk_constraints,
                Exception("Permission denied"),
            ])

            result = await validate_referential_integrity()

            assert result["summary"]["total_constraints_checked"] == 2
            assert result["summary"]["error_constraints"] == 2
            assert result["summary"]["integrity_status"] == "VIOLATED"

            # Check that error status is recorded
            error_checks = [c for c in result["constraint_checks"] if c["status"] == "ERROR"]
            assert len(error_checks) == 2

    @pytest.mark.asyncio
    async def test_validate_referential_integrity_database_error(self):
        """Test handling of database connection errors."""
        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=Exception("Database connection failed"))

            with pytest.raises(MCPPostgresError):
                await validate_referential_integrity()

    @pytest.mark.asyncio
    async def test_validate_referential_integrity_custom_schema(self, mock_fk_constraints):
        """Test validation in custom schema."""
        with patch("src.mcp_postgres.tools.relation_tools.connection_manager") as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[mock_fk_constraints] + [[] for _ in mock_fk_constraints])

            result = await validate_referential_integrity(schema_name="custom_schema")

            assert result["metadata"]["schema_name"] == "custom_schema"
