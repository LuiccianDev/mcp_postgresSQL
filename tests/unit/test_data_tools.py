"""Unit tests for data tools module."""

from unittest.mock import AsyncMock, patch

import pytest

from src.mcp_postgres.tools.data_tools import (
    bulk_insert,
    delete_data,
    insert_data,
    update_data,
)


class TestInsertData:
    """Test cases for insert_data tool."""

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all external dependencies."""
        with patch("src.mcp_postgres.tools.data_tools.connection_manager") as mock_conn, \
             patch("src.mcp_postgres.tools.data_tools.check_table_access") as mock_access, \
             patch("src.mcp_postgres.tools.data_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.data_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.data_tools.validate_table_name") as mock_table_name, \
             patch("src.mcp_postgres.tools.data_tools.validate_column_name") as mock_column_name:

            mock_conn.execute_query = AsyncMock()
            mock_access.return_value = True
            mock_sanitize.side_effect = lambda x: x
            mock_validate.return_value = (True, None)

            yield {
                "connection_manager": mock_conn,
                "check_table_access": mock_access,
                "sanitize_parameters": mock_sanitize,
                "validate_query_permissions": mock_validate,
                "validate_table_name": mock_table_name,
                "validate_column_name": mock_column_name,
            }

    @pytest.mark.asyncio
    async def test_insert_data_success(self, mock_dependencies):
        """Test successful data insertion."""
        # Setup
        mock_dependencies["connection_manager"].execute_query.return_value = "INSERT 0 1"

        # Execute
        result = await insert_data(
            table_name="users",
            data={"name": "John", "email": "john@example.com"}
        )

        # Verify
        assert result["success"] is True
        assert result["data"]["inserted"] is True
        assert result["data"]["table_name"] == "users"
        assert result["data"]["rows_affected"] == 1

        # Verify query execution
        mock_dependencies["connection_manager"].execute_query.assert_called_once()
        call_args = mock_dependencies["connection_manager"].execute_query.call_args
        assert "INSERT INTO users" in call_args[1]["query"]
        assert call_args[1]["parameters"] == ["John", "john@example.com"]

    @pytest.mark.asyncio
    async def test_insert_data_with_return_columns(self, mock_dependencies):
        """Test data insertion with return columns."""
        # Setup - create a proper mock record
        mock_result = {"id": 1, "created_at": "2023-01-01"}
        mock_dependencies["connection_manager"].execute_query.return_value = mock_result

        # Execute
        result = await insert_data(
            table_name="users",
            data={"name": "John"},
            return_columns=["id", "created_at"]
        )

        # Verify
        assert result["success"] is True
        assert result["data"]["inserted"] is True
        assert "returned_data" in result["data"]

        # Verify RETURNING clause
        call_args = mock_dependencies["connection_manager"].execute_query.call_args
        assert "RETURNING id, created_at" in call_args[1]["query"]

    @pytest.mark.asyncio
    async def test_insert_data_on_conflict_ignore(self, mock_dependencies):
        """Test data insertion with conflict ignore."""
        # Setup
        mock_dependencies["connection_manager"].execute_query.return_value = "INSERT 0 0"

        # Execute
        result = await insert_data(
            table_name="users",
            data={"name": "John"},
            on_conflict="ignore"
        )

        # Verify
        assert result["success"] is True
        assert result["data"]["inserted"] is False  # No rows affected

        # Verify ON CONFLICT clause
        call_args = mock_dependencies["connection_manager"].execute_query.call_args
        assert "ON CONFLICT DO NOTHING" in call_args[1]["query"]

    @pytest.mark.asyncio
    async def test_insert_data_on_conflict_update(self, mock_dependencies):
        """Test data insertion with conflict update."""
        # Setup
        mock_dependencies["connection_manager"].execute_query.return_value = "INSERT 0 1"

        # Execute
        result = await insert_data(
            table_name="users",
            data={"name": "John", "email": "john@example.com"},
            on_conflict="update"
        )

        # Verify
        assert result["success"] is True

        # Verify ON CONFLICT UPDATE clause
        call_args = mock_dependencies["connection_manager"].execute_query.call_args
        assert "ON CONFLICT DO UPDATE SET" in call_args[1]["query"]
        assert "name = EXCLUDED.name" in call_args[1]["query"]

    @pytest.mark.asyncio
    async def test_insert_data_validation_errors(self, mock_dependencies):
        """Test validation error handling."""
        # Test empty data
        result = await insert_data("users", {})
        assert "error" in result
        assert "non-empty dictionary" in result["error"]["message"]

        # Test invalid on_conflict
        result = await insert_data("users", {"name": "John"}, on_conflict="invalid")
        assert "error" in result
        assert "on_conflict must be one of" in result["error"]["message"]

    @pytest.mark.asyncio
    async def test_insert_data_security_error(self, mock_dependencies):
        """Test security error handling."""
        # Setup
        mock_dependencies["check_table_access"].return_value = False

        # Execute
        result = await insert_data("users", {"name": "John"})

        # Verify
        assert "error" in result
        assert "Access denied" in result["error"]["message"]


class TestUpdateData:
    """Test cases for update_data tool."""

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all external dependencies."""
        with patch("src.mcp_postgres.tools.data_tools.connection_manager") as mock_conn, \
             patch("src.mcp_postgres.tools.data_tools.check_table_access") as mock_access, \
             patch("src.mcp_postgres.tools.data_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.data_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.data_tools.validate_table_name") as mock_table_name, \
             patch("src.mcp_postgres.tools.data_tools.validate_column_name") as mock_column_name:

            mock_conn.execute_query = AsyncMock()
            mock_access.return_value = True
            mock_sanitize.side_effect = lambda x: x
            mock_validate.return_value = (True, None)

            yield {
                "connection_manager": mock_conn,
                "check_table_access": mock_access,
                "sanitize_parameters": mock_sanitize,
                "validate_query_permissions": mock_validate,
                "validate_table_name": mock_table_name,
                "validate_column_name": mock_column_name,
            }

    @pytest.mark.asyncio
    async def test_update_data_success(self, mock_dependencies):
        """Test successful data update."""
        # Setup
        mock_dependencies["connection_manager"].execute_query.return_value = "UPDATE 2"

        # Execute
        result = await update_data(
            table_name="users",
            data={"name": "Jane"},
            where_conditions={"id": 1}
        )

        # Verify
        assert result["success"] is True
        assert result["data"]["updated"] is True
        assert result["data"]["rows_affected"] == 2

        # Verify query structure
        call_args = mock_dependencies["connection_manager"].execute_query.call_args
        query = call_args[1]["query"]
        assert "UPDATE users SET name = $1 WHERE id = $2" in query
        assert call_args[1]["parameters"] == ["Jane", 1]

    @pytest.mark.asyncio
    async def test_update_data_with_limit(self, mock_dependencies):
        """Test data update with limit."""
        # Setup
        mock_dependencies["connection_manager"].execute_query.return_value = "UPDATE 1"

        # Execute
        result = await update_data(
            table_name="users",
            data={"name": "Jane"},
            where_conditions={"status": "active"},
            limit=5
        )

        # Verify
        assert result["success"] is True

        # Verify limit is applied via subquery
        call_args = mock_dependencies["connection_manager"].execute_query.call_args
        query = call_args[1]["query"]
        assert "LIMIT 5" in query
        assert "ctid IN" in query

    @pytest.mark.asyncio
    async def test_update_data_with_return_columns(self, mock_dependencies):
        """Test data update with return columns."""
        # Setup - create proper mock records
        mock_result = [{"id": 1, "name": "Jane"}]
        mock_dependencies["connection_manager"].execute_query.return_value = mock_result

        # Execute
        result = await update_data(
            table_name="users",
            data={"name": "Jane"},
            where_conditions={"id": 1},
            return_columns=["id", "name"]
        )

        # Verify
        assert result["success"] is True
        assert "returned_data" in result["data"]
        assert len(result["data"]["returned_data"]) == 1

    @pytest.mark.asyncio
    async def test_update_data_validation_errors(self, mock_dependencies):
        """Test validation error handling."""
        # Test empty data
        result = await update_data("users", {}, {"id": 1})
        assert "error" in result
        assert "non-empty dictionary" in result["error"]["message"]

        # Test empty where conditions
        result = await update_data("users", {"name": "Jane"}, {})
        assert "error" in result
        assert "Where conditions must be" in result["error"]["message"]

        # Test invalid limit
        result = await update_data("users", {"name": "Jane"}, {"id": 1}, limit=0)
        assert "error" in result
        assert "positive integer" in result["error"]["message"]


class TestDeleteData:
    """Test cases for delete_data tool."""

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all external dependencies."""
        with patch("src.mcp_postgres.tools.data_tools.connection_manager") as mock_conn, \
             patch("src.mcp_postgres.tools.data_tools.check_table_access") as mock_access, \
             patch("src.mcp_postgres.tools.data_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.data_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.data_tools.validate_table_name") as mock_table_name, \
             patch("src.mcp_postgres.tools.data_tools.validate_column_name") as mock_column_name:

            mock_conn.execute_query = AsyncMock()
            mock_access.return_value = True
            mock_sanitize.side_effect = lambda x: x
            mock_validate.return_value = (True, None)

            yield {
                "connection_manager": mock_conn,
                "check_table_access": mock_access,
                "sanitize_parameters": mock_sanitize,
                "validate_query_permissions": mock_validate,
                "validate_table_name": mock_table_name,
                "validate_column_name": mock_column_name,
            }

    @pytest.mark.asyncio
    async def test_delete_data_success(self, mock_dependencies):
        """Test successful data deletion."""
        # Setup
        mock_dependencies["connection_manager"].execute_query.return_value = "DELETE 3"

        # Execute
        result = await delete_data(
            table_name="users",
            where_conditions={"status": "inactive"},
            confirm_delete=True
        )

        # Verify
        assert result["success"] is True
        assert result["data"]["deleted"] is True
        assert result["data"]["rows_affected"] == 3

        # Verify query structure
        call_args = mock_dependencies["connection_manager"].execute_query.call_args
        query = call_args[1]["query"]
        assert "DELETE FROM users WHERE status = $1" in query
        assert call_args[1]["parameters"] == ["inactive"]

    @pytest.mark.asyncio
    async def test_delete_data_without_confirmation(self, mock_dependencies):
        """Test delete data without confirmation."""
        # Execute
        result = await delete_data(
            table_name="users",
            where_conditions={"id": 1}
        )

        # Verify
        assert "error" in result
        assert "explicit confirmation" in result["error"]["message"]

    @pytest.mark.asyncio
    async def test_delete_data_with_limit(self, mock_dependencies):
        """Test data deletion with limit."""
        # Setup
        mock_dependencies["connection_manager"].execute_query.return_value = "DELETE 2"

        # Execute
        result = await delete_data(
            table_name="users",
            where_conditions={"status": "inactive"},
            limit=2,
            confirm_delete=True
        )

        # Verify
        assert result["success"] is True

        # Verify limit is applied via subquery
        call_args = mock_dependencies["connection_manager"].execute_query.call_args
        query = call_args[1]["query"]
        assert "LIMIT 2" in query
        assert "ctid IN" in query

    @pytest.mark.asyncio
    async def test_delete_data_with_return_columns(self, mock_dependencies):
        """Test data deletion with return columns."""
        # Setup - create proper mock records
        mock_result = [{"id": 1, "name": "John"}]
        mock_dependencies["connection_manager"].execute_query.return_value = mock_result

        # Execute
        result = await delete_data(
            table_name="users",
            where_conditions={"id": 1},
            return_columns=["id", "name"],
            confirm_delete=True
        )

        # Verify
        assert result["success"] is True
        assert "returned_data" in result["data"]
        assert len(result["data"]["returned_data"]) == 1

    @pytest.mark.asyncio
    async def test_delete_data_validation_errors(self, mock_dependencies):
        """Test validation error handling."""
        # Test empty where conditions
        result = await delete_data("users", {}, confirm_delete=True)
        assert "error" in result
        assert "Where conditions must be" in result["error"]["message"]

        # Test invalid limit
        result = await delete_data("users", {"id": 1}, limit=-1, confirm_delete=True)
        assert "error" in result
        assert "positive integer" in result["error"]["message"]


class TestBulkInsert:
    """Test cases for bulk_insert tool."""

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all external dependencies."""
        with patch("src.mcp_postgres.tools.data_tools.connection_manager") as mock_conn, \
             patch("src.mcp_postgres.tools.data_tools.check_table_access") as mock_access, \
             patch("src.mcp_postgres.tools.data_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.data_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.data_tools.validate_table_name") as mock_table_name, \
             patch("src.mcp_postgres.tools.data_tools.validate_column_name") as mock_column_name:

            mock_conn.execute_query = AsyncMock()
            mock_access.return_value = True
            mock_sanitize.side_effect = lambda x: x
            mock_validate.return_value = (True, None)

            yield {
                "connection_manager": mock_conn,
                "check_table_access": mock_access,
                "sanitize_parameters": mock_sanitize,
                "validate_query_permissions": mock_validate,
                "validate_table_name": mock_table_name,
                "validate_column_name": mock_column_name,
            }

    @pytest.mark.asyncio
    async def test_bulk_insert_success(self, mock_dependencies):
        """Test successful bulk insertion."""
        # Setup
        data = [
            {"name": "John", "email": "john@example.com"},
            {"name": "Jane", "email": "jane@example.com"},
            {"name": "Bob", "email": "bob@example.com"}
        ]

        # Execute
        result = await bulk_insert(
            table_name="users",
            data=data,
            batch_size=2
        )

        # Verify
        assert result["success"] is True
        assert result["data"]["bulk_inserted"] is True
        assert result["data"]["total_records"] == 3
        assert result["data"]["processed_records"] == 3
        assert result["data"]["successful_batches"] == 2  # 2 records + 1 record
        assert result["data"]["failed_batches"] == 0

        # Verify multiple batch executions
        assert mock_dependencies["connection_manager"].execute_query.call_count == 2

    @pytest.mark.asyncio
    async def test_bulk_insert_with_conflict_handling(self, mock_dependencies):
        """Test bulk insertion with conflict handling."""
        # Setup
        data = [{"name": "John", "email": "john@example.com"}]

        # Execute
        result = await bulk_insert(
            table_name="users",
            data=data,
            on_conflict="ignore"
        )

        # Verify
        assert result["success"] is True

        # Verify ON CONFLICT clause
        call_args = mock_dependencies["connection_manager"].execute_query.call_args
        query = call_args[1]["query"]
        assert "ON CONFLICT DO NOTHING" in query

    @pytest.mark.asyncio
    async def test_bulk_insert_batch_failure(self, mock_dependencies):
        """Test bulk insertion with batch failure."""
        # Setup
        data = [
            {"name": "John", "email": "john@example.com"},
            {"name": "Jane", "email": "jane@example.com"}
        ]

        # Make second batch fail
        mock_dependencies["connection_manager"].execute_query.side_effect = [
            None,  # First batch succeeds
            Exception("Database error")  # Second batch fails
        ]

        # Execute
        result = await bulk_insert(
            table_name="users",
            data=data,
            batch_size=1,
            on_conflict="ignore"  # Don't stop on error
        )

        # Verify
        assert result["success"] is True
        assert result["data"]["successful_batches"] == 1
        assert result["data"]["failed_batches"] == 1
        assert len(result["data"]["errors"]) == 1

    @pytest.mark.asyncio
    async def test_bulk_insert_validation_errors(self, mock_dependencies):
        """Test validation error handling."""
        # Test empty data
        result = await bulk_insert("users", [])
        assert "error" in result
        assert "non-empty list" in result["error"]["message"]

        # Test invalid batch size
        result = await bulk_insert("users", [{"name": "John"}], batch_size=0)
        assert "error" in result
        assert "between 1 and 10000" in result["error"]["message"]

        # Test inconsistent columns
        data = [
            {"name": "John", "email": "john@example.com"},
            {"name": "Jane"}  # Missing email
        ]
        result = await bulk_insert("users", data)
        assert "error" in result
        assert "different columns" in result["error"]["message"]

    @pytest.mark.asyncio
    async def test_bulk_insert_large_dataset(self, mock_dependencies):
        """Test bulk insertion with large dataset."""
        # Setup
        data = [{"name": f"User{i}", "email": f"user{i}@example.com"} for i in range(2500)]

        # Execute
        result = await bulk_insert(
            table_name="users",
            data=data,
            batch_size=1000
        )

        # Verify
        assert result["success"] is True
        assert result["data"]["total_records"] == 2500
        assert result["data"]["successful_batches"] == 3  # 1000 + 1000 + 500

        # Verify batch processing
        assert mock_dependencies["connection_manager"].execute_query.call_count == 3

    @pytest.mark.asyncio
    async def test_bulk_insert_with_summary(self, mock_dependencies):
        """Test bulk insertion with detailed summary."""
        # Setup
        data = [{"name": "John", "email": "john@example.com"}]

        # Execute
        result = await bulk_insert(
            table_name="users",
            data=data,
            return_summary=True
        )

        # Verify
        assert result["success"] is True
        assert "summary" in result["data"]
        assert result["data"]["summary"]["columns"] == ["name", "email"]
        assert "processing_rate_per_sec" in result["data"]["summary"]
