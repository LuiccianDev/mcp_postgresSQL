"""Unit tests for backup tools module."""

import csv
import io
from unittest.mock import AsyncMock, patch

import pytest

from src.mcp_postgres.tools.backup_tools import (
    backup_table,
    export_table_csv,
    import_csv_data,
)


class TestExportTableCsv:
    """Test cases for export_table_csv function."""

    @pytest.mark.asyncio
    async def test_export_table_csv_success_basic(self):
        """Test successful CSV export with basic options."""
        # Mock data
        mock_rows = [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"}
        ]

        # Create mock records that behave like asyncpg Records
        class MockRecord:
            def __init__(self, data):
                self._data = data

            def keys(self):
                return self._data.keys()

            def __getitem__(self, key):
                return self._data[key]

        mock_records = [MockRecord(row) for row in mock_rows]

        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access, \
             patch("src.mcp_postgres.tools.backup_tools.connection_manager") as mock_conn_mgr:

            # Setup mocks
            mock_validate_table.return_value = True
            mock_check_access.return_value = True
            mock_conn_mgr.execute_query = AsyncMock(return_value=mock_records)

            # Execute function
            result = await export_table_csv("users")

            # Verify result
            assert result["success"] is True
            assert "csv_data" in result["data"]
            assert result["data"]["row_count"] == 2
            assert result["data"]["column_count"] == 3
            assert result["data"]["columns"] == ["id", "name", "email"]

            # Verify CSV content
            csv_data = result["data"]["csv_data"]
            csv_reader = csv.reader(io.StringIO(csv_data))
            rows = list(csv_reader)

            # Should have headers + 2 data rows
            assert len(rows) == 3
            assert rows[0] == ["id", "name", "email"]  # headers
            assert "John" in rows[1]
            assert "Jane" in rows[2]

            # Verify connection manager was called correctly
            mock_conn_mgr.execute_query.assert_called_once()
            call_args = mock_conn_mgr.execute_query.call_args
            assert 'SELECT * FROM "users"' in call_args[1]["query"]

    @pytest.mark.asyncio
    async def test_export_table_csv_with_columns_and_where(self):
        """Test CSV export with specific columns and WHERE clause."""
        mock_rows = [{"name": "John", "email": "john@example.com"}]

        class MockRecord:
            def __init__(self, data):
                self._data = data

            def keys(self):
                return self._data.keys()

            def __getitem__(self, key):
                return self._data[key]

        mock_records = [MockRecord(row) for row in mock_rows]

        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access, \
             patch("src.mcp_postgres.tools.backup_tools.validate_query_permissions") as mock_validate_query, \
             patch("src.mcp_postgres.tools.backup_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.backup_tools.connection_manager") as mock_conn_mgr:

            # Setup mocks
            mock_validate_table.return_value = True
            mock_check_access.return_value = True
            mock_validate_query.return_value = (True, None)
            mock_sanitize.return_value = [1]
            mock_conn_mgr.execute_query = AsyncMock(return_value=mock_records)

            # Execute function
            result = await export_table_csv(
                table_name="users",
                columns=["name", "email"],
                where_clause="id = $1",
                parameters=[1],
                limit=10
            )

            # Verify result
            assert result["success"] is True
            assert result["data"]["row_count"] == 1

            # Verify query construction
            call_args = mock_conn_mgr.execute_query.call_args
            query = call_args[1]["query"]
            assert '"name", "email"' in query
            assert 'FROM "users"' in query
            assert "WHERE id = $1" in query
            assert "LIMIT 10" in query

    @pytest.mark.asyncio
    async def test_export_table_csv_no_headers(self):
        """Test CSV export without headers."""
        mock_rows = [{"id": 1, "name": "John"}]

        class MockRecord:
            def __init__(self, data):
                self._data = data

            def keys(self):
                return self._data.keys()

            def __getitem__(self, key):
                return self._data[key]

        mock_records = [MockRecord(row) for row in mock_rows]

        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access, \
             patch("src.mcp_postgres.tools.backup_tools.connection_manager") as mock_conn_mgr:

            # Setup mocks
            mock_validate_table.return_value = True
            mock_check_access.return_value = True
            mock_conn_mgr.execute_query = AsyncMock(return_value=mock_records)

            # Execute function
            result = await export_table_csv("users", include_headers=False)

            # Verify CSV content has no headers
            csv_data = result["data"]["csv_data"]
            csv_reader = csv.reader(io.StringIO(csv_data))
            rows = list(csv_reader)

            # Should have only 1 data row (no headers)
            assert len(rows) == 1
            assert "1" in rows[0]  # First row should be data, not headers

    @pytest.mark.asyncio
    async def test_export_table_csv_invalid_table_name(self):
        """Test CSV export with invalid table name."""
        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table:
            mock_validate_table.return_value = False

            result = await export_table_csv("invalid-table")

            assert "error" in result
            assert "Invalid table name" in result["error"]["message"]

    @pytest.mark.asyncio
    async def test_export_table_csv_access_denied(self):
        """Test CSV export with access denied."""
        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access:

            mock_validate_table.return_value = True
            mock_check_access.return_value = False

            result = await export_table_csv("users")

            assert "error" in result
            assert "Access denied" in result["error"]["message"]

    @pytest.mark.asyncio
    async def test_export_table_csv_empty_result(self):
        """Test CSV export with no data."""
        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access, \
             patch("src.mcp_postgres.tools.backup_tools.connection_manager") as mock_conn_mgr:

            # Setup mocks
            mock_validate_table.return_value = True
            mock_check_access.return_value = True
            mock_conn_mgr.execute_query = AsyncMock(return_value=[])

            # Execute function
            result = await export_table_csv("empty_table")

            # Verify result
            assert result["success"] is True
            assert result["data"]["csv_data"] == ""
            assert result["data"]["row_count"] == 0
            assert result["data"]["column_count"] == 0


class TestImportCsvData:
    """Test cases for import_csv_data function."""

    @pytest.mark.asyncio
    async def test_import_csv_data_success_with_headers(self):
        """Test successful CSV import with headers."""
        csv_data = "id,name,email\n1,John,john@example.com\n2,Jane,jane@example.com"

        # Mock table schema
        mock_schema = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO", "column_default": None},
            {"column_name": "name", "data_type": "text", "is_nullable": "YES", "column_default": None},
            {"column_name": "email", "data_type": "text", "is_nullable": "YES", "column_default": None},
        ]

        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access, \
             patch("src.mcp_postgres.tools.backup_tools.connection_manager") as mock_conn_mgr:

            # Setup mocks
            mock_validate_table.return_value = True
            mock_check_access.return_value = True
            mock_conn_mgr.execute_query = AsyncMock(return_value=mock_schema)
            mock_conn_mgr.execute_transaction = AsyncMock(return_value=["INSERT 0 1", "INSERT 0 1"])

            # Execute function
            result = await import_csv_data("users", csv_data)

            # Verify result
            assert result["success"] is True
            assert result["data"]["total_rows"] == 2
            assert result["data"]["successful_rows"] == 2
            assert result["data"]["failed_rows"] == 0

            # Verify transaction was called
            mock_conn_mgr.execute_transaction.assert_called_once()

    @pytest.mark.asyncio
    async def test_import_csv_data_without_headers(self):
        """Test CSV import without headers."""
        csv_data = "1,John,john@example.com\n2,Jane,jane@example.com"
        columns = ["id", "name", "email"]

        mock_schema = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO", "column_default": None},
            {"column_name": "name", "data_type": "text", "is_nullable": "YES", "column_default": None},
            {"column_name": "email", "data_type": "text", "is_nullable": "YES", "column_default": None},
        ]

        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access, \
             patch("src.mcp_postgres.tools.backup_tools.connection_manager") as mock_conn_mgr:

            # Setup mocks
            mock_validate_table.return_value = True
            mock_check_access.return_value = True
            mock_conn_mgr.execute_query = AsyncMock(return_value=mock_schema)
            mock_conn_mgr.execute_transaction = AsyncMock(return_value=["INSERT 0 1", "INSERT 0 1"])

            # Execute function
            result = await import_csv_data("users", csv_data, has_headers=False, columns=columns)

            # Verify result
            assert result["success"] is True
            assert result["data"]["total_rows"] == 2
            assert result["data"]["successful_rows"] == 2

    @pytest.mark.asyncio
    async def test_import_csv_data_validation_error(self):
        """Test CSV import with validation errors."""
        # Create CSV with mismatched column count to trigger validation error
        csv_data = "id,name,email\n1,John\n2,Jane,jane@example.com"  # First row has missing column

        mock_schema = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO", "column_default": None},
            {"column_name": "name", "data_type": "text", "is_nullable": "YES", "column_default": None},
            {"column_name": "email", "data_type": "text", "is_nullable": "YES", "column_default": None},
        ]

        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access, \
             patch("src.mcp_postgres.tools.backup_tools.connection_manager") as mock_conn_mgr:

            # Setup mocks
            mock_validate_table.return_value = True
            mock_check_access.return_value = True
            mock_conn_mgr.execute_query = AsyncMock(return_value=mock_schema)
            mock_conn_mgr.execute_transaction = AsyncMock(return_value=[])

            # Execute function
            result = await import_csv_data("users", csv_data, validate_data=True)

            # Verify result - should have validation errors due to column count mismatch
            assert result["success"] is True  # Function succeeds but reports errors
            assert result["data"]["total_rows"] == 2
            # At least one row should fail due to column count mismatch
            assert result["data"]["failed_rows"] > 0 or len(result["data"]["errors"]) > 0

    @pytest.mark.asyncio
    async def test_import_csv_data_empty_csv(self):
        """Test CSV import with empty data."""
        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access:

            mock_validate_table.return_value = True
            mock_check_access.return_value = True

            result = await import_csv_data("users", "")

            assert "error" in result
            assert "CSV data cannot be empty" in result["error"]["message"]

    @pytest.mark.asyncio
    async def test_import_csv_data_table_not_found(self):
        """Test CSV import with non-existent table."""
        csv_data = "id,name\n1,John"

        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access, \
             patch("src.mcp_postgres.tools.backup_tools.connection_manager") as mock_conn_mgr:

            # Setup mocks
            mock_validate_table.return_value = True
            mock_check_access.return_value = True
            mock_conn_mgr.execute_query = AsyncMock(return_value=[])  # No schema found

            result = await import_csv_data("nonexistent_table", csv_data)

            assert "error" in result
            assert "not found" in result["error"]["message"]


class TestBackupTable:
    """Test cases for backup_table function."""

    @pytest.mark.asyncio
    async def test_backup_table_structure_and_data_sql(self):
        """Test table backup with structure and data in SQL format."""
        # Mock table schema
        mock_columns = [
            {
                "column_name": "id",
                "data_type": "integer",
                "character_maximum_length": None,
                "numeric_precision": None,
                "numeric_scale": None,
                "is_nullable": "NO",
                "column_default": "nextval('users_id_seq'::regclass)",
                "ordinal_position": 1,
            },
            {
                "column_name": "name",
                "data_type": "text",
                "character_maximum_length": None,
                "numeric_precision": None,
                "numeric_scale": None,
                "is_nullable": "YES",
                "column_default": None,
                "ordinal_position": 2,
            },
        ]

        mock_pk = [{"column_name": "id"}]
        mock_fk = []
        mock_indexes = []
        mock_data = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]

        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access, \
             patch("src.mcp_postgres.tools.backup_tools.connection_manager") as mock_conn_mgr:

            # Setup mocks
            mock_validate_table.return_value = True
            mock_check_access.return_value = True

            # Mock multiple query calls
            mock_conn_mgr.execute_query = AsyncMock()
            mock_conn_mgr.execute_query.side_effect = [
                mock_columns,  # Table definition
                mock_pk,       # Primary key
                mock_fk,       # Foreign keys
                mock_indexes,  # Indexes
                mock_data,     # Table data
            ]

            # Execute function
            result = await backup_table("users", format_type="sql")

            # Verify result
            assert result["success"] is True
            assert "structure" in result["data"]
            assert "data" in result["data"]

            # Verify SQL structure contains CREATE TABLE
            structure_sql = result["data"]["structure"]
            assert "CREATE TABLE" in structure_sql
            assert '"users"' in structure_sql
            assert '"id" integer NOT NULL' in structure_sql
            assert "PRIMARY KEY" in structure_sql

            # Verify SQL data contains INSERT statements
            data_sql = result["data"]["data"]
            assert "INSERT INTO" in data_sql
            assert "John" in data_sql
            assert "Jane" in data_sql

    @pytest.mark.asyncio
    async def test_backup_table_json_format(self):
        """Test table backup in JSON format."""
        mock_columns = [
            {"column_name": "id", "data_type": "integer", "is_nullable": "NO"}
        ]
        mock_pk = []
        mock_fk = []
        mock_indexes = []
        mock_data = [{"id": 1, "name": "John"}]

        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access, \
             patch("src.mcp_postgres.tools.backup_tools.connection_manager") as mock_conn_mgr:

            # Setup mocks
            mock_validate_table.return_value = True
            mock_check_access.return_value = True
            mock_conn_mgr.execute_query = AsyncMock()
            mock_conn_mgr.execute_query.side_effect = [
                mock_columns, mock_pk, mock_fk, mock_indexes, mock_data
            ]

            # Execute function
            result = await backup_table("users", format_type="json")

            # Verify result
            assert result["success"] is True

            # Verify JSON structure
            structure = result["data"]["structure"]
            assert isinstance(structure, dict)
            assert "columns" in structure
            assert "primary_key" in structure
            assert "foreign_keys" in structure
            assert "indexes" in structure

            # Verify JSON data
            data = result["data"]["data"]
            assert isinstance(data, list)
            assert len(data) == 1
            assert data[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_backup_table_structure_only(self):
        """Test table backup with structure only."""
        mock_columns = [
            {
                "column_name": "id",
                "data_type": "integer",
                "is_nullable": "NO",
                "character_maximum_length": None,
                "numeric_precision": None,
                "numeric_scale": None,
                "column_default": None,
                "ordinal_position": 1
            }
        ]
        mock_pk = []
        mock_fk = []
        mock_indexes = []

        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access, \
             patch("src.mcp_postgres.tools.backup_tools.connection_manager") as mock_conn_mgr:

            # Setup mocks
            mock_validate_table.return_value = True
            mock_check_access.return_value = True
            mock_conn_mgr.execute_query = AsyncMock()
            mock_conn_mgr.execute_query.side_effect = [
                mock_columns, mock_pk, mock_fk, mock_indexes
            ]

            # Execute function
            result = await backup_table("users", include_data=False, include_structure=True)

            # Verify result
            assert result["success"] is True
            assert result["data"]["structure"] is not None
            assert result["data"]["data"] is None

    @pytest.mark.asyncio
    async def test_backup_table_data_only(self):
        """Test table backup with data only."""
        mock_data = [{"id": 1, "name": "John"}]

        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access, \
             patch("src.mcp_postgres.tools.backup_tools.connection_manager") as mock_conn_mgr:

            # Setup mocks
            mock_validate_table.return_value = True
            mock_check_access.return_value = True
            mock_conn_mgr.execute_query = AsyncMock(return_value=mock_data)

            # Execute function
            result = await backup_table("users", include_data=True, include_structure=False)

            # Verify result
            assert result["success"] is True
            assert result["data"]["structure"] is None
            assert result["data"]["data"] is not None

    @pytest.mark.asyncio
    async def test_backup_table_invalid_options(self):
        """Test table backup with invalid options."""
        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access:

            mock_validate_table.return_value = True
            mock_check_access.return_value = True

            # Test with both include_data and include_structure False
            result = await backup_table("users", include_data=False, include_structure=False)

            assert "error" in result
            assert "At least one of include_data or include_structure must be True" in result["error"]["message"]

    @pytest.mark.asyncio
    async def test_backup_table_with_where_clause(self):
        """Test table backup with WHERE clause."""
        mock_columns = [
            {
                "column_name": "id",
                "data_type": "integer",
                "is_nullable": "NO",
                "character_maximum_length": None,
                "numeric_precision": None,
                "numeric_scale": None,
                "column_default": None,
                "ordinal_position": 1
            }
        ]
        mock_pk = []
        mock_fk = []
        mock_indexes = []
        mock_data = [{"id": 1, "name": "John"}]

        with patch("src.mcp_postgres.tools.backup_tools.validate_table_name") as mock_validate_table, \
             patch("src.mcp_postgres.tools.backup_tools.check_table_access") as mock_check_access, \
             patch("src.mcp_postgres.tools.backup_tools.validate_query_permissions") as mock_validate_query, \
             patch("src.mcp_postgres.tools.backup_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.backup_tools.connection_manager") as mock_conn_mgr:

            # Setup mocks
            mock_validate_table.return_value = True
            mock_check_access.return_value = True
            mock_validate_query.return_value = (True, None)
            mock_sanitize.return_value = [1]
            mock_conn_mgr.execute_query = AsyncMock()
            mock_conn_mgr.execute_query.side_effect = [
                mock_columns, mock_pk, mock_fk, mock_indexes, mock_data
            ]

            # Execute function
            result = await backup_table(
                "users",
                where_clause="id = $1",
                parameters=[1]
            )

            # Verify result
            assert result["success"] is True

            # Verify WHERE clause was used in data query
            data_query_call = mock_conn_mgr.execute_query.call_args_list[-1]
            assert "WHERE id = $1" in data_query_call[1]["query"]
