"""Unit tests for generation tools module."""

from unittest.mock import patch

import pytest

from src.mcp_postgres.tools.generation_tools import (
    _generate_sample_value,
    _get_required_imports,
    _map_postgres_to_django_field,
    _map_postgres_to_python_type,
    _map_postgres_to_sqlalchemy_type,
    generate_ddl,
    generate_insert_template,
    generate_orm_model,
)


class TestGenerateDDL:
    """Test cases for generate_ddl function."""

    @pytest.fixture
    def mock_connection_manager(self):
        """Mock connection manager for testing."""
        with patch("src.mcp_postgres.tools.generation_tools.connection_manager") as mock:
            yield mock

    @pytest.fixture
    def sample_columns(self):
        """Sample column data for testing."""
        return [
            {
                "column_name": "id",
                "data_type": "integer",
                "character_maximum_length": None,
                "numeric_precision": 32,
                "numeric_scale": 0,
                "is_nullable": "NO",
                "column_default": "nextval('users_id_seq'::regclass)",
                "ordinal_position": 1,
            },
            {
                "column_name": "name",
                "data_type": "character varying",
                "character_maximum_length": 100,
                "numeric_precision": None,
                "numeric_scale": None,
                "is_nullable": "NO",
                "column_default": None,
                "ordinal_position": 2,
            },
            {
                "column_name": "email",
                "data_type": "character varying",
                "character_maximum_length": 255,
                "numeric_precision": None,
                "numeric_scale": None,
                "is_nullable": "YES",
                "column_default": None,
                "ordinal_position": 3,
            },
        ]

    @pytest.fixture
    def sample_pk_columns(self):
        """Sample primary key columns."""
        return [{"column_name": "id"}]

    @pytest.fixture
    def sample_indexes(self):
        """Sample index data."""
        return [
            {
                "indexname": "idx_users_email",
                "indexdef": "CREATE INDEX idx_users_email ON public.users USING btree (email)",
            }
        ]

    @pytest.mark.asyncio
    async def test_generate_ddl_success(
        self, mock_connection_manager, sample_columns, sample_pk_columns, sample_indexes
    ):
        """Test successful DDL generation."""
        # Mock database responses
        async def mock_execute_query(*args, **kwargs):
            if "EXISTS" in args[0]:
                return True
            elif "information_schema.columns" in args[0]:
                return sample_columns
            elif "PRIMARY KEY" in args[0]:
                return sample_pk_columns
            elif "FOREIGN KEY" in args[0]:
                return []
            elif "UNIQUE" in args[0]:
                return []
            elif "pg_indexes" in args[0]:
                return sample_indexes
            return []

        mock_connection_manager.execute_query.side_effect = mock_execute_query

        result = await generate_ddl("users", "public", True)

        assert result["success"] is True
        assert "create_table_ddl" in result["data"]
        assert "CREATE TABLE public.users" in result["data"]["create_table_ddl"]
        assert result["data"]["column_count"] == 3
        assert result["data"]["has_primary_key"] is True
        assert result["data"]["index_count"] == 1

    @pytest.mark.asyncio
    async def test_generate_ddl_table_not_found(self, mock_connection_manager):
        """Test DDL generation when table doesn't exist."""
        async def mock_execute_query(*args, **kwargs):
            return False

        mock_connection_manager.execute_query.side_effect = mock_execute_query

        result = await generate_ddl("nonexistent", "public")

        assert "error" in result
        assert "TABLE_NOT_FOUND" in result["error"]["code"]

    @pytest.mark.asyncio
    async def test_generate_ddl_without_indexes(
        self, mock_connection_manager, sample_columns, sample_pk_columns
    ):
        """Test DDL generation without indexes."""
        async def mock_execute_query(*args, **kwargs):
            if "EXISTS" in args[0]:
                return True
            elif "information_schema.columns" in args[0]:
                return sample_columns
            elif "PRIMARY KEY" in args[0]:
                return sample_pk_columns
            else:
                return []

        mock_connection_manager.execute_query.side_effect = mock_execute_query

        result = await generate_ddl("users", "public", False)

        assert result["success"] is True
        assert "index_statements" not in result["data"]
        assert "index_count" not in result["data"]

    @pytest.mark.asyncio
    async def test_generate_ddl_invalid_table_name(self):
        """Test DDL generation with invalid table name."""
        result = await generate_ddl("", "public")

        assert "error" in result
        assert "VALIDATION_ERROR" in result["error"]["code"]


class TestGenerateInsertTemplate:
    """Test cases for generate_insert_template function."""

    @pytest.fixture
    def mock_connection_manager(self):
        """Mock connection manager for testing."""
        with patch("src.mcp_postgres.tools.generation_tools.connection_manager") as mock:
            yield mock

    @pytest.fixture
    def sample_columns(self):
        """Sample column data for testing."""
        return [
            {
                "column_name": "id",
                "data_type": "integer",
                "is_nullable": "NO",
                "column_default": "nextval('users_id_seq'::regclass)",
                "ordinal_position": 1,
                "character_maximum_length": None,
                "numeric_precision": 32,
                "numeric_scale": 0,
            },
            {
                "column_name": "name",
                "data_type": "character varying",
                "is_nullable": "NO",
                "column_default": None,
                "ordinal_position": 2,
                "character_maximum_length": 100,
                "numeric_precision": None,
                "numeric_scale": None,
            },
            {
                "column_name": "email",
                "data_type": "character varying",
                "is_nullable": "YES",
                "column_default": None,
                "ordinal_position": 3,
                "character_maximum_length": 255,
                "numeric_precision": None,
                "numeric_scale": None,
            },
        ]

    @pytest.mark.asyncio
    async def test_generate_insert_template_success(
        self, mock_connection_manager, sample_columns
    ):
        """Test successful INSERT template generation."""
        async def mock_execute_query(*args, **kwargs):
            if "EXISTS" in args[0]:
                return True
            else:
                return sample_columns

        mock_connection_manager.execute_query.side_effect = mock_execute_query

        result = await generate_insert_template("users", "public", True)

        assert result["success"] is True
        assert "templates" in result["data"]
        assert "required_only" in result["data"]["templates"]
        assert "all_columns" in result["data"]["templates"]
        assert "named_parameters" in result["data"]["templates"]
        assert "with_sample_data" in result["data"]["templates"]
        assert result["data"]["column_summary"]["total_columns"] == 3
        assert result["data"]["column_summary"]["required_columns"] == 1

    @pytest.mark.asyncio
    async def test_generate_insert_template_required_only(
        self, mock_connection_manager, sample_columns
    ):
        """Test INSERT template generation with required columns only."""
        async def mock_execute_query(*args, **kwargs):
            if "EXISTS" in args[0]:
                return True
            else:
                return sample_columns

        mock_connection_manager.execute_query.side_effect = mock_execute_query

        result = await generate_insert_template("users", "public", False)

        assert result["success"] is True
        assert "required_only" in result["data"]["templates"]
        assert "INSERT INTO public.users (name)" in result["data"]["templates"]["required_only"]["sql"]

    @pytest.mark.asyncio
    async def test_generate_insert_template_table_not_found(self, mock_connection_manager):
        """Test INSERT template generation when table doesn't exist."""
        async def mock_execute_query(*args, **kwargs):
            return False

        mock_connection_manager.execute_query.side_effect = mock_execute_query

        result = await generate_insert_template("nonexistent", "public")

        assert "error" in result
        assert "TABLE_NOT_FOUND" in result["error"]["code"]


class TestGenerateORMModel:
    """Test cases for generate_orm_model function."""

    @pytest.fixture
    def mock_connection_manager(self):
        """Mock connection manager for testing."""
        with patch("src.mcp_postgres.tools.generation_tools.connection_manager") as mock:
            yield mock

    @pytest.fixture
    def sample_columns(self):
        """Sample column data for testing."""
        return [
            {
                "column_name": "id",
                "data_type": "integer",
                "character_maximum_length": None,
                "numeric_precision": 32,
                "numeric_scale": 0,
                "is_nullable": "NO",
                "column_default": "nextval('users_id_seq'::regclass)",
                "ordinal_position": 1,
            },
            {
                "column_name": "name",
                "data_type": "character varying",
                "character_maximum_length": 100,
                "numeric_precision": None,
                "numeric_scale": None,
                "is_nullable": "NO",
                "column_default": None,
                "ordinal_position": 2,
            },
        ]

    @pytest.fixture
    def sample_pk_columns(self):
        """Sample primary key columns."""
        return [{"column_name": "id"}]

    @pytest.mark.asyncio
    async def test_generate_orm_model_sqlalchemy(
        self, mock_connection_manager, sample_columns, sample_pk_columns
    ):
        """Test SQLAlchemy model generation."""
        async def mock_execute_query(*args, **kwargs):
            if "EXISTS" in args[0]:
                return True
            elif "information_schema.columns" in args[0]:
                return sample_columns
            elif "PRIMARY KEY" in args[0]:
                return sample_pk_columns
            else:
                return []

        mock_connection_manager.execute_query.side_effect = mock_execute_query

        result = await generate_orm_model("users", "public", "sqlalchemy", "User")

        assert result["success"] is True
        assert result["data"]["model_type"] == "sqlalchemy"
        assert result["data"]["class_name"] == "User"
        assert "class User(Base):" in result["data"]["model_code"]
        assert "__tablename__ = \"users\"" in result["data"]["model_code"]

    @pytest.mark.asyncio
    async def test_generate_orm_model_django(
        self, mock_connection_manager, sample_columns, sample_pk_columns
    ):
        """Test Django model generation."""
        async def mock_execute_query(*args, **kwargs):
            if "EXISTS" in args[0]:
                return True
            elif "information_schema.columns" in args[0]:
                return sample_columns
            elif "PRIMARY KEY" in args[0]:
                return sample_pk_columns
            else:
                return []

        mock_connection_manager.execute_query.side_effect = mock_execute_query

        result = await generate_orm_model("users", "public", "django", "User")

        assert result["success"] is True
        assert result["data"]["model_type"] == "django"
        assert "class User(models.Model):" in result["data"]["model_code"]
        assert "db_table = \"users\"" in result["data"]["model_code"]

    @pytest.mark.asyncio
    async def test_generate_orm_model_pydantic(
        self, mock_connection_manager, sample_columns, sample_pk_columns
    ):
        """Test Pydantic model generation."""
        async def mock_execute_query(*args, **kwargs):
            if "EXISTS" in args[0]:
                return True
            elif "information_schema.columns" in args[0]:
                return sample_columns
            elif "PRIMARY KEY" in args[0]:
                return sample_pk_columns
            else:
                return []

        mock_connection_manager.execute_query.side_effect = mock_execute_query

        result = await generate_orm_model("users", "public", "pydantic", "User")

        assert result["success"] is True
        assert result["data"]["model_type"] == "pydantic"
        assert "class User(BaseModel):" in result["data"]["model_code"]
        assert "from_attributes = True" in result["data"]["model_code"]

    @pytest.mark.asyncio
    async def test_generate_orm_model_invalid_type(self, mock_connection_manager):
        """Test ORM model generation with invalid model type."""
        result = await generate_orm_model("users", "public", "invalid_type")

        assert "error" in result
        assert "VALIDATION_ERROR" in result["error"]["code"]

    @pytest.mark.asyncio
    async def test_generate_orm_model_auto_class_name(
        self, mock_connection_manager, sample_columns, sample_pk_columns
    ):
        """Test ORM model generation with automatic class name generation."""
        async def mock_execute_query(*args, **kwargs):
            if "EXISTS" in args[0]:
                return True
            elif "information_schema.columns" in args[0]:
                return sample_columns
            elif "PRIMARY KEY" in args[0]:
                return sample_pk_columns
            else:
                return []

        mock_connection_manager.execute_query.side_effect = mock_execute_query

        result = await generate_orm_model("user_profiles", "public", "sqlalchemy")

        assert result["success"] is True
        assert result["data"]["class_name"] == "UserProfiles"


class TestHelperFunctions:
    """Test cases for helper functions."""

    def test_generate_sample_value_integer(self):
        """Test sample value generation for integer types."""
        col_info = {"name": "id", "data_type": "integer"}
        result = _generate_sample_value("integer", col_info)
        assert result == "1"

    def test_generate_sample_value_varchar(self):
        """Test sample value generation for varchar types."""
        col_info = {"name": "name", "max_length": 100}
        result = _generate_sample_value("character varying", col_info)
        assert result == "'sample_name'"

    def test_generate_sample_value_short_varchar(self):
        """Test sample value generation for short varchar types."""
        col_info = {"name": "code", "max_length": 5}
        result = _generate_sample_value("character varying", col_info)
        assert result == "'sample'"

    def test_generate_sample_value_boolean(self):
        """Test sample value generation for boolean types."""
        col_info = {"name": "active"}
        result = _generate_sample_value("boolean", col_info)
        assert result == "true"

    def test_generate_sample_value_timestamp(self):
        """Test sample value generation for timestamp types."""
        col_info = {"name": "created_at"}
        result = _generate_sample_value("timestamp", col_info)
        assert result == "'2024-01-01 12:00:00'"

    def test_map_postgres_to_sqlalchemy_type_integer(self):
        """Test PostgreSQL to SQLAlchemy type mapping for integers."""
        col = {
            "data_type": "integer",
            "character_maximum_length": None,
            "numeric_precision": 32,
            "numeric_scale": 0,
        }
        result = _map_postgres_to_sqlalchemy_type(col)
        assert result == "Integer"

    def test_map_postgres_to_sqlalchemy_type_varchar(self):
        """Test PostgreSQL to SQLAlchemy type mapping for varchar."""
        col = {
            "data_type": "character varying",
            "character_maximum_length": 100,
            "numeric_precision": None,
            "numeric_scale": None,
        }
        result = _map_postgres_to_sqlalchemy_type(col)
        assert result == "String(100)"

    def test_map_postgres_to_django_field_integer(self):
        """Test PostgreSQL to Django field mapping for integers."""
        col = {
            "data_type": "integer",
            "character_maximum_length": None,
            "numeric_precision": 32,
            "numeric_scale": 0,
        }
        result = _map_postgres_to_django_field(col)
        assert result == "IntegerField"

    def test_map_postgres_to_django_field_varchar(self):
        """Test PostgreSQL to Django field mapping for varchar."""
        col = {
            "data_type": "character varying",
            "character_maximum_length": 100,
            "numeric_precision": None,
            "numeric_scale": None,
        }
        result = _map_postgres_to_django_field(col)
        assert result == "CharField(max_length=100)"

    def test_map_postgres_to_python_type_integer(self):
        """Test PostgreSQL to Python type mapping for integers."""
        col = {"data_type": "integer"}
        result = _map_postgres_to_python_type(col)
        assert result == "int"

    def test_map_postgres_to_python_type_varchar(self):
        """Test PostgreSQL to Python type mapping for varchar."""
        col = {"data_type": "character varying"}
        result = _map_postgres_to_python_type(col)
        assert result == "str"

    def test_get_required_imports_sqlalchemy(self):
        """Test required imports for SQLAlchemy."""
        result = _get_required_imports("sqlalchemy")
        assert len(result) == 5
        assert "from sqlalchemy import" in result[0]
        assert "Base = declarative_base()" in result[4]

    def test_get_required_imports_django(self):
        """Test required imports for Django."""
        result = _get_required_imports("django")
        assert len(result) == 1
        assert "from django.db import models" in result[0]

    def test_get_required_imports_pydantic(self):
        """Test required imports for Pydantic."""
        result = _get_required_imports("pydantic")
        assert len(result) == 5
        assert "from pydantic import BaseModel" in result[0]
        assert "from typing import Optional" in result[1]


class TestEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_generate_ddl_empty_table_name(self):
        """Test DDL generation with empty table name."""
        result = await generate_ddl("", "public")
        assert "error" in result
        assert "VALIDATION_ERROR" in result["error"]["code"]

    @pytest.mark.asyncio
    async def test_generate_insert_template_empty_table_name(self):
        """Test INSERT template generation with empty table name."""
        result = await generate_insert_template("", "public")
        assert "error" in result
        assert "VALIDATION_ERROR" in result["error"]["code"]

    @pytest.mark.asyncio
    async def test_generate_orm_model_empty_table_name(self):
        """Test ORM model generation with empty table name."""
        result = await generate_orm_model("", "public")
        assert "error" in result
        assert "VALIDATION_ERROR" in result["error"]["code"]

    def test_generate_sample_value_unknown_type(self):
        """Test sample value generation for unknown data types."""
        col_info = {"name": "custom_field"}
        result = _generate_sample_value("custom_type", col_info)
        assert result == "'sample_custom_field'"

    def test_map_postgres_to_sqlalchemy_type_unknown(self):
        """Test PostgreSQL to SQLAlchemy type mapping for unknown types."""
        col = {
            "data_type": "unknown_type",
            "character_maximum_length": None,
            "numeric_precision": None,
            "numeric_scale": None,
        }
        result = _map_postgres_to_sqlalchemy_type(col)
        assert result == "String"

    def test_get_required_imports_unknown_type(self):
        """Test required imports for unknown model type."""
        result = _get_required_imports("unknown")
        assert result == []
