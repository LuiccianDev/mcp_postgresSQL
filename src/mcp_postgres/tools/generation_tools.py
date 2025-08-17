"""Code generation tools for MCP Postgres server.

This module provides tools for generating SQL DDL statements, INSERT templates,
and ORM model classes based on database schema information.
"""

import logging
from typing import Any

from ..core.connection import connection_manager
from ..utils.exceptions import (
    MCPPostgresError,
    TableNotFoundError,
    ValidationError,
    handle_postgres_error,
)
from ..utils.formatters import format_error_response, format_success_response
from ..utils.validators import validate_table_name


logger = logging.getLogger(__name__)


async def generate_ddl(
    table_name: str, schema_name: str | None = None, include_indexes: bool = True
) -> dict[str, Any]:
    """Generate CREATE TABLE DDL statement for a table.

    This tool analyzes an existing table structure and generates the corresponding
    CREATE TABLE statement with columns, constraints, and optionally indexes.

    Args:
        table_name: Name of the table to generate DDL for
        schema_name: Optional schema name (defaults to 'public')
        include_indexes: Whether to include CREATE INDEX statements

    Returns:
        Dictionary containing the generated DDL statements

    Raises:
        ValidationError: If table name is invalid
        TableNotFoundError: If table doesn't exist
        MCPPostgresError: If query execution fails
    """
    try:
        try:
            validate_table_name(table_name)
        except ValueError as e:
            raise ValidationError(str(e), field_name="table_name", field_value=table_name) from e

        if schema_name is None:
            schema_name = "public"

        try:
            validate_table_name(schema_name)
        except ValueError as e:
            raise ValidationError(str(e), field_name="schema_name", field_value=schema_name) from e

        # Check if table exists
        table_exists_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )
        """
        exists = await connection_manager.execute_query(
            table_exists_query, [schema_name, table_name], fetch_mode="val"
        )

        if not exists:
            raise TableNotFoundError(
                f"Table '{schema_name}.{table_name}' does not exist"
            )

        # Get table columns with detailed information
        columns_query = """
        SELECT
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            c.ordinal_position
        FROM information_schema.columns c
        WHERE c.table_schema = $1 AND c.table_name = $2
        ORDER BY c.ordinal_position
        """

        columns = await connection_manager.execute_query(
            columns_query, [schema_name, table_name]
        )

        if not columns:
            raise MCPPostgresError(
                f"No columns found for table '{schema_name}.{table_name}'"
            )

        # Get primary key constraints
        pk_query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = $1
            AND tc.table_name = $2
            AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position
        """

        pk_columns = await connection_manager.execute_query(
            pk_query, [schema_name, table_name]
        )
        pk_column_names = [row["column_name"] for row in pk_columns]

        # Get foreign key constraints
        fk_query = """
        SELECT
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.table_schema = $1
            AND tc.table_name = $2
            AND tc.constraint_type = 'FOREIGN KEY'
        """

        fk_constraints = await connection_manager.execute_query(
            fk_query, [schema_name, table_name]
        )

        # Get unique constraints
        unique_query = """
        SELECT
            tc.constraint_name,
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = $1
            AND tc.table_name = $2
            AND tc.constraint_type = 'UNIQUE'
        ORDER BY tc.constraint_name, kcu.ordinal_position
        """

        unique_constraints = await connection_manager.execute_query(
            unique_query, [schema_name, table_name]
        )

        # Generate CREATE TABLE statement
        ddl_lines = []
        ddl_lines.append(f"CREATE TABLE {schema_name}.{table_name} (")

        # Add columns
        column_definitions = []
        for col in columns:
            col_def = f"    {col['column_name']}"

            # Add data type
            data_type = col["data_type"].upper()
            if col["character_maximum_length"]:
                col_def += f" {data_type}({col['character_maximum_length']})"
            elif col["numeric_precision"] and col["numeric_scale"] is not None:
                col_def += f" {data_type}({col['numeric_precision']},{col['numeric_scale']})"
            elif col["numeric_precision"]:
                col_def += f" {data_type}({col['numeric_precision']})"
            else:
                col_def += f" {data_type}"

            # Add NOT NULL constraint
            if col["is_nullable"] == "NO":
                col_def += " NOT NULL"

            # Add default value
            if col["column_default"]:
                col_def += f" DEFAULT {col['column_default']}"

            column_definitions.append(col_def)

        ddl_lines.extend(column_definitions)

        # Add primary key constraint
        if pk_column_names:
            pk_def = f"    PRIMARY KEY ({', '.join(pk_column_names)})"
            ddl_lines.append(f",\n{pk_def}")

        # Add unique constraints
        unique_groups : dict[str, Any] = {}
        for constraint in unique_constraints:
            constraint_name = constraint["constraint_name"]
            if constraint_name not in unique_groups:
                unique_groups[constraint_name] = []
            unique_groups[constraint_name].append(constraint["column_name"])

        for constraint_name, columns_list in unique_groups.items():
            unique_def = f"    CONSTRAINT {constraint_name} UNIQUE ({', '.join(columns_list)})"
            ddl_lines.append(f",\n{unique_def}")

        # Add foreign key constraints
        fk_groups : dict[str, Any] = {}
        for fk in fk_constraints:
            constraint_name = fk["constraint_name"]
            if constraint_name not in fk_groups:
                fk_groups[constraint_name] = {
                    "columns": [],
                    "foreign_table": f"{fk['foreign_table_schema']}.{fk['foreign_table_name']}",
                    "foreign_columns": [],
                }
            fk_groups[constraint_name]["columns"].append(fk["column_name"])
            fk_groups[constraint_name]["foreign_columns"].append(
                fk["foreign_column_name"]
            )

        for constraint_name, fk_info in fk_groups.items():
            fk_def = (
                f"    CONSTRAINT {constraint_name} "
                f"FOREIGN KEY ({', '.join(fk_info['columns'])}) "
                f"REFERENCES {fk_info['foreign_table']} ({', '.join(fk_info['foreign_columns'])})"
            )
            ddl_lines.append(f",\n{fk_def}")

        # Close CREATE TABLE statement
        ddl_lines.append("\n);")

        create_table_ddl = ",\n".join(ddl_lines[1:-1])
        create_table_ddl = ddl_lines[0] + "\n" + create_table_ddl + ddl_lines[-1]

        result = {
            "table_name": table_name,
            "schema_name": schema_name,
            "create_table_ddl": create_table_ddl,
            "column_count": len(columns),
            "has_primary_key": bool(pk_column_names),
            "foreign_key_count": len(fk_groups),
            "unique_constraint_count": len(unique_groups),
        }

        # Optionally include index statements
        if include_indexes:
            indexes_query = """
            SELECT
                i.indexname,
                i.indexdef
            FROM pg_indexes i
            WHERE i.schemaname = $1
                AND i.tablename = $2
                AND i.indexname NOT LIKE '%_pkey'
            ORDER BY i.indexname
            """

            indexes = await connection_manager.execute_query(
                indexes_query, [schema_name, table_name]
            )

            index_statements = []
            for idx in indexes:
                index_statements.append(f"{idx['indexdef']};")

            result["index_statements"] = index_statements
            result["index_count"] = len(index_statements)

        logger.info(
            f"Generated DDL for table '{schema_name}.{table_name}' with {len(columns)} columns"
        )

        return format_success_response(
            data=result, message="DDL generated successfully"
        )

    except (ValidationError, TableNotFoundError) as e:
        logger.warning(f"DDL generation validation error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"DDL generation error: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


async def generate_insert_template(
    table_name: str, schema_name: str | None = None, include_optional: bool = True
) -> dict[str, Any]:
    """Generate INSERT statement template for a table.

    This tool analyzes a table structure and generates INSERT statement templates
    with placeholders for all columns or just required columns.

    Args:
        table_name: Name of the table to generate INSERT template for
        schema_name: Optional schema name (defaults to 'public')
        include_optional: Whether to include nullable columns in template

    Returns:
        Dictionary containing INSERT statement templates and column information

    Raises:
        ValidationError: If table name is invalid
        TableNotFoundError: If table doesn't exist
        MCPPostgresError: If query execution fails
    """
    try:
        try:
            validate_table_name(table_name)
        except ValueError as e:
            raise ValidationError(str(e), field_name="table_name", field_value=table_name) from e

        if schema_name is None:
            schema_name = "public"

        try:
            validate_table_name(schema_name)
        except ValueError as e:
            raise ValidationError(str(e), field_name="schema_name", field_value=schema_name) from e

        # Check if table exists
        table_exists_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )
        """
        exists = await connection_manager.execute_query(
            table_exists_query, [schema_name, table_name], fetch_mode="val"
        )

        if not exists:
            raise TableNotFoundError(
                f"Table '{schema_name}.{table_name}' does not exist"
            )

        # Get table columns with detailed information
        columns_query = """
        SELECT
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.column_default,
            c.ordinal_position,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale
        FROM information_schema.columns c
        WHERE c.table_schema = $1 AND c.table_name = $2
        ORDER BY c.ordinal_position
        """

        columns = await connection_manager.execute_query(
            columns_query, [schema_name, table_name]
        )

        if not columns:
            raise MCPPostgresError(
                f"No columns found for table '{schema_name}.{table_name}'"
            )

        # Separate required and optional columns
        required_columns = []
        optional_columns = []
        all_columns = []

        for col in columns:
            col_info = {
                "name": col["column_name"],
                "data_type": col["data_type"],
                "is_nullable": col["is_nullable"] == "YES",
                "has_default": col["column_default"] is not None,
                "default_value": col["column_default"],
                "max_length": col["character_maximum_length"],
                "precision": col["numeric_precision"],
                "scale": col["numeric_scale"],
            }

            all_columns.append(col_info)

            # Column is required if it's NOT NULL and has no default value
            if col["is_nullable"] == "NO" and col["column_default"] is None:
                required_columns.append(col_info)
            else:
                optional_columns.append(col_info)

        # Generate INSERT templates
        templates = {}

        # Template with only required columns
        if required_columns:
            req_column_names = [col["name"] for col in required_columns]
            req_placeholders = [f"${i+1}" for i in range(len(required_columns))]

            templates["required_only"] = {
                "sql": f"INSERT INTO {schema_name}.{table_name} ({', '.join(req_column_names)}) VALUES ({', '.join(req_placeholders)});",
                "columns": required_columns,
                "parameter_count": len(required_columns),
            }

        # Template with all columns (if include_optional is True)
        all_column_names = [col["name"] for col in all_columns]
        if include_optional:
            all_placeholders = [f"${i+1}" for i in range(len(all_columns))]

            templates["all_columns"] = {
                "sql": f"INSERT INTO {schema_name}.{table_name} ({', '.join(all_column_names)}) VALUES ({', '.join(all_placeholders)});",
                "columns": all_columns,
                "parameter_count": len(all_columns),
            }

        # Template with named placeholders (for easier reading)
        all_column_names = [col["name"] for col in all_columns]
        named_placeholders = [f":{col['name']}" for col in all_columns]

        templates["named_parameters"] = {
            "sql": f"INSERT INTO {schema_name}.{table_name} ({', '.join(all_column_names)}) VALUES ({', '.join(named_placeholders)});",
            "columns": all_columns,
            "parameter_count": len(all_columns),
            "note": "Use named parameters - replace :column_name with actual values",
        }

        # Generate sample data based on column types
        sample_values = []
        for col in all_columns:
            data_type = col["data_type"].lower()
            sample_value = _generate_sample_value(data_type, col)
            sample_values.append(sample_value)

        templates["with_sample_data"] = {
            "sql": f"INSERT INTO {schema_name}.{table_name} ({', '.join(all_column_names)}) VALUES ({', '.join(sample_values)});",
            "columns": all_columns,
            "note": "Example with sample data - replace with actual values",
        }

        result = {
            "table_name": table_name,
            "schema_name": schema_name,
            "templates": templates,
            "column_summary": {
                "total_columns": len(all_columns),
                "required_columns": len(required_columns),
                "optional_columns": len(optional_columns),
            },
            "column_details": {
                "required": required_columns,
                "optional": optional_columns,
            },
        }

        logger.info(
            f"Generated INSERT templates for table '{schema_name}.{table_name}' with {len(all_columns)} columns"
        )

        return format_success_response(
            data=result, message="INSERT templates generated successfully"
        )

    except (ValidationError, TableNotFoundError) as e:
        logger.warning(f"INSERT template generation validation error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"INSERT template generation error: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


def _generate_sample_value(data_type: str, col_info: dict[str, Any]) -> str:
    """Generate sample value based on column data type."""
    data_type = data_type.lower()

    if "int" in data_type or "serial" in data_type:
        return "1"
    elif "float" in data_type or "double" in data_type or "numeric" in data_type:
        return "1.0"
    elif "bool" in data_type:
        return "true"
    elif "date" in data_type:
        return "'2024-01-01'"
    elif "timestamp" in data_type:
        return "'2024-01-01 12:00:00'"
    elif "time" in data_type:
        return "'12:00:00'"
    elif "uuid" in data_type:
        return "'550e8400-e29b-41d4-a716-446655440000'"
    elif "json" in data_type:
        return "'{\"key\": \"value\"}'"
    elif "text" in data_type or "char" in data_type or "varchar" in data_type:
        max_length = col_info.get("max_length")
        if max_length and max_length < 20:
            return "'sample'"
        return f"'sample_{col_info['name']}'"
    else:
        return f"'sample_{col_info['name']}'"


async def generate_orm_model(
    table_name: str,
    schema_name: str | None = None,
    model_type: str = "sqlalchemy",
    class_name: str | None = None,
) -> dict[str, Any]:
    """Generate ORM model class definition for a table.

    This tool analyzes a table structure and generates ORM model class definitions
    for popular Python ORMs like SQLAlchemy, Django, or Pydantic.

    Args:
        table_name: Name of the table to generate model for
        schema_name: Optional schema name (defaults to 'public')
        model_type: Type of ORM model ('sqlalchemy', 'django', 'pydantic')
        class_name: Optional custom class name (defaults to PascalCase table name)

    Returns:
        Dictionary containing the generated ORM model class code

    Raises:
        ValidationError: If table name or model type is invalid
        TableNotFoundError: If table doesn't exist
        MCPPostgresError: If query execution fails
    """
    try:
        try:
            validate_table_name(table_name)
        except ValueError as e:
            raise ValidationError(str(e), field_name="table_name", field_value=table_name) from e

        if schema_name is None:
            schema_name = "public"

        try:
            validate_table_name(schema_name)
        except ValueError as e:
            raise ValidationError(str(e), field_name="schema_name", field_value=schema_name) from e

        # Validate model type
        supported_types = {"sqlalchemy", "django", "pydantic"}
        if model_type not in supported_types:
            raise ValidationError(
                f"Unsupported model_type: {model_type}. Must be one of: {', '.join(supported_types)}"
            )

        # Generate class name if not provided
        if class_name is None:
            # Convert snake_case to PascalCase
            class_name = "".join(word.capitalize() for word in table_name.split("_"))

        # Check if table exists
        table_exists_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )
        """
        exists = await connection_manager.execute_query(
            table_exists_query, [schema_name, table_name], fetch_mode="val"
        )

        if not exists:
            raise TableNotFoundError(
                f"Table '{schema_name}.{table_name}' does not exist"
            )

        # Get table columns with detailed information
        columns_query = """
        SELECT
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            c.ordinal_position
        FROM information_schema.columns c
        WHERE c.table_schema = $1 AND c.table_name = $2
        ORDER BY c.ordinal_position
        """

        columns = await connection_manager.execute_query(
            columns_query, [schema_name, table_name]
        )

        if not columns:
            raise MCPPostgresError(
                f"No columns found for table '{schema_name}.{table_name}'"
            )

        # Get primary key information
        pk_query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = $1
            AND tc.table_name = $2
            AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position
        """

        pk_columns = await connection_manager.execute_query(
            pk_query, [schema_name, table_name]
        )
        pk_column_names = {row["column_name"] for row in pk_columns}

        # Get foreign key information
        fk_query = """
        SELECT
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.table_schema = $1
            AND tc.table_name = $2
            AND tc.constraint_type = 'FOREIGN KEY'
        """

        fk_constraints = await connection_manager.execute_query(
            fk_query, [schema_name, table_name]
        )
        fk_info = {row["column_name"]: row for row in fk_constraints}

        # Generate model code based on type
        if model_type == "sqlalchemy":
            model_code = _generate_sqlalchemy_model(
                class_name, table_name, schema_name, columns, pk_column_names, fk_info
            )
        elif model_type == "django":
            model_code = _generate_django_model(
                class_name, table_name, columns, pk_column_names, fk_info
            )
        elif model_type == "pydantic":
            model_code = _generate_pydantic_model(
                class_name, columns, pk_column_names
            )

        result = {
            "table_name": table_name,
            "schema_name": schema_name,
            "class_name": class_name,
            "model_type": model_type,
            "model_code": model_code,
            "column_count": len(columns),
            "primary_key_columns": list(pk_column_names),
            "foreign_key_count": len(fk_info),
            "imports_needed": _get_required_imports(model_type),
        }

        logger.info(
            f"Generated {model_type} model for table '{schema_name}.{table_name}' as class '{class_name}'"
        )

        return format_success_response(
            data=result, message=f"{model_type.title()} model generated successfully"
        )

    except (ValidationError, TableNotFoundError) as e:
        logger.warning(f"ORM model generation validation error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"ORM model generation error: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


def _generate_sqlalchemy_model(
    class_name: str,
    table_name: str,
    schema_name: str,
    columns: list[dict],
    pk_columns: set[str],
    fk_info: dict[str, dict],
) -> str:
    """Generate SQLAlchemy model class code."""
    lines = []
    lines.append(f"class {class_name}(Base):")
    lines.append(f'    __tablename__ = "{table_name}"')
    if schema_name != "public":
        lines.append(f'    __table_args__ = {{"schema": "{schema_name}"}}')
    lines.append("")

    for col in columns:
        col_name = col["column_name"]
        data_type = col["data_type"].lower() # noqa: F841
        is_nullable = col["is_nullable"] == "YES"
        has_default = col["column_default"] is not None
        is_pk = col_name in pk_columns
        is_fk = col_name in fk_info

        # Map PostgreSQL types to SQLAlchemy types
        sa_type = _map_postgres_to_sqlalchemy_type(col)

        # Build column definition
        col_def = f"    {col_name} = Column({sa_type}"

        # Add constraints
        constraints = []
        if is_pk:
            constraints.append("primary_key=True")
        if is_fk:
            fk_table = fk_info[col_name]["foreign_table_name"]
            fk_column = fk_info[col_name]["foreign_column_name"]
            constraints.append(f'ForeignKey("{fk_table}.{fk_column}")')
        if not is_nullable and not is_pk:
            constraints.append("nullable=False")
        if has_default:
            constraints.append(f'default="{col["column_default"]}"')

        if constraints:
            col_def += ", " + ", ".join(constraints)

        col_def += ")"
        lines.append(col_def)

    return "\n".join(lines)


def _generate_django_model(
    class_name: str,
    table_name: str,
    columns: list[dict],
    pk_columns: set[str],
    fk_info: dict[str, dict],
) -> str:
    """Generate Django model class code."""
    lines = []
    lines.append(f"class {class_name}(models.Model):")

    for col in columns:
        col_name = col["column_name"]
        data_type = col["data_type"].lower() # noqa: F841
        is_nullable = col["is_nullable"] == "YES"
        has_default = col["column_default"] is not None
        is_pk = col_name in pk_columns
        is_fk = col_name in fk_info

        # Map PostgreSQL types to Django field types
        django_field = _map_postgres_to_django_field(col)

        # Build field definition
        field_def = f"    {col_name} = models.{django_field}("

        # Add field options
        options = []
        if is_pk and len(pk_columns) == 1:
            options.append("primary_key=True")
        if is_fk:
            fk_table = fk_info[col_name]["foreign_table_name"]
            # Convert table name to model name
            fk_model = "".join(word.capitalize() for word in fk_table.split("_"))
            field_def = f"    {col_name} = models.ForeignKey({fk_model}, on_delete=models.CASCADE"
            if is_nullable:
                options.append("null=True, blank=True")
        else:
            if is_nullable:
                options.append("null=True, blank=True")
            if has_default and not is_pk:
                default_val = col["column_default"]
                if "nextval" not in str(default_val):  # Skip sequence defaults
                    options.append(f'default="{default_val}"')

        if options:
            field_def += ", ".join(options)

        field_def += ")"
        lines.append(field_def)

    lines.append("")
    lines.append("    class Meta:")
    lines.append(f'        db_table = "{table_name}"')

    return "\n".join(lines)


def _generate_pydantic_model(
    class_name: str, columns: list[dict], pk_columns: set[str]
) -> str:
    """Generate Pydantic model class code."""
    lines = []
    lines.append(f"class {class_name}(BaseModel):")

    for col in columns:
        col_name = col["column_name"]
        data_type = col["data_type"].lower() # noqa: F841
        is_nullable = col["is_nullable"] == "YES"
        has_default = col["column_default"] is not None

        # Map PostgreSQL types to Python types
        python_type = _map_postgres_to_python_type(col)

        # Build field definition
        if is_nullable:
            field_def = f"    {col_name}: Optional[{python_type}] = None"
        elif has_default:
            field_def = f"    {col_name}: {python_type} = Field(default=...)"
        else:
            field_def = f"    {col_name}: {python_type}"

        lines.append(field_def)

    lines.append("")
    lines.append("    class Config:")
    lines.append("        from_attributes = True")

    return "\n".join(lines)


def _map_postgres_to_sqlalchemy_type(col: dict[str, Any]) -> str:
    """Map PostgreSQL data type to SQLAlchemy type."""
    data_type = col["data_type"].lower()
    max_length = col["character_maximum_length"]
    precision = col["numeric_precision"]
    scale = col["numeric_scale"]

    if "int" in data_type or "serial" in data_type:
        if "bigint" in data_type or "bigserial" in data_type:
            return "BigInteger"
        elif "smallint" in data_type or "smallserial" in data_type:
            return "SmallInteger"
        return "Integer"
    elif "varchar" in data_type or "character varying" in data_type:
        return f"String({max_length})" if max_length else "String"
    elif "char" in data_type and "varchar" not in data_type:
        return f"CHAR({max_length})" if max_length else "CHAR"
    elif "text" in data_type:
        return "Text"
    elif "numeric" in data_type or "decimal" in data_type:
        if precision and scale is not None:
            return f"Numeric({precision}, {scale})"
        return "Numeric"
    elif "float" in data_type or "double" in data_type or "real" in data_type:
        return "Float"
    elif "bool" in data_type:
        return "Boolean"
    elif "date" in data_type and "timestamp" not in data_type:
        return "Date"
    elif "timestamp" in data_type:
        return "DateTime"
    elif "time" in data_type:
        return "Time"
    elif "uuid" in data_type:
        return "UUID"
    elif "json" in data_type:
        return "JSON"
    else:
        return "String"


def _map_postgres_to_django_field(col: dict[str, Any]) -> str:
    """Map PostgreSQL data type to Django field type."""
    data_type = col["data_type"].lower()
    max_length = col["character_maximum_length"]

    if "int" in data_type or "serial" in data_type:
        if "bigint" in data_type or "bigserial" in data_type:
            return "BigIntegerField"
        elif "smallint" in data_type or "smallserial" in data_type:
            return "SmallIntegerField"
        return "IntegerField"
    elif "varchar" in data_type or "character varying" in data_type:
        return f"CharField(max_length={max_length})" if max_length else "CharField(max_length=255)"
    elif "char" in data_type and "varchar" not in data_type:
        return f"CharField(max_length={max_length})" if max_length else "CharField(max_length=1)"
    elif "text" in data_type:
        return "TextField"
    elif "numeric" in data_type or "decimal" in data_type:
        precision = col["numeric_precision"]
        scale = col["numeric_scale"]
        if precision and scale is not None:
            return f"DecimalField(max_digits={precision}, decimal_places={scale})"
        return "DecimalField(max_digits=10, decimal_places=2)"
    elif "float" in data_type or "double" in data_type or "real" in data_type:
        return "FloatField"
    elif "bool" in data_type:
        return "BooleanField"
    elif "date" in data_type and "timestamp" not in data_type:
        return "DateField"
    elif "timestamp" in data_type:
        return "DateTimeField"
    elif "time" in data_type:
        return "TimeField"
    elif "uuid" in data_type:
        return "UUIDField"
    elif "json" in data_type:
        return "JSONField"
    else:
        return "CharField(max_length=255)"


def _map_postgres_to_python_type(col: dict[str, Any]) -> str:
    """Map PostgreSQL data type to Python type for Pydantic."""
    data_type = col["data_type"].lower()

    if "int" in data_type or "serial" in data_type:
        return "int"
    elif "varchar" in data_type or "char" in data_type or "text" in data_type:
        return "str"
    elif "numeric" in data_type or "decimal" in data_type:
        return "Decimal"
    elif "float" in data_type or "double" in data_type or "real" in data_type:
        return "float"
    elif "bool" in data_type:
        return "bool"
    elif "date" in data_type and "timestamp" not in data_type:
        return "date"
    elif "timestamp" in data_type:
        return "datetime"
    elif "time" in data_type:
        return "time"
    elif "uuid" in data_type:
        return "UUID"
    elif "json" in data_type:
        return "dict"
    else:
        return "str"


def _get_required_imports(model_type: str) -> list[str]:
    """Get required imports for the model type."""
    if model_type == "sqlalchemy":
        return [
            "from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey",
            "from sqlalchemy.ext.declarative import declarative_base",
            "from sqlalchemy.orm import relationship",
            "",
            "Base = declarative_base()",
        ]
    elif model_type == "django":
        return [
            "from django.db import models",
        ]
    elif model_type == "pydantic":
        return [
            "from pydantic import BaseModel, Field",
            "from typing import Optional",
            "from datetime import datetime, date, time",
            "from decimal import Decimal",
            "from uuid import UUID",
        ]
    return []


# Tool schema definitions for MCP registration
GENERATE_DDL_SCHEMA = {
    "name": "generate_ddl",
    "description": "Generate CREATE TABLE DDL statement for an existing table with columns, constraints, and indexes",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to generate DDL for",
            },
            "schema_name": {
                "type": "string",
                "description": "Schema name (defaults to 'public')",
                "default": "public",
            },
            "include_indexes": {
                "type": "boolean",
                "description": "Whether to include CREATE INDEX statements",
                "default": True,
            },
        },
        "required": ["table_name"],
    },
}

GENERATE_INSERT_TEMPLATE_SCHEMA = {
    "name": "generate_insert_template",
    "description": "Generate INSERT statement templates for a table with parameter placeholders and sample data",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to generate INSERT template for",
            },
            "schema_name": {
                "type": "string",
                "description": "Schema name (defaults to 'public')",
                "default": "public",
            },
            "include_optional": {
                "type": "boolean",
                "description": "Whether to include nullable columns in templates",
                "default": True,
            },
        },
        "required": ["table_name"],
    },
}

GENERATE_ORM_MODEL_SCHEMA = {
    "name": "generate_orm_model",
    "description": "Generate ORM model class definition for a table (SQLAlchemy, Django, or Pydantic)",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to generate model for",
            },
            "schema_name": {
                "type": "string",
                "description": "Schema name (defaults to 'public')",
                "default": "public",
            },
            "model_type": {
                "type": "string",
                "description": "Type of ORM model to generate",
                "enum": ["sqlalchemy", "django", "pydantic"],
                "default": "sqlalchemy",
            },
            "class_name": {
                "type": "string",
                "description": "Custom class name (defaults to PascalCase table name)",
            },
        },
        "required": ["table_name"],
    },
}

# Export tool functions and schemas
__all__ = [
    "generate_ddl",
    "generate_insert_template",
    "generate_orm_model",
    "GENERATE_DDL_SCHEMA",
    "GENERATE_INSERT_TEMPLATE_SCHEMA",
    "GENERATE_ORM_MODEL_SCHEMA",
]
