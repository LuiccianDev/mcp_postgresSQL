# Development Guide

This guide covers development setup, testing, and contribution guidelines for MCP Postgres.

## Development Setup

### Prerequisites

- Python 3.13 or higher
- PostgreSQL database (local or remote)
- `uv` package manager (recommended)

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd mcp-postgres

# Install dependencies
uv sync

# Install the package in development mode
uv pip install -e .

# Install pre-commit hooks
uv run pre-commit install
```

### Environment Setup

Create a `.env` file for development:

```bash
# Copy example configuration
cp .env.example .env

# Edit with your database credentials
DATABASE_URL="postgresql://postgres:password@localhost:5432/test_db"
LOG_LEVEL="DEBUG"
```

## Development Workflow

### Code Quality

```bash
# Run linter and formatter
uv run ruff check --fix
uv run ruff format

# Type checking
uv run mypy src/

# Run all pre-commit hooks
uv run pre-commit run --all-files
```

### Testing

```bash
# Run tests (when implemented)
uv run pytest

# Run tests with coverage
uv run pytest --cov=src/mcp_postgres

# Run specific test file
uv run pytest tests/unit/test_query_tools.py
```

### Running the Server

#### From Cloned Project

```bash
# Development mode with debug logging
uv run python -m mcp_postgres --dev

# Standard mode
uv run python -m mcp_postgres

# With custom log level
uv run python -m mcp_postgres --log-level DEBUG

# Using the virtual environment Python directly
.venv/Scripts/python.exe -m mcp_postgres --dev  # Windows
.venv/bin/python -m mcp_postgres --dev          # macOS/Linux
```

#### From Installed Package

```bash
# Development mode
mcp-postgres --dev

# Standard mode
mcp-postgres

# With custom log level
mcp-postgres --log-level DEBUG

# Using Python module
python -m mcp_postgres --dev
```

## Project Structure

```
src/mcp_postgres/
├── __init__.py
├── __main__.py               # Module entry point
├── main.py                   # MCP server entry point with CLI
├── config/
│   ├── __init__.py
│   ├── settings.py          # Application configuration
│   └── database.py          # Database-specific config
├── core/
│   ├── __init__.py
│   ├── connection.py        # Connection pool & transactions
│   ├── security.py          # Permission validation
│   └── context.py           # MCP context management
├── tools/
│   ├── __init__.py
│   ├── query_tools.py       # Basic query execution (3 tools)
│   ├── schema_tools.py      # Metadata & structure (8 tools)
│   ├── analysis_tools.py    # Statistical analysis (4 tools)
│   ├── data_tools.py        # CRUD operations (4 tools)
│   ├── relation_tools.py    # Foreign key relations (3 tools)
│   ├── performance_tools.py # Performance analysis (3 tools)
│   ├── backup_tools.py      # Backup & restore (3 tools)
│   ├── admin_tools.py       # Database administration (4 tools)
│   ├── validation_tools.py  # Data validation (3 tools)
│   ├── generation_tools.py  # Code generation (3 tools)
│   └── register_tools.py    # Tool registration
└── utils/
    ├── __init__.py
    ├── validators.py        # Input validation
    ├── formatters.py        # Output formatting
    ├── exceptions.py        # Custom exceptions
    └── helpers.py           # Common utilities
```

## Architecture Overview

### Core Components

1. **MCP Server** (`main.py`): Entry point that handles MCP protocol communication
2. **Connection Manager** (`core/connection.py`): Manages PostgreSQL connection pool
3. **Security Layer** (`core/security.py`): Validates permissions and sanitizes inputs
4. **Tool Registry** (`tools/register_tools.py`): Registers and manages all 50+ tools
5. **Utilities** (`utils/`): Common functions for validation, formatting, and error handling

### Tool Organization

Tools are organized into 10 functional modules:

- **Query Tools**: Basic SQL execution with safety features
- **Schema Tools**: Database metadata inspection and analysis
- **Analysis Tools**: Statistical data analysis and profiling
- **Data Tools**: CRUD operations with validation
- **Relation Tools**: Foreign key and relationship management
- **Performance Tools**: Query optimization and monitoring
- **Backup Tools**: Data export/import capabilities
- **Admin Tools**: Database maintenance and monitoring
- **Validation Tools**: Data integrity and constraint checking
- **Generation Tools**: SQL and ORM code generation

## Adding New Tools

### 1. Create Tool Function

```python
# In appropriate tools module (e.g., tools/my_tools.py)
async def my_new_tool(param1: str, param2: int = 10) -> dict[str, Any]:
    """
    Description of what the tool does.

    Args:
        param1: Description of parameter
        param2: Optional parameter with default

    Returns:
        Dictionary with tool results

    Raises:
        ValidationError: If parameters are invalid
        DatabaseError: If database operation fails
    """
    try:
        # Validate inputs
        validate_table_name(param1)

        # Perform database operation
        result = await connection_manager.execute_query(
            "SELECT * FROM my_table WHERE name = $1 LIMIT $2",
            [param1, param2]
        )

        return {
            "success": True,
            "data": result,
            "row_count": len(result)
        }

    except Exception as e:
        logger.error(f"Error in my_new_tool: {e}")
        raise DatabaseError(f"Tool execution failed: {e}") from e
```

### 2. Define Tool Schema

```python
# MCP tool schema definition
MY_NEW_TOOL_SCHEMA = {
    "name": "my_new_tool",
    "description": "Description of what the tool does",
    "inputSchema": {
        "type": "object",
        "properties": {
            "param1": {
                "type": "string",
                "description": "Description of parameter"
            },
            "param2": {
                "type": "integer",
                "description": "Optional parameter",
                "default": 10
            }
        },
        "required": ["param1"]
    }
}
```

### 3. Register Tool

```python
# In tools/register_tools.py, add to TOOL_REGISTRY
"my_new_tool": {
    "function": my_new_tool,
    "schema": MY_NEW_TOOL_SCHEMA,
    "module": "my_tools",
},
```

### 4. Add Tests

```python
# In tests/unit/test_my_tools.py
import pytest
from mcp_postgres.tools.my_tools import my_new_tool

@pytest.mark.asyncio
async def test_my_new_tool():
    result = await my_new_tool("test_param")
    assert result["success"] is True
    assert "data" in result
```

## Testing Guidelines

### Unit Tests

- Test each tool function in isolation
- Mock database connections using pytest fixtures
- Test both success and error cases
- Validate input parameter handling

### Integration Tests

- Test with real database connections
- Verify MCP protocol compliance
- Test tool registration and discovery
- End-to-end workflow testing

### Test Structure

```
tests/
├── unit/
│   ├── test_query_tools.py
│   ├── test_schema_tools.py
│   ├── test_core_connection.py
│   └── test_utils_validators.py
├── integration/
│   ├── test_database_integration.py
│   └── test_mcp_protocol.py
└── fixtures/
    ├── sample_database.sql
    └── test_data.json
```

## Code Style Guidelines

### Python Style

- Follow PEP 8 with 88-character line length
- Use type hints for all function parameters and return values
- Use double quotes for strings
- Organize imports with ruff/isort

### Documentation

- Use Google-style docstrings
- Document all public functions and classes
- Include parameter types and descriptions
- Provide usage examples for complex functions

### Error Handling

- Use custom exception classes from `utils/exceptions.py`
- Log errors with appropriate context
- Provide meaningful error messages to users
- Handle database connection failures gracefully

## Security Considerations

### Input Validation

- Validate all user inputs using `utils/validators.py`
- Use parameterized queries to prevent SQL injection
- Sanitize table and column names
- Implement access control for sensitive operations

### Database Security

- Use connection pooling with proper timeouts
- Implement query timeouts to prevent long-running operations
- Log security-relevant events
- Validate database permissions before operations

## Performance Guidelines

### Database Operations

- Use connection pooling for efficiency
- Implement query result limits
- Cache schema metadata when possible
- Use transactions for multi-query operations

### Memory Management

- Stream large result sets when possible
- Implement pagination for large datasets
- Clean up resources properly
- Monitor memory usage in long-running operations

## Debugging

### Logging

```python
import logging
logger = logging.getLogger(__name__)

# Use appropriate log levels
logger.debug("Detailed debugging information")
logger.info("General information")
logger.warning("Warning about potential issues")
logger.error("Error that needs attention")
```

### Development Mode

```bash
# Enable debug logging and development features
uv run python -m mcp_postgres --dev
```

### Database Debugging

```python
# Use raw query tool for debugging
result = await session.call_tool(
    "execute_raw_query",
    arguments={
        "query": "EXPLAIN ANALYZE SELECT * FROM users"
    }
)
```

## Contributing

### Pull Request Process

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run code quality checks
6. Submit a pull request

### Code Review Checklist

- [ ] Code follows style guidelines
- [ ] All tests pass
- [ ] New functionality is tested
- [ ] Documentation is updated
- [ ] Security considerations are addressed
- [ ] Performance impact is considered

## Release Process

### Version Management

- Use semantic versioning (MAJOR.MINOR.PATCH)
- Update version in `pyproject.toml`
- Create release notes
- Tag releases in git

### Deployment

```bash
# Build package
uv build

# Test installation
uv pip install dist/mcp_postgres-*.whl

# Publish to PyPI (when ready)
uv publish
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure `src/` is in Python path
2. **Database Connection**: Check DATABASE_URL format
3. **Permission Errors**: Verify database user permissions
4. **Type Errors**: Many are expected due to dynamic query results

### Getting Help

- Check the troubleshooting section in README.md
- Review configuration documentation
- Enable debug logging for detailed information
- Create an issue on GitHub with reproduction steps
