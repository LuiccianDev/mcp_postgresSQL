# TOOLS

## Tools Básicos de Consulta

```python
@mcp.tool()
async def execute_query(query: str, params: list = None, ctx: Context) -> dict:
    """Ejecutar query SQL con parámetros opcionales"""

@mcp.tool()
async def execute_raw_query(query: str, ctx: Context) -> dict:
    """Ejecutar query SQL sin validación (solo lectura)"""

@mcp.tool()
async def count_rows(table_name: str, where_clause: str = None, ctx: Context) -> dict:
    """Contar filas en una tabla con condición opcional"""

```

## Tools de Metadata y Esquema

```python
@mcp.tool()
async def list_databases(ctx: Context) -> dict:
    """Listar todas las bases de datos disponibles"""

@mcp.tool()
async def list_tables(schema: str = "public", ctx: Context) -> dict:
    """Listar tablas de un esquema específico"""

@mcp.tool()
async def list_views(schema: str = "public", ctx: Context) -> dict:
    """Listar vistas de un esquema"""

@mcp.tool()
async def describe_table(table_name: str, schema: str = "public", ctx: Context) -> dict:
    """Obtener estructura completa de tabla (columnas, tipos, constraints)"""

@mcp.tool()
async def get_table_indexes(table_name: str, schema: str = "public", ctx: Context) -> dict:
    """Obtener índices de una tabla"""

@mcp.tool()
async def get_table_constraints(table_name: str, schema: str = "public", ctx: Context) -> dict:
    """Obtener constraints (PK, FK, CHECK, etc.)"""

@mcp.tool()
async def list_functions(schema: str = "public", ctx: Context) -> dict:
    """Listar funciones almacenadas"""

@mcp.tool()
async def list_procedures(schema: str = "public", ctx: Context) -> dict:
    """Listar procedimientos almacenados"""

```

## Tools de Análisis de Datos

```python
@mcp.tool()
async def analyze_table(table_name: str, ctx: Context) -> dict:
    """Análisis estadístico básico de una tabla"""

@mcp.tool()
async def get_table_size(table_name: str, ctx: Context) -> dict:
    """Obtener tamaño de tabla en disco"""

@mcp.tool()
async def find_duplicates(table_name: str, columns: list, ctx: Context) -> dict:
    """Encontrar registros duplicados"""

@mcp.tool()
async def get_column_stats(table_name: str, column_name: str, ctx: Context) -> dict:
    """Estadísticas de una columna específica"""

```

## Tools de Gestión de Datos

```python
@mcp.tool()
async def insert_data(table_name: str, data: dict, ctx: Context) -> dict:
    """Insertar un registro en tabla"""

@mcp.tool()
async def update_data(table_name: str, data: dict, where_clause: str, params: list, ctx: Context) -> dict:
    """Actualizar registros con condición"""

@mcp.tool()
async def delete_data(table_name: str, where_clause: str, params: list, ctx: Context) -> dict:
    """Eliminar registros con condición"""

@mcp.tool()
async def bulk_insert(table_name: str, data_list: list, ctx: Context) -> dict:
    """Inserción masiva de datos"""

```

## Tools de Relaciones

```python
@mcp.tool()
async def get_foreign_keys(table_name: str, ctx: Context) -> dict:
    """Obtener claves foráneas de una tabla"""

@mcp.tool()
async def get_referenced_tables(table_name: str, ctx: Context) -> dict:
    """Obtener tablas que referencian a esta tabla"""

@mcp.tool()
async def generate_join_query(tables: list, join_conditions: list, ctx: Context) -> dict:
    """Generar query con JOINs automáticos"""

```

## Tools de Performance

```python
@mcp.tool()
async def explain_query(query: str, analyze: bool = False, ctx: Context) -> dict:
    """EXPLAIN (ANALYZE) de un query"""

@mcp.tool()
async def get_slow_queries(limit: int = 10, ctx: Context) -> dict:
    """Obtener queries más lentos"""

@mcp.tool()
async def get_table_statistics(table_name: str, ctx: Context) -> dict:
    """Estadísticas de uso de tabla"""

```

## Tools de Backup y Restore

```python
@mcp.tool()
async def export_table_csv(table_name: str, file_path: str, ctx: Context) -> dict:
    """Exportar tabla a CSV"""

@mcp.tool()
async def import_csv_to_table(table_name: str, file_path: str, ctx: Context) -> dict:
    """Importar CSV a tabla existente"""

@mcp.tool()
async def create_table_backup(table_name: str, backup_name: str, ctx: Context) -> dict:
    """Crear respaldo de tabla"""
```

## Tools de Administración

```python
@mcp.tool()
async def get_database_info(ctx: Context) -> dict:
    """Información general de la base de datos"""

@mcp.tool()
async def get_connection_info(ctx: Context) -> dict:
    """Información de conexiones activas"""

@mcp.tool()
async def vacuum_table(table_name: str, analyze: bool = True, ctx: Context) -> dict:
    """VACUUM (ANALYZE) de una tabla"""

@mcp.tool()
async def reindex_table(table_name: str, ctx: Context) -> dict:
    """Reindexar una tabla"""
```

## Tools de Validación

```python
@mcp.tool()
async def validate_query(query: str, ctx: Context) -> dict:
    """Validar sintaxis de query sin ejecutar"""

@mcp.tool()
async def check_table_integrity(table_name: str, ctx: Context) -> dict:
    """Verificar integridad de datos"""

@mcp.tool()
async def find_orphaned_records(parent_table: str, child_table: str, fk_column: str, ctx: Context) -> dict:
    """Encontrar registros huérfanos"""
```

## Tools de Generacion

```python
@mcp.tool()
async def generate_create_table_sql(table_name: str, ctx: Context) -> dict:
    """Generar SQL CREATE TABLE de tabla existente"""

@mcp.tool()
async def generate_insert_template(table_name: str, ctx: Context) -> dict:
    """Generar template de INSERT para tabla"""

@mcp.tool()
async def generate_model_class(table_name: str, language: str = "python", ctx: Context) -> dict:
    """Generar clase modelo para ORM"""
```
