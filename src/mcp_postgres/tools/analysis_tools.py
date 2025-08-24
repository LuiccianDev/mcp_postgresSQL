"""Data analysis tools for MCP Postgres server."""

import logging
from typing import Any

from mcp_postgres.core.connection import connection_manager
from mcp_postgres.utils.formatters import (
    format_analysis_result,
    format_error_response,
    serialize_value,
)
from mcp_postgres.utils.validators import (
    validate_column_name,
    validate_limit_offset,
    validate_table_name,
)


logger = logging.getLogger(__name__)


async def analyze_column(table_name: str, column_name: str) -> dict[str, Any]:
    """Perform statistical analysis on a specific column.

    Analyzes column data to provide statistics like count, nulls, distinct values,
    min/max values, and data distribution information.

    Args:
        table_name: Name of the table containing the column
        column_name: Name of the column to analyze

    Returns:
        Dictionary containing statistical analysis results

    Raises:
        ValueError: If table or column name is invalid
        Exception: If analysis fails
    """
    try:
        # Validate inputs
        validate_table_name(table_name)
        validate_column_name(column_name)

        logger.info(f"Analyzing column {column_name} in table {table_name}")

        # Check if table and column exist
        table_check_query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = $1 AND column_name = $2
        """

        column_info = await connection_manager.execute_query(
            table_check_query, [table_name, column_name], fetch_mode="one"
        )

        if not column_info:
            raise ValueError(
                f"Column '{column_name}' not found in table '{table_name}'"
            )

        data_type = column_info["data_type"]
        is_nullable = column_info["is_nullable"] == "YES"

        # Basic statistics query # noqa: S608
        basic_stats_query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT("{column_name}") as non_null_count,
            COUNT(*) - COUNT("{column_name}") as null_count,
            COUNT(DISTINCT "{column_name}") as distinct_count
        FROM "{table_name}"
        """

        basic_stats = await connection_manager.execute_query(
            basic_stats_query, fetch_mode="one"
        )
        basic_stats = basic_stats if basic_stats is not None else {}

        analysis_result: dict[str, Any] = {
            "column_info": {
                "name": column_name,
                "data_type": data_type,
                "is_nullable": is_nullable,
            },
            "basic_stats": {
                "total_rows": basic_stats["total_rows"],
                "non_null_count": basic_stats["non_null_count"],
                "null_count": basic_stats["null_count"],
                "distinct_count": basic_stats["distinct_count"],
                "null_percentage": round(
                    (basic_stats["null_count"] / basic_stats["total_rows"]) * 100, 2
                )
                if basic_stats["total_rows"] > 0
                else 0,
                "uniqueness_ratio": round(
                    basic_stats["distinct_count"] / basic_stats["non_null_count"], 4
                )
                if basic_stats["non_null_count"] > 0
                else 0,
            },
        }

        # Additional statistics for numeric columns
        if data_type in [
            "integer",
            "bigint",
            "smallint",
            "numeric",
            "decimal",
            "real",
            "double precision",
        ]:
            numeric_stats_query = f"""
            SELECT
                MIN("{column_name}") as min_value,
                MAX("{column_name}") as max_value,
                AVG("{column_name}") as avg_value,
                STDDEV("{column_name}") as std_dev,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{column_name}") as q1,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{column_name}") as median,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{column_name}") as q3
            FROM "{table_name}"
            WHERE "{column_name}" IS NOT NULL
            """

            numeric_stats = await connection_manager.execute_query(
                numeric_stats_query, fetch_mode="one"
            )

            if numeric_stats and numeric_stats["min_value"] is not None:
                analysis_result["numeric_stats"] = {
                    "min_value": serialize_value(numeric_stats["min_value"]),
                    "max_value": serialize_value(numeric_stats["max_value"]),
                    "avg_value": round(float(numeric_stats["avg_value"]), 4)
                    if numeric_stats["avg_value"]
                    else None,
                    "std_dev": round(float(numeric_stats["std_dev"]), 4)
                    if numeric_stats["std_dev"]
                    else None,
                    "q1": serialize_value(numeric_stats["q1"]),
                    "median": serialize_value(numeric_stats["median"]),
                    "q3": serialize_value(numeric_stats["q3"]),
                }

        # Additional statistics for text columns
        elif data_type in ["text", "varchar", "char", "character varying"]:
            text_stats_query = f"""
            SELECT
                MIN(LENGTH("{column_name}")) as min_length,
                MAX(LENGTH("{column_name}")) as max_length,
                AVG(LENGTH("{column_name}")) as avg_length
            FROM "{table_name}"
            WHERE "{column_name}" IS NOT NULL
            """

            text_stats = await connection_manager.execute_query(
                text_stats_query, fetch_mode="one"
            )

            if text_stats:
                analysis_result["text_stats"] = {
                    "min_length": text_stats["min_length"],
                    "max_length": text_stats["max_length"],
                    "avg_length": round(float(text_stats["avg_length"]), 2)
                    if text_stats["avg_length"]
                    else None,
                }

        # Most frequent values (top 10)
        if (
            basic_stats["distinct_count"] <= 1000
        ):  # Only for columns with reasonable distinct count
            frequent_values_query = f"""
            SELECT "{column_name}" as value, COUNT(*) as frequency
            FROM "{table_name}"
            WHERE "{column_name}" IS NOT NULL
            GROUP BY "{column_name}"
            ORDER BY COUNT(*) DESC
            LIMIT 10
            """

            frequent_values = await connection_manager.execute_query(
                frequent_values_query, fetch_mode="all"
            )

            analysis_result["frequent_values"] = [
                {
                    "value": serialize_value(row["value"]),
                    "frequency": row["frequency"],
                    "percentage": round(
                        (row["frequency"] / basic_stats["non_null_count"]) * 100, 2
                    ),
                }
                for row in frequent_values
            ]

        logger.info(f"Column analysis completed for {column_name}")
        return format_analysis_result(
            "column_analysis", table_name, column_name, analysis_result
        )

    except ValueError as e:
        logger.error(f"Validation error in analyze_column: {e}")
        return format_error_response("VALIDATION_ERROR", str(e))
    except Exception as e:
        logger.error(f"Error analyzing column {column_name} in table {table_name}: {e}")
        return format_error_response("ANALYSIS_ERROR", f"Failed to analyze column: {e}")


async def find_duplicates(
    table_name: str, columns: list[str] | None = None, limit: int | None = 100
) -> dict[str, Any]:
    """Find duplicate records in a table based on specified columns.

    Identifies duplicate records by comparing values across specified columns
    or all columns if none specified.

    Args:
        table_name: Name of the table to check for duplicates
        columns: List of column names to check for duplicates (optional)
        limit: Maximum number of duplicate groups to return

    Returns:
        Dictionary containing duplicate analysis results

    Raises:
        ValueError: If table name or parameters are invalid
        Exception: If duplicate detection fails
    """
    try:
        # Validate inputs
        validate_table_name(table_name)
        limit, _ = validate_limit_offset(limit)

        logger.info(f"Finding duplicates in table {table_name}")

        # Get table columns if not specified
        if not columns:
            columns_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position
            """

            column_rows = await connection_manager.execute_query(
                columns_query, [table_name], fetch_mode="all"
            )

            if not column_rows:
                raise ValueError(f"Table '{table_name}' not found or has no columns")

            columns = [row["column_name"] for row in column_rows]
        else:
            # Validate specified columns exist
            for column in columns:
                validate_column_name(column)

            column_check_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = $1 AND column_name = ANY($2)
            """

            existing_columns = await connection_manager.execute_query(
                column_check_query, [table_name, columns], fetch_mode="all"
            )

            existing_column_names = {row["column_name"] for row in existing_columns}
            missing_columns = set(columns) - existing_column_names

            if missing_columns:
                raise ValueError(
                    f"Columns not found in table '{table_name}': {', '.join(missing_columns)}"
                )

        # Build column list for GROUP BY
        quoted_columns = [f'"{col}"' for col in columns]
        column_list = ", ".join(quoted_columns)

        # Find duplicates query
        duplicates_query = f"""
        WITH duplicate_groups AS (
            SELECT {column_list}, COUNT(*) as duplicate_count
            FROM "{table_name}"
            GROUP BY {column_list}
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
            {f"LIMIT {limit}" if limit else ""}
        ),
        duplicate_details AS (
            SELECT
                dg.*,
                array_agg(ctid) as row_ids
            FROM duplicate_groups dg
            JOIN "{table_name}" t ON ({" AND ".join([f't."{col}" = dg."{col}"' for col in columns])})
            GROUP BY {", ".join([f'dg."{col}"' for col in columns])}, dg.duplicate_count
        )
        SELECT * FROM duplicate_details
        ORDER BY duplicate_count DESC
        """

        duplicate_groups = await connection_manager.execute_query(
            duplicates_query, fetch_mode="all"
        )

        # Get total duplicate count
        total_duplicates_query = f"""
        SELECT
            COUNT(*) as total_duplicate_groups,
            SUM(cnt - 1) as total_duplicate_rows
        FROM (
            SELECT COUNT(*) as cnt
            FROM "{table_name}"
            GROUP BY {column_list}
            HAVING COUNT(*) > 1
        ) duplicate_counts
        """

        total_stats = await connection_manager.execute_query(
            total_duplicates_query, fetch_mode="one"
        )
        total_stats = total_stats if total_stats is not None else {}

        # Format results
        duplicate_results = []
        for group in duplicate_groups:
            group_data = {}
            for col in columns:
                group_data[col] = serialize_value(group[col])

            duplicate_results.append(
                {
                    "duplicate_values": group_data,
                    "duplicate_count": group["duplicate_count"],
                    "row_ids": group["row_ids"] if "row_ids" in group else [],
                }
            )

        analysis_result = {
            "table_info": {
                "table_name": table_name,
                "analyzed_columns": columns,
                "column_count": len(columns),
            },
            "duplicate_summary": {
                "total_duplicate_groups": total_stats["total_duplicate_groups"] or 0,
                "total_duplicate_rows": total_stats["total_duplicate_rows"] or 0,
                "groups_returned": len(duplicate_results),
            },
            "duplicate_groups": duplicate_results,
        }

        logger.info(f"Duplicate analysis completed for table {table_name}")
        return format_analysis_result(
            "duplicate_analysis", table_name, None, analysis_result
        )

    except ValueError as e:
        logger.error(f"Validation error in find_duplicates: {e}")
        return format_error_response("VALIDATION_ERROR", str(e))
    except Exception as e:
        logger.error(f"Error finding duplicates in table {table_name}: {e}")
        return format_error_response(
            "ANALYSIS_ERROR", f"Failed to find duplicates: {e}"
        )


async def profile_table(
    table_name: str, sample_size: int | None = None
) -> dict[str, Any]:
    """Analyze data distribution and types across all columns in a table.

    Provides comprehensive profiling including data types, null counts,
    distinct values, and basic statistics for each column.

    Args:
        table_name: Name of the table to profile
        sample_size: Optional sample size for large tables (uses TABLESAMPLE)

    Returns:
        Dictionary containing table profiling results

    Raises:
        ValueError: If table name is invalid
        Exception: If profiling fails
    """
    try:
        # Validate inputs
        validate_table_name(table_name)

        if sample_size is not None and (
            not isinstance(sample_size, int) or sample_size <= 0
        ):
            raise ValueError("Sample size must be a positive integer")

        logger.info(f"Profiling table {table_name}")

        # Get table columns and metadata
        columns_query = """
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_name = $1
        ORDER BY ordinal_position
        """

        columns_info = await connection_manager.execute_query(
            columns_query, [table_name], fetch_mode="all"
        )

        if not columns_info:
            raise ValueError(f"Table '{table_name}' not found")

        # Get table row count
        row_count_query = f'SELECT COUNT(*) as total_rows FROM "{table_name}"'
        row_count_result = await connection_manager.execute_query(
            row_count_query, fetch_mode="one"
        )
        row_count_result = row_count_result if row_count_result is not None else {}
        total_rows = row_count_result["total_rows"]

        # Determine sampling strategy
        sample_clause = ""
        if sample_size and total_rows > sample_size:
            # Use TABLESAMPLE for large tables
            sample_percentage = min(100, (sample_size / total_rows) * 100)
            sample_clause = f"TABLESAMPLE SYSTEM ({sample_percentage})"

        # Profile each column
        column_profiles = []

        for col_info in columns_info:
            column_name = col_info["column_name"]
            data_type = col_info["data_type"]

            try:
                # Basic statistics for each column
                basic_query = f"""
                SELECT
                    COUNT(*) as sample_rows,
                    COUNT("{column_name}") as non_null_count,
                    COUNT(*) - COUNT("{column_name}") as null_count,
                    COUNT(DISTINCT "{column_name}") as distinct_count
                FROM "{table_name}" {sample_clause}
                """

                basic_stats = await connection_manager.execute_query(
                    basic_query, fetch_mode="one"
                )
                basic_stats = basic_stats if basic_stats is not None else {}

                column_profile = {
                    "column_name": column_name,
                    "data_type": data_type,
                    "is_nullable": col_info["is_nullable"] == "YES",
                    "column_default": col_info["column_default"],
                    "character_maximum_length": col_info["character_maximum_length"],
                    "numeric_precision": col_info["numeric_precision"],
                    "numeric_scale": col_info["numeric_scale"],
                    "sample_rows": basic_stats["sample_rows"],
                    "non_null_count": basic_stats["non_null_count"],
                    "null_count": basic_stats["null_count"],
                    "distinct_count": basic_stats["distinct_count"],
                    "null_percentage": round(
                        (basic_stats["null_count"] / basic_stats["sample_rows"]) * 100,
                        2,
                    )
                    if basic_stats["sample_rows"] > 0
                    else 0,
                    "uniqueness_ratio": round(
                        basic_stats["distinct_count"] / basic_stats["non_null_count"], 4
                    )
                    if basic_stats["non_null_count"] > 0
                    else 0,
                }

                # Additional stats for numeric columns
                if data_type in [
                    "integer",
                    "bigint",
                    "smallint",
                    "numeric",
                    "decimal",
                    "real",
                    "double precision",
                ]:
                    numeric_query = f"""
                    SELECT
                        MIN("{column_name}") as min_value,
                        MAX("{column_name}") as max_value,
                        AVG("{column_name}") as avg_value
                    FROM "{table_name}" {sample_clause}
                    WHERE "{column_name}" IS NOT NULL
                    """

                    numeric_stats = await connection_manager.execute_query(
                        numeric_query, fetch_mode="one"
                    )

                    if numeric_stats and numeric_stats["min_value"] is not None:
                        column_profile["numeric_stats"] = {
                            "min_value": serialize_value(numeric_stats["min_value"]),
                            "max_value": serialize_value(numeric_stats["max_value"]),
                            "avg_value": round(float(numeric_stats["avg_value"]), 4)
                            if numeric_stats["avg_value"]
                            else None,
                        }

                # Additional stats for text columns
                elif data_type in ["text", "varchar", "char", "character varying"]:
                    text_query = f"""
                    SELECT
                        MIN(LENGTH("{column_name}")) as min_length,
                        MAX(LENGTH("{column_name}")) as max_length,
                        AVG(LENGTH("{column_name}")) as avg_length
                    FROM "{table_name}" {sample_clause}
                    WHERE "{column_name}" IS NOT NULL
                    """

                    text_stats = await connection_manager.execute_query(
                        text_query, fetch_mode="one"
                    )

                    if text_stats:
                        column_profile["text_stats"] = {
                            "min_length": text_stats["min_length"],
                            "max_length": text_stats["max_length"],
                            "avg_length": round(float(text_stats["avg_length"]), 2)
                            if text_stats["avg_length"]
                            else None,
                        }

                # Sample values for low-cardinality columns
                if (
                    basic_stats["distinct_count"] <= 20
                    and basic_stats["distinct_count"] > 0
                ):
                    sample_values_query = f"""
                    SELECT "{column_name}" as value, COUNT(*) as frequency
                    FROM "{table_name}" {sample_clause}
                    WHERE "{column_name}" IS NOT NULL
                    GROUP BY "{column_name}"
                    ORDER BY COUNT(*) DESC
                    LIMIT 10
                    """

                    sample_values = await connection_manager.execute_query(
                        sample_values_query, fetch_mode="all"
                    )

                    column_profile["sample_values"] = [
                        {
                            "value": serialize_value(row["value"]),
                            "frequency": row["frequency"],
                        }
                        for row in sample_values
                    ]

                column_profiles.append(column_profile)

            except Exception as e:
                logger.warning(f"Error profiling column {column_name}: {e}")
                # Add basic info even if detailed profiling fails
                column_profiles.append(
                    {
                        "column_name": column_name,
                        "data_type": data_type,
                        "is_nullable": col_info["is_nullable"] == "YES",
                        "error": str(e),
                    }
                )

        # Table-level statistics
        table_profile = {
            "table_name": table_name,
            "total_rows": total_rows,
            "column_count": len(columns_info),
            "sampled": sample_size is not None and total_rows > sample_size,
            "sample_size": sample_size
            if sample_size and total_rows > sample_size
            else total_rows,
            "columns": column_profiles,
            "summary": {
                "nullable_columns": sum(
                    1 for col in column_profiles if col.get("is_nullable", False)
                ),
                "numeric_columns": sum(
                    1
                    for col in column_profiles
                    if col["data_type"]
                    in [
                        "integer",
                        "bigint",
                        "smallint",
                        "numeric",
                        "decimal",
                        "real",
                        "double precision",
                    ]
                ),
                "text_columns": sum(
                    1
                    for col in column_profiles
                    if col["data_type"]
                    in ["text", "varchar", "char", "character varying"]
                ),
                "columns_with_nulls": sum(
                    1 for col in column_profiles if col.get("null_count", 0) > 0
                ),
                "unique_columns": sum(
                    1
                    for col in column_profiles
                    if col.get("uniqueness_ratio", 0) == 1.0
                ),
            },
        }

        logger.info(f"Table profiling completed for {table_name}")
        return format_analysis_result("table_profile", table_name, None, table_profile)

    except ValueError as e:
        logger.error(f"Validation error in profile_table: {e}")
        return format_error_response("VALIDATION_ERROR", str(e))
    except Exception as e:
        logger.error(f"Error profiling table {table_name}: {e}")
        return format_error_response("ANALYSIS_ERROR", f"Failed to profile table: {e}")


async def analyze_correlations(
    table_name: str, columns: list[str] | None = None, method: str = "pearson"
) -> dict[str, Any]:
    """Analyze correlations between numeric columns in a table.

    Calculates correlation coefficients between numeric columns to identify
    relationships and dependencies in the data.

    Args:
        table_name: Name of the table to analyze
        columns: List of numeric column names to analyze (optional)
        method: Correlation method ('pearson' only supported currently)

    Returns:
        Dictionary containing correlation analysis results

    Raises:
        ValueError: If table name or parameters are invalid
        Exception: If correlation analysis fails
    """
    try:
        # Validate inputs
        validate_table_name(table_name)

        if method not in ["pearson"]:
            raise ValueError(
                f"Unsupported correlation method: {method}. Only 'pearson' is supported."
            )

        logger.info(f"Analyzing correlations in table {table_name}")

        # Get numeric columns if not specified
        if not columns:
            numeric_columns_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = $1
            AND data_type IN ('integer', 'bigint', 'smallint', 'numeric', 'decimal', 'real', 'double precision')
            ORDER BY ordinal_position
            """

            column_rows = await connection_manager.execute_query(
                numeric_columns_query, [table_name], fetch_mode="all"
            )

            if not column_rows:
                raise ValueError(f"No numeric columns found in table '{table_name}'")

            columns = [row["column_name"] for row in column_rows]
        else:
            # Validate specified columns exist and are numeric
            for column in columns:
                validate_column_name(column)

            column_check_query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = $1 AND column_name = ANY($2)
            """

            existing_columns = await connection_manager.execute_query(
                column_check_query, [table_name, columns], fetch_mode="all"
            )

            existing_column_info = {
                row["column_name"]: row["data_type"] for row in existing_columns
            }
            missing_columns = set(columns) - set(existing_column_info.keys())

            if missing_columns:
                raise ValueError(
                    f"Columns not found in table '{table_name}': {', '.join(missing_columns)}"
                )

            # Check if columns are numeric
            non_numeric_columns = [
                col
                for col, dtype in existing_column_info.items()
                if dtype
                not in [
                    "integer",
                    "bigint",
                    "smallint",
                    "numeric",
                    "decimal",
                    "real",
                    "double precision",
                ]
            ]

            if non_numeric_columns:
                raise ValueError(
                    f"Non-numeric columns specified: {', '.join(non_numeric_columns)}"
                )

        if len(columns) < 2:
            raise ValueError(
                "At least 2 numeric columns are required for correlation analysis"
            )

        # Calculate correlations between all pairs
        correlations = []

        for i, col1 in enumerate(columns):
            for j, col2 in enumerate(columns):
                if i < j:  # Only calculate upper triangle to avoid duplicates
                    try:
                        # Calculate Pearson correlation coefficient # noqa: S608
                        correlation_query = f"""
                        WITH stats AS (
                            SELECT
                                COUNT(*) as n,
                                AVG("{col1}") as mean1,
                                AVG("{col2}") as mean2,
                                STDDEV("{col1}") as std1,
                                STDDEV("{col2}") as std2
                            FROM "{table_name}"
                            WHERE "{col1}" IS NOT NULL AND "{col2}" IS NOT NULL
                        ),
                        correlation AS (
                            SELECT
                                s.n,
                                s.mean1,
                                s.mean2,
                                s.std1,
                                s.std2,
                                CASE
                                    WHEN s.std1 = 0 OR s.std2 = 0 OR s.n <= 1 THEN NULL
                                    ELSE (
                                        SUM(("{col1}" - s.mean1) * ("{col2}" - s.mean2)) / (s.n - 1)
                                    ) / (s.std1 * s.std2)
                                END as correlation_coefficient
                            FROM "{table_name}" t
                            CROSS JOIN stats s
                            WHERE t."{col1}" IS NOT NULL AND t."{col2}" IS NOT NULL
                            GROUP BY s.n, s.mean1, s.mean2, s.std1, s.std2
                        )
                        SELECT * FROM correlation
                        """

                        correlation_result = await connection_manager.execute_query(
                            correlation_query, fetch_mode="one"
                        )

                        if correlation_result:
                            correlation_coeff = correlation_result[
                                "correlation_coefficient"
                            ]
                            sample_size = correlation_result["n"]

                            # Interpret correlation strength
                            if correlation_coeff is None:
                                strength = "undefined"
                                interpretation = "Cannot calculate (zero variance or insufficient data)"
                            else:
                                abs_corr = abs(correlation_coeff)
                                if abs_corr >= 0.8:
                                    strength = "very strong"
                                elif abs_corr >= 0.6:
                                    strength = "strong"
                                elif abs_corr >= 0.4:
                                    strength = "moderate"
                                elif abs_corr >= 0.2:
                                    strength = "weak"
                                else:
                                    strength = "very weak"

                                direction = (
                                    "positive" if correlation_coeff > 0 else "negative"
                                )
                                interpretation = f"{strength} {direction} correlation"

                            correlations.append(
                                {
                                    "column1": col1,
                                    "column2": col2,
                                    "correlation_coefficient": round(
                                        correlation_coeff, 4
                                    )
                                    if correlation_coeff is not None
                                    else None,
                                    "sample_size": sample_size,
                                    "strength": strength,
                                    "interpretation": interpretation,
                                }
                            )

                    except Exception as e:
                        logger.warning(
                            f"Error calculating correlation between {col1} and {col2}: {e}"
                        )
                        correlations.append(
                            {
                                "column1": col1,
                                "column2": col2,
                                "correlation_coefficient": None,
                                "error": str(e),
                            }
                        )

        # Sort correlations by absolute value (strongest first)
        correlations.sort(
            key=lambda x: abs(x["correlation_coefficient"])
            if x["correlation_coefficient"] is not None
            else -1,
            reverse=True,
        )

        # Summary statistics
        valid_correlations = [
            c for c in correlations if c["correlation_coefficient"] is not None
        ]

        summary = {
            "total_pairs": len(correlations),
            "valid_correlations": len(valid_correlations),
            "method": method,
            "analyzed_columns": columns,
            "column_count": len(columns),
        }

        if valid_correlations:
            correlation_values = [
                c["correlation_coefficient"] for c in valid_correlations
            ]
            summary.update(
                {
                    "strongest_positive": max(correlation_values),
                    "strongest_negative": min(correlation_values),
                    "average_correlation": round(
                        sum(abs(c) for c in correlation_values)
                        / len(correlation_values),
                        4,
                    ),
                    "strong_correlations": len(
                        [c for c in correlation_values if abs(c) >= 0.6]
                    ),
                    "moderate_correlations": len(
                        [c for c in correlation_values if 0.4 <= abs(c) < 0.6]
                    ),
                }
            )

        analysis_result = {
            "table_info": {
                "table_name": table_name,
                "analyzed_columns": columns,
                "column_count": len(columns),
            },
            "correlation_summary": summary,
            "correlations": correlations,
        }

        logger.info(f"Correlation analysis completed for table {table_name}")
        return format_analysis_result(
            "correlation_analysis", table_name, None, analysis_result
        )

    except ValueError as e:
        logger.error(f"Validation error in analyze_correlations: {e}")
        return format_error_response("VALIDATION_ERROR", str(e))
    except Exception as e:
        logger.error(f"Error analyzing correlations in table {table_name}: {e}")
        return format_error_response(
            "ANALYSIS_ERROR", f"Failed to analyze correlations: {e}"
        )


# Tool schema definitions for MCP registration
ANALYZE_COLUMN_SCHEMA = {
    "name": "analyze_column",
    "description": "Perform statistical analysis on a specific column including count, nulls, distinct values, and distribution",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table containing the column",
            },
            "column_name": {
                "type": "string",
                "description": "Name of the column to analyze",
            },
        },
        "required": ["table_name", "column_name"],
    },
}

FIND_DUPLICATES_SCHEMA = {
    "name": "find_duplicates",
    "description": "Find duplicate records in a table based on specified columns or all columns",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to check for duplicates",
            },
            "columns": {
                "type": "array",
                "description": "List of column names to check for duplicates (optional, defaults to all columns)",
                "items": {"type": "string"},
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of duplicate groups to return",
                "default": 100,
                "minimum": 1,
            },
        },
        "required": ["table_name"],
    },
}

PROFILE_TABLE_SCHEMA = {
    "name": "profile_table",
    "description": "Analyze data distribution and types across all columns in a table with comprehensive profiling",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to profile",
            },
            "sample_size": {
                "type": "integer",
                "description": "Optional sample size for large tables (uses TABLESAMPLE)",
                "minimum": 1,
            },
        },
        "required": ["table_name"],
    },
}

ANALYZE_CORRELATIONS_SCHEMA = {
    "name": "analyze_correlations",
    "description": "Analyze correlations between numeric columns in a table to identify relationships",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to analyze",
            },
            "columns": {
                "type": "array",
                "description": "List of numeric column names to analyze (optional, defaults to all numeric columns)",
                "items": {"type": "string"},
            },
            "method": {
                "type": "string",
                "description": "Correlation method to use",
                "enum": ["pearson"],
                "default": "pearson",
            },
        },
        "required": ["table_name"],
    },
}

# Export tool functions and schemas
__all__ = [
    "analyze_column",
    "find_duplicates",
    "profile_table",
    "analyze_correlations",
    "ANALYZE_COLUMN_SCHEMA",
    "FIND_DUPLICATES_SCHEMA",
    "PROFILE_TABLE_SCHEMA",
    "ANALYZE_CORRELATIONS_SCHEMA",
]
