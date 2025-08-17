"""Data validation tools for MCP Postgres server."""

import logging
from typing import Any

from ..core.connection import connection_manager
from ..utils.formatters import (
    format_analysis_result,
    format_error_response,
    serialize_value,
)
from ..utils.validators import (
    validate_column_name,
    validate_table_name,
)


logger = logging.getLogger(__name__)


async def validate_constraints(table_name: str) -> dict[str, Any]:
    """Check constraint violations in a table.

    Validates all constraints including primary keys, foreign keys, unique constraints,
    check constraints, and not-null constraints to identify data integrity issues.

    Args:
        table_name: Name of the table to validate constraints for

    Returns:
        Dictionary containing constraint validation results

    Raises:
        ValueError: If table name is invalid
        Exception: If constraint validation fails
    """
    try:
        # Validate inputs
        validate_table_name(table_name)

        logger.info(f"Validating constraints for table {table_name}")

        # Check if table exists
        table_check_query = """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_name = $1 AND table_schema = 'public'
        """

        table_info = await connection_manager.execute_query(
            table_check_query, [table_name], fetch_mode="one"
        )

        if not table_info:
            raise ValueError(f"Table '{table_name}' not found")

        validation_results: dict[str, Any] = {
            "table_name": table_name,
            "constraint_violations": [],
            "validation_summary": {
                "total_constraints_checked": 0,
                "violations_found": 0,
                "constraint_types_checked": [],
            },
        }

        # 1. Check NOT NULL constraints
        logger.info("Checking NOT NULL constraints")
        not_null_query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = $1 AND is_nullable = 'NO'
        ORDER BY ordinal_position
        """

        not_null_columns = await connection_manager.execute_query(
            not_null_query, [table_name], fetch_mode="all"
        )

        validation_results["validation_summary"]["constraint_types_checked"].append(
            "NOT NULL"
        )

        for column in not_null_columns:
            column_name = column["column_name"]
            null_check_query = f"""
            SELECT COUNT(*) as null_count
            FROM "{table_name}"
            WHERE "{column_name}" IS NULL
            """

            null_result = (
                await connection_manager.execute_query(
                    null_check_query, fetch_mode="one"
                )
                or {}
            )

            validation_results["validation_summary"]["total_constraints_checked"] += 1

            if null_result["null_count"] > 0:
                validation_results["constraint_violations"].append(
                    {
                        "constraint_type": "NOT NULL",
                        "constraint_name": f"{table_name}_{column_name}_not_null",
                        "column_name": column_name,
                        "violation_count": null_result["null_count"],
                        "description": f"Column '{column_name}' has {null_result['null_count']} NULL values but is defined as NOT NULL",
                    }
                )
                validation_results["validation_summary"]["violations_found"] += 1

        # 2. Check PRIMARY KEY constraints
        logger.info("Checking PRIMARY KEY constraints")
        pk_query = """
        SELECT
            tc.constraint_name,
            string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'
        GROUP BY tc.constraint_name
        """

        pk_constraints = await connection_manager.execute_query(
            pk_query, [table_name], fetch_mode="all"
        )

        if pk_constraints:
            validation_results["validation_summary"]["constraint_types_checked"].append(
                "PRIMARY KEY"
            )

        for pk in pk_constraints:
            constraint_name = pk["constraint_name"]
            columns = pk["columns"].split(", ")

            # Check for duplicate primary key values
            pk_columns_quoted = [f'"{col}"' for col in columns]
            pk_duplicate_query = f"""
            SELECT {", ".join(pk_columns_quoted)}, COUNT(*) as duplicate_count
            FROM "{table_name}"
            WHERE {" AND ".join([f'"{col}" IS NOT NULL' for col in columns])}
            GROUP BY {", ".join(pk_columns_quoted)}
            HAVING COUNT(*) > 1
            """

            pk_duplicates = await connection_manager.execute_query(
                pk_duplicate_query, fetch_mode="all"
            )

            validation_results["validation_summary"]["total_constraints_checked"] += 1

            if pk_duplicates:
                duplicate_examples = []
                for dup in pk_duplicates[:5]:  # Show first 5 examples
                    example = {}
                    for col in columns:
                        example[col] = serialize_value(dup[col])
                    duplicate_examples.append(
                        {"values": example, "count": dup["duplicate_count"]}
                    )

                validation_results["constraint_violations"].append(
                    {
                        "constraint_type": "PRIMARY KEY",
                        "constraint_name": constraint_name,
                        "columns": columns,
                        "violation_count": len(pk_duplicates),
                        "description": f"Primary key constraint violated: {len(pk_duplicates)} duplicate key groups found",
                        "examples": duplicate_examples,
                    }
                )
                validation_results["validation_summary"]["violations_found"] += 1

        # 3. Check UNIQUE constraints
        logger.info("Checking UNIQUE constraints")
        unique_query = """
        SELECT
            tc.constraint_name,
            string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = $1 AND tc.constraint_type = 'UNIQUE'
        GROUP BY tc.constraint_name
        """

        unique_constraints = await connection_manager.execute_query(
            unique_query, [table_name], fetch_mode="all"
        )

        if unique_constraints:
            validation_results["validation_summary"]["constraint_types_checked"].append(
                "UNIQUE"
            )

        for unique in unique_constraints:
            constraint_name = unique["constraint_name"]
            columns = unique["columns"].split(", ")

            # Check for duplicate unique values
            unique_columns_quoted = [f'"{col}"' for col in columns]
            unique_duplicate_query = f"""
            SELECT {", ".join(unique_columns_quoted)}, COUNT(*) as duplicate_count
            FROM "{table_name}"
            WHERE {" AND ".join([f'"{col}" IS NOT NULL' for col in columns])}
            GROUP BY {", ".join(unique_columns_quoted)}
            HAVING COUNT(*) > 1
            """

            unique_duplicates = await connection_manager.execute_query(
                unique_duplicate_query, fetch_mode="all"
            )

            validation_results["validation_summary"]["total_constraints_checked"] += 1

            if unique_duplicates:
                duplicate_examples = []
                for dup in unique_duplicates[:5]:  # Show first 5 examples
                    example = {}
                    for col in columns:
                        example[col] = serialize_value(dup[col])
                    duplicate_examples.append(
                        {"values": example, "count": dup["duplicate_count"]}
                    )

                validation_results["constraint_violations"].append(
                    {
                        "constraint_type": "UNIQUE",
                        "constraint_name": constraint_name,
                        "columns": columns,
                        "violation_count": len(unique_duplicates),
                        "description": f"Unique constraint violated: {len(unique_duplicates)} duplicate value groups found",
                        "examples": duplicate_examples,
                    }
                )
                validation_results["validation_summary"]["violations_found"] += 1

        # 4. Check FOREIGN KEY constraints
        logger.info("Checking FOREIGN KEY constraints")
        fk_query = """
        SELECT
            tc.constraint_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.table_name = $1 AND tc.constraint_type = 'FOREIGN KEY'
        """

        fk_constraints = await connection_manager.execute_query(
            fk_query, [table_name], fetch_mode="all"
        )

        if fk_constraints:
            validation_results["validation_summary"]["constraint_types_checked"].append(
                "FOREIGN KEY"
            )

        for fk in fk_constraints:
            constraint_name = fk["constraint_name"]
            column_name = fk["column_name"]
            foreign_table = fk["foreign_table_name"]
            foreign_column = fk["foreign_column_name"]

            # Check for orphaned foreign key values
            fk_violation_query = f"""
            SELECT COUNT(*) as violation_count
            FROM "{table_name}" t
            WHERE t."{column_name}" IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM "{foreign_table}" f
                WHERE f."{foreign_column}" = t."{column_name}"
            )
            """

            fk_violations = (
                await connection_manager.execute_query(
                    fk_violation_query, fetch_mode="one"
                )
                or {}
            )

            validation_results["validation_summary"]["total_constraints_checked"] += 1

            if fk_violations["violation_count"] > 0:
                # Get examples of orphaned values
                fk_examples_query = f"""
                SELECT DISTINCT t."{column_name}" as orphaned_value
                FROM "{table_name}" t
                WHERE t."{column_name}" IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM "{foreign_table}" f
                    WHERE f."{foreign_column}" = t."{column_name}"
                )
                LIMIT 5
                """

                fk_examples = await connection_manager.execute_query(
                    fk_examples_query, fetch_mode="all"
                )

                validation_results["constraint_violations"].append(
                    {
                        "constraint_type": "FOREIGN KEY",
                        "constraint_name": constraint_name,
                        "column_name": column_name,
                        "foreign_table": foreign_table,
                        "foreign_column": foreign_column,
                        "violation_count": fk_violations["violation_count"],
                        "description": f"Foreign key constraint violated: {fk_violations['violation_count']} orphaned references found",
                        "examples": [
                            serialize_value(ex["orphaned_value"]) for ex in fk_examples
                        ],
                    }
                )
                validation_results["validation_summary"]["violations_found"] += 1

        # 5. Check CHECK constraints
        logger.info("Checking CHECK constraints")
        check_query = """
        SELECT
            tc.constraint_name,
            cc.check_clause
        FROM information_schema.table_constraints tc
        JOIN information_schema.check_constraints cc
            ON tc.constraint_name = cc.constraint_name
        WHERE tc.table_name = $1 AND tc.constraint_type = 'CHECK'
        """

        check_constraints = await connection_manager.execute_query(
            check_query, [table_name], fetch_mode="all"
        )

        if check_constraints:
            validation_results["validation_summary"]["constraint_types_checked"].append(
                "CHECK"
            )

        for check in check_constraints:
            constraint_name = check["constraint_name"]
            check_clause = check["check_clause"]

            # This is complex as we'd need to parse and validate the check clause
            # For now, we'll note that check constraints exist but can't validate them automatically
            validation_results["validation_summary"]["total_constraints_checked"] += 1

            # Add info about check constraints (but can't validate automatically)
            validation_results["constraint_violations"].append(
                {
                    "constraint_type": "CHECK",
                    "constraint_name": constraint_name,
                    "check_clause": check_clause,
                    "violation_count": 0,
                    "description": f"Check constraint exists but cannot be automatically validated: {check_clause}",
                    "note": "Manual validation required for CHECK constraints",
                }
            )

        logger.info(f"Constraint validation completed for table {table_name}")
        return format_analysis_result(
            "constraint_validation", table_name, None, validation_results
        )

    except ValueError as e:
        logger.error(f"Validation error in validate_constraints: {e}")
        return format_error_response("VALIDATION_ERROR", str(e))
    except Exception as e:
        logger.error(f"Error validating constraints for table {table_name}: {e}")
        return format_error_response(
            "CONSTRAINT_VALIDATION_ERROR", f"Failed to validate constraints: {e}"
        )


async def validate_data_types(
    table_name: str, column_name: str | None = None
) -> dict[str, Any]:
    """Verify column data type compliance.

    Checks if actual data in columns matches their defined data types,
    identifying values that don't conform to the expected type constraints.

    Args:
        table_name: Name of the table to validate
        column_name: Optional specific column to validate (validates all if None)

    Returns:
        Dictionary containing data type validation results

    Raises:
        ValueError: If table or column name is invalid
        Exception: If data type validation fails
    """
    try:
        # Validate inputs
        validate_table_name(table_name)
        if column_name:
            validate_column_name(column_name)

        logger.info(
            f"Validating data types for table {table_name}"
            + (f", column {column_name}" if column_name else "")
        )

        # Get column information
        if column_name:
            columns_query = """
            SELECT column_name, data_type, is_nullable, character_maximum_length,
                   numeric_precision, numeric_scale, column_default
            FROM information_schema.columns
            WHERE table_name = $1 AND column_name = $2
            ORDER BY ordinal_position
            """
            columns_info = await connection_manager.execute_query(
                columns_query, [table_name, column_name], fetch_mode="all"
            )
        else:
            columns_query = """
            SELECT column_name, data_type, is_nullable, character_maximum_length,
                   numeric_precision, numeric_scale, column_default
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position
            """
            columns_info = await connection_manager.execute_query(
                columns_query, [table_name], fetch_mode="all"
            )

        if not columns_info:
            if column_name:
                raise ValueError(
                    f"Column '{column_name}' not found in table '{table_name}'"
                )
            else:
                raise ValueError(f"Table '{table_name}' not found or has no columns")

        validation_results: dict[str, Any] = {
            "table_name": table_name,
            "column_validations": [],
            "validation_summary": {
                "total_columns_checked": len(columns_info),
                "columns_with_violations": 0,
                "total_violations": 0,
            },
        }

        for col_info in columns_info:
            col_name = col_info["column_name"]
            data_type = col_info["data_type"]
            is_nullable = col_info["is_nullable"] == "YES"
            max_length = col_info["character_maximum_length"]
            numeric_precision = col_info["numeric_precision"]
            numeric_scale = col_info["numeric_scale"]

            logger.info(f"Validating data type for column {col_name} ({data_type})")

            column_validation: dict[str, Any] = {
                "column_name": col_name,
                "expected_type": data_type,
                "is_nullable": is_nullable,
                "violations": [],
                "violation_count": 0,
            }

            # Add type-specific constraints
            if max_length:
                column_validation["max_length"] = max_length
            if numeric_precision:
                column_validation["numeric_precision"] = numeric_precision
            if numeric_scale:
                column_validation["numeric_scale"] = numeric_scale

            # Type-specific validations
            if data_type in ["integer", "bigint", "smallint"]:
                # Check for non-integer values (this shouldn't happen in a well-formed DB)
                # But we can check for values outside the range
                if data_type == "smallint":
                    range_query = f"""
                    SELECT COUNT(*) as violation_count
                    FROM "{table_name}"
                    WHERE "{col_name}" IS NOT NULL
                    AND ("{col_name}" < -32768 OR "{col_name}" > 32767)
                    """
                    range_result = (
                        await connection_manager.execute_query(
                            range_query, fetch_mode="one"
                        )
                        or {}
                    )
                    if range_result["violation_count"] > 0:
                        column_validation["violations"].append(
                            {
                                "violation_type": "out_of_range",
                                "count": range_result["violation_count"],
                                "description": "Values outside smallint range (-32768 to 32767)",
                            }
                        )

            elif data_type in ["numeric", "decimal"]:
                # Check precision and scale violations
                if numeric_precision and numeric_scale is not None:
                    precision_query = f"""
                    SELECT COUNT(*) as violation_count
                    FROM "{table_name}"
                    WHERE "{col_name}" IS NOT NULL
                    AND (
                        LENGTH(REPLACE(ABS("{col_name}")::text, '.', '')) > {numeric_precision}
                        OR LENGTH(SUBSTRING(ABS("{col_name}")::text FROM POSITION('.' IN ABS("{col_name}")::text) + 1)) > {numeric_scale}
                    )
                    """
                    precision_result = (
                        await connection_manager.execute_query(
                            precision_query, fetch_mode="one"
                        )
                        or {}
                    )
                    if precision_result["violation_count"] > 0:
                        column_validation["violations"].append(
                            {
                                "violation_type": "precision_scale_violation",
                                "count": precision_result["violation_count"],
                                "description": f"Values exceed precision({numeric_precision},{numeric_scale}) constraints",
                            }
                        )

            elif data_type in ["varchar", "char", "character varying"]:
                # Check length violations
                if max_length:
                    length_query = f"""
                    SELECT COUNT(*) as violation_count
                    FROM "{table_name}"
                    WHERE "{col_name}" IS NOT NULL
                    AND LENGTH("{col_name}") > {max_length}
                    """
                    length_result = (
                        await connection_manager.execute_query(
                            length_query, fetch_mode="one"
                        )
                        or {}
                    )
                    if length_result["violation_count"] > 0:
                        # Get examples of violating values
                        examples_query = f"""
                        SELECT "{col_name}", LENGTH("{col_name}") as actual_length
                        FROM "{table_name}"
                        WHERE "{col_name}" IS NOT NULL
                        AND LENGTH("{col_name}") > {max_length}
                        LIMIT 3
                        """
                        examples = await connection_manager.execute_query(
                            examples_query, fetch_mode="all"
                        )

                        column_validation["violations"].append(
                            {
                                "violation_type": "length_exceeded",
                                "count": length_result["violation_count"],
                                "description": f"Values exceed maximum length of {max_length} characters",
                                "examples": [
                                    {
                                        "value": serialize_value(ex[col_name])[:50]
                                        + "..."
                                        if len(str(ex[col_name])) > 50
                                        else serialize_value(ex[col_name]),
                                        "actual_length": ex["actual_length"],
                                        "max_length": max_length,
                                    }
                                    for ex in examples
                                ],
                            }
                        )

            elif data_type == "boolean":
                # Check for non-boolean values (shouldn't happen in well-formed DB)
                # PostgreSQL is pretty strict about this, so violations are rare
                pass

            elif data_type in ["date", "timestamp", "timestamptz"]:
                # Check for invalid date/timestamp formats
                # PostgreSQL is strict about this, but we can check for extreme values
                if data_type == "date":
                    date_range_query = f"""
                    SELECT COUNT(*) as violation_count
                    FROM "{table_name}"
                    WHERE "{col_name}" IS NOT NULL
                    AND ("{col_name}" < '1900-01-01'::date OR "{col_name}" > '2100-12-31'::date)
                    """
                    date_result = (
                        await connection_manager.execute_query(
                            date_range_query, fetch_mode="one"
                        )
                        or {}
                    )
                    if date_result["violation_count"] > 0:
                        column_validation["violations"].append(
                            {
                                "violation_type": "suspicious_date_range",
                                "count": date_result["violation_count"],
                                "description": "Dates outside reasonable range (1900-2100)",
                            }
                        )

            # Check for NULL values in NOT NULL columns (this overlaps with constraint validation)
            if not is_nullable:
                null_query = f"""
                SELECT COUNT(*) as null_count
                FROM "{table_name}"
                WHERE "{col_name}" IS NULL
                """
                null_result = (
                    await connection_manager.execute_query(null_query, fetch_mode="one")
                    or {}
                )
                if null_result["null_count"] > 0:
                    column_validation["violations"].append(
                        {
                            "violation_type": "unexpected_null",
                            "count": null_result["null_count"],
                            "description": "NULL values found in NOT NULL column",
                        }
                    )

            # Count total violations for this column
            column_validation["violation_count"] = sum(
                v["count"] for v in column_validation["violations"]
            )

            if column_validation["violation_count"] > 0:
                validation_results["validation_summary"]["columns_with_violations"] += 1
                validation_results["validation_summary"]["total_violations"] += (
                    column_validation["violation_count"]
                )

            validation_results["column_validations"].append(column_validation)

        logger.info(f"Data type validation completed for table {table_name}")
        return format_analysis_result(
            "data_type_validation", table_name, column_name, validation_results
        )

    except ValueError as e:
        logger.error(f"Validation error in validate_data_types: {e}")
        return format_error_response("VALIDATION_ERROR", str(e))
    except Exception as e:
        logger.error(f"Error validating data types for table {table_name}: {e}")
        return format_error_response(
            "DATA_TYPE_VALIDATION_ERROR", f"Failed to validate data types: {e}"
        )


async def check_data_integrity(
    table_name: str, comprehensive: bool = True
) -> dict[str, Any]:
    """Perform comprehensive data integrity checks.

    Combines constraint validation, data type validation, and additional
    integrity checks to provide a complete assessment of data quality.

    Args:
        table_name: Name of the table to check
        comprehensive: Whether to perform all checks (True) or basic checks only (False)

    Returns:
        Dictionary containing comprehensive data integrity results

    Raises:
        ValueError: If table name is invalid
        Exception: If integrity check fails
    """
    try:
        # Validate inputs
        validate_table_name(table_name)

        logger.info(
            f"Performing {'comprehensive' if comprehensive else 'basic'} data integrity check for table {table_name}"
        )

        # Check if table exists
        table_check_query = """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_name = $1 AND table_schema = 'public'
        """

        table_info = await connection_manager.execute_query(
            table_check_query, [table_name], fetch_mode="one"
        )

        if not table_info:
            raise ValueError(f"Table '{table_name}' not found")

        integrity_results: dict[str, Any] = {
            "table_name": table_name,
            "check_type": "comprehensive" if comprehensive else "basic",
            "timestamp": None,  # Will be set by format_analysis_result
            "checks_performed": [],
            "integrity_summary": {
                "overall_status": "PASS",
                "total_issues": 0,
                "critical_issues": 0,
                "warning_issues": 0,
                "info_issues": 0,
            },
            "detailed_results": {},
        }

        # 1. Basic table statistics
        logger.info("Gathering basic table statistics")
        stats_query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT ctid) as distinct_rows
        FROM "{table_name}"
        """

        basic_stats = (
            await connection_manager.execute_query(stats_query, fetch_mode="one") or {}
        )

        integrity_results["detailed_results"]["basic_stats"] = {
            "total_rows": basic_stats["total_rows"],
            "distinct_rows": basic_stats["distinct_rows"],
            "has_duplicate_rows": basic_stats["total_rows"]
            != basic_stats["distinct_rows"],
        }

        integrity_results["checks_performed"].append("basic_statistics")

        # 2. Constraint validation
        logger.info("Running constraint validation")
        constraint_results = await validate_constraints(table_name)

        if "error" in constraint_results:
            integrity_results["detailed_results"]["constraint_validation"] = {
                "status": "ERROR",
                "error": constraint_results["error"]["message"],
            }
            integrity_results["integrity_summary"]["critical_issues"] += 1
        else:
            constraint_data = constraint_results["results"]
            violations_count = constraint_data["validation_summary"]["violations_found"]

            integrity_results["detailed_results"]["constraint_validation"] = {
                "status": "PASS" if violations_count == 0 else "FAIL",
                "violations_found": violations_count,
                "constraints_checked": constraint_data["validation_summary"][
                    "total_constraints_checked"
                ],
                "constraint_types": constraint_data["validation_summary"][
                    "constraint_types_checked"
                ],
            }

            if violations_count > 0:
                integrity_results["integrity_summary"]["critical_issues"] += (
                    violations_count
                )

        integrity_results["checks_performed"].append("constraint_validation")

        # 3. Data type validation
        logger.info("Running data type validation")
        datatype_results = await validate_data_types(table_name)

        if "error" in datatype_results:
            integrity_results["detailed_results"]["data_type_validation"] = {
                "status": "ERROR",
                "error": datatype_results["error"]["message"],
            }
            integrity_results["integrity_summary"]["critical_issues"] += 1
        else:
            datatype_data = datatype_results["results"]
            violations_count = datatype_data["validation_summary"]["total_violations"]

            integrity_results["detailed_results"]["data_type_validation"] = {
                "status": "PASS" if violations_count == 0 else "FAIL",
                "violations_found": violations_count,
                "columns_checked": datatype_data["validation_summary"][
                    "total_columns_checked"
                ],
                "columns_with_violations": datatype_data["validation_summary"][
                    "columns_with_violations"
                ],
            }

            if violations_count > 0:
                integrity_results["integrity_summary"]["warning_issues"] += (
                    violations_count
                )

        integrity_results["checks_performed"].append("data_type_validation")

        if comprehensive:
            # 4. Orphaned records check (if there are foreign keys)
            logger.info("Checking for orphaned records")
            fk_query = """
            SELECT COUNT(*) as fk_count
            FROM information_schema.table_constraints
            WHERE table_name = $1 AND constraint_type = 'FOREIGN KEY'
            """

            fk_count_result = (
                await connection_manager.execute_query(
                    fk_query, [table_name], fetch_mode="one"
                )
                or {}
            )

            if fk_count_result["fk_count"] > 0:
                # This is already covered in constraint validation
                integrity_results["detailed_results"]["orphaned_records"] = {
                    "status": "COVERED_IN_CONSTRAINTS",
                    "note": "Orphaned record checks are included in foreign key constraint validation",
                }
            else:
                integrity_results["detailed_results"]["orphaned_records"] = {
                    "status": "NOT_APPLICABLE",
                    "note": "No foreign key constraints found",
                }

            integrity_results["checks_performed"].append("orphaned_records")

            # 5. Data distribution analysis
            logger.info("Analyzing data distribution")
            columns_query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position
            LIMIT 10  -- Limit to first 10 columns for performance
            """

            columns_info = await connection_manager.execute_query(
                columns_query, [table_name], fetch_mode="all"
            )

            distribution_issues: list[dict[str, Any]] = []
            for col_info in columns_info:
                col_name = col_info["column_name"]

                # Check for columns with all NULL values
                null_check_query = f"""
                SELECT
                    COUNT(*) as total_count,
                    COUNT("{col_name}") as non_null_count
                FROM "{table_name}"
                """

                null_check = (
                    await connection_manager.execute_query(
                        null_check_query, fetch_mode="one"
                    )
                    or {}
                )

                if null_check["non_null_count"] == 0 and null_check["total_count"] > 0:
                    distribution_issues.append(
                        {
                            "column": col_name,
                            "issue": "all_null_values",
                            "description": f"Column '{col_name}' contains only NULL values",
                        }
                    )

                # Check for columns with all identical values (excluding NULL)
                elif null_check["non_null_count"] > 1:
                    distinct_check_query = f"""
                    SELECT COUNT(DISTINCT "{col_name}") as distinct_count
                    FROM "{table_name}"
                    WHERE "{col_name}" IS NOT NULL
                    """

                    distinct_check = (
                        await connection_manager.execute_query(
                            distinct_check_query, fetch_mode="one"
                        )
                        or {}
                    )

                    if distinct_check["distinct_count"] == 1:
                        distribution_issues.append(
                            {
                                "column": col_name,
                                "issue": "single_value",
                                "description": f"Column '{col_name}' contains only one distinct non-NULL value",
                            }
                        )

            integrity_results["detailed_results"]["data_distribution"] = {
                "status": "PASS" if len(distribution_issues) == 0 else "WARNING",
                "issues_found": len(distribution_issues),
                "issues": distribution_issues,
            }

            if distribution_issues:
                integrity_results["integrity_summary"]["info_issues"] += len(
                    distribution_issues
                )

            integrity_results["checks_performed"].append("data_distribution")

            # 6. Table health metrics
            logger.info("Gathering table health metrics")
            health_query = f"""
            SELECT
                schemaname,
                tablename,
                n_tup_ins as inserts,
                n_tup_upd as updates,
                n_tup_del as deletes,
                n_live_tup as live_tuples,
                n_dead_tup as dead_tuples,
                last_vacuum,
                last_autovacuum,
                last_analyze,
                last_autoanalyze
            FROM pg_stat_user_tables
            WHERE tablename = '{table_name}'
            """

            try:
                health_stats = await connection_manager.execute_query(
                    health_query, fetch_mode="one"
                )

                if health_stats:
                    dead_tuple_ratio = 0
                    if health_stats["live_tuples"] and health_stats["live_tuples"] > 0:
                        dead_tuple_ratio = health_stats["dead_tuples"] / (
                            health_stats["live_tuples"] + health_stats["dead_tuples"]
                        )

                    health_issues = []
                    if dead_tuple_ratio > 0.1:  # More than 10% dead tuples
                        health_issues.append(
                            {
                                "issue": "high_dead_tuple_ratio",
                                "value": round(dead_tuple_ratio * 100, 2),
                                "description": f"High dead tuple ratio ({dead_tuple_ratio:.1%}) - consider VACUUM",
                            }
                        )

                    integrity_results["detailed_results"]["table_health"] = {
                        "status": "PASS" if len(health_issues) == 0 else "WARNING",
                        "live_tuples": health_stats["live_tuples"],
                        "dead_tuples": health_stats["dead_tuples"],
                        "dead_tuple_ratio": round(dead_tuple_ratio * 100, 2),
                        "last_vacuum": serialize_value(health_stats["last_vacuum"]),
                        "last_analyze": serialize_value(health_stats["last_analyze"]),
                        "issues": health_issues,
                    }

                    if health_issues:
                        integrity_results["integrity_summary"]["info_issues"] += len(
                            health_issues
                        )
                else:
                    integrity_results["detailed_results"]["table_health"] = {
                        "status": "NO_DATA",
                        "note": "No statistics available (table may be new or statistics not collected)",
                    }

            except Exception as e:
                logger.warning(f"Could not gather table health metrics: {e}")
                integrity_results["detailed_results"]["table_health"] = {
                    "status": "ERROR",
                    "error": str(e),
                }

            integrity_results["checks_performed"].append("table_health")

        # Calculate overall status
        total_issues = (
            integrity_results["integrity_summary"]["critical_issues"]
            + integrity_results["integrity_summary"]["warning_issues"]
            + integrity_results["integrity_summary"]["info_issues"]
        )

        integrity_results["integrity_summary"]["total_issues"] = total_issues

        if integrity_results["integrity_summary"]["critical_issues"] > 0:
            integrity_results["integrity_summary"]["overall_status"] = "CRITICAL"
        elif integrity_results["integrity_summary"]["warning_issues"] > 0:
            integrity_results["integrity_summary"]["overall_status"] = "WARNING"
        elif integrity_results["integrity_summary"]["info_issues"] > 0:
            integrity_results["integrity_summary"]["overall_status"] = "INFO"
        else:
            integrity_results["integrity_summary"]["overall_status"] = "PASS"

        logger.info(f"Data integrity check completed for table {table_name}")
        return format_analysis_result(
            "data_integrity_check", table_name, None, integrity_results
        )

    except ValueError as e:
        logger.error(f"Validation error in check_data_integrity: {e}")
        return format_error_response("VALIDATION_ERROR", str(e))
    except Exception as e:
        logger.error(f"Error checking data integrity for table {table_name}: {e}")
        return format_error_response(
            "DATA_INTEGRITY_ERROR", f"Failed to check data integrity: {e}"
        )


# Tool schema definitions for MCP registration
VALIDATE_CONSTRAINTS_SCHEMA = {
    "name": "validate_constraints",
    "description": "Validate table constraints and identify any constraint violations",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to validate constraints for",
            },
            "constraint_types": {
                "type": "array",
                "description": "Types of constraints to validate (optional, defaults to all)",
                "items": {
                    "type": "string",
                    "enum": [
                        "PRIMARY KEY",
                        "FOREIGN KEY",
                        "UNIQUE",
                        "CHECK",
                        "NOT NULL",
                    ],
                },
            },
            "fix_violations": {
                "type": "boolean",
                "description": "Whether to attempt to fix violations (WARNING: may modify data)",
                "default": False,
            },
        },
        "required": ["table_name"],
    },
}

VALIDATE_DATA_TYPES_SCHEMA = {
    "name": "validate_data_types",
    "description": "Validate data type compliance and identify type conversion issues",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to validate data types for",
            },
            "columns": {
                "type": "array",
                "description": "Specific columns to validate (optional, defaults to all columns)",
                "items": {"type": "string"},
            },
            "strict_mode": {
                "type": "boolean",
                "description": "Whether to use strict type validation",
                "default": False,
            },
        },
        "required": ["table_name"],
    },
}

CHECK_DATA_INTEGRITY_SCHEMA = {
    "name": "check_data_integrity",
    "description": "Perform comprehensive data integrity checks including constraints, types, and relationships",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Optional table name to focus integrity checks on specific table",
            },
            "schema_name": {
                "type": "string",
                "description": "Schema name (defaults to 'public')",
                "default": "public",
            },
            "check_types": {
                "type": "array",
                "description": "Types of integrity checks to perform",
                "items": {
                    "type": "string",
                    "enum": [
                        "constraints",
                        "data_types",
                        "referential_integrity",
                        "duplicates",
                    ],
                },
                "default": ["constraints", "data_types", "referential_integrity"],
            },
            "detailed_report": {
                "type": "boolean",
                "description": "Whether to include detailed violation information",
                "default": True,
            },
        },
        "required": [],
    },
}

# Export tool functions and schemas
__all__ = [
    "validate_constraints",
    "validate_data_types",
    "check_data_integrity",
    "VALIDATE_CONSTRAINTS_SCHEMA",
    "VALIDATE_DATA_TYPES_SCHEMA",
    "CHECK_DATA_INTEGRITY_SCHEMA",
]
