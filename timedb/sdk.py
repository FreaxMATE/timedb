"""
High-level SDK for TimeDB.

Provides a simple interface for working with TimeDB, including automatic
DataFrame conversion for time series data.
"""
import os
import uuid
from typing import Optional, List, Tuple, NamedTuple
from datetime import datetime, timezone
import pandas as pd

from dotenv import load_dotenv, find_dotenv
from . import db
import psycopg
from psycopg import errors

load_dotenv(find_dotenv())

# Default tenant ID for single-tenant installations (all zeros UUID)
DEFAULT_TENANT_ID = uuid.UUID('00000000-0000-0000-0000-000000000000')


class InsertResult(NamedTuple):
    """Result from insert_run containing the IDs that were used."""
    run_id: uuid.UUID
    workflow_id: str
    entity_id: uuid.UUID
    tenant_id: uuid.UUID


def _get_conninfo() -> str:
    """Get database connection string from environment variables."""
    conninfo = os.environ.get("TIMEDB_DSN") or os.environ.get("DATABASE_URL")
    if not conninfo:
        raise ValueError(
            "Database connection not configured. Set TIMEDB_DSN or DATABASE_URL environment variable."
        )
    return conninfo


def _dataframe_to_value_rows(
    df: pd.DataFrame,
    tenant_id: uuid.UUID,
    entity_id: uuid.UUID,
    valid_time_col: str = 'valid_time',
    valid_time_end_col: Optional[str] = None,
    value_key: Optional[str] = None,
    value_columns: Optional[List[str]] = None,
) -> List[Tuple]:
    """
    Convert a pandas DataFrame to TimeDB value_rows format.
    
    Args:
        df: DataFrame with time series data
        tenant_id: Tenant UUID
        entity_id: Entity UUID
        valid_time_col: Column name for valid_time (default: 'valid_time')
        valid_time_end_col: Column name for valid_time_end (None for point-in-time)
        value_key: The value key name for single value column (default: 'value')
        value_columns: List of column names to use as value keys for multiple columns.
                      If None and value_key is None, uses all columns except valid_time_col and valid_time_end_col
    
    Returns:
        List of tuples in TimeDB format:
        - Point-in-time: (tenant_id, valid_time, entity_id, value_key, value)
        - Interval: (tenant_id, valid_time, valid_time_end, entity_id, value_key, value)
    """
    if valid_time_col not in df.columns:
        raise ValueError(f"Column '{valid_time_col}' not found in DataFrame")
    
    # Determine if we have interval values
    has_intervals = valid_time_end_col is not None and valid_time_end_col in df.columns
    
    # Determine value columns
    if value_columns is not None:
        # Explicit list provided
        value_cols = value_columns
        # Validate columns exist
        missing_cols = [col for col in value_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Columns not found in DataFrame: {missing_cols}")
        
        # Melt if multiple columns
        if len(value_cols) > 1:
            df_melted = df.melt(
                id_vars=[valid_time_col] + ([valid_time_end_col] if has_intervals else []),
                value_vars=value_cols,
                var_name='value_key',
                value_name='value'
            )
        else:
            # Single column, no need to melt
            df_melted = df[[valid_time_col] + ([valid_time_end_col] if has_intervals else []) + value_cols].copy()
            # Use value_key if provided, otherwise use column name
            df_melted['value_key'] = value_key if value_key is not None else value_cols[0]
            df_melted['value'] = df_melted[value_cols[0]]
    elif value_key is not None:
        # value_key explicitly specified
        # Auto-detect value column: look for 'value' first, then single remaining column
        exclude_cols = {valid_time_col}
        if has_intervals:
            exclude_cols.add(valid_time_end_col)
        remaining_cols = [col for col in df.columns if col not in exclude_cols]
        
        if 'value' in df.columns and 'value' not in exclude_cols:
            value_col = 'value'
        elif len(remaining_cols) == 1:
            value_col = remaining_cols[0]
        else:
            raise ValueError(
                f"Cannot determine value column. Found columns: {remaining_cols}. "
                "Either specify value_columns or ensure DataFrame has a single value column or a 'value' column."
            )
        
        value_cols = [value_col]
        df_melted = df[[valid_time_col] + ([valid_time_end_col] if has_intervals else []) + value_cols].copy()
        df_melted['value_key'] = value_key  # Use explicitly provided value_key
        df_melted['value'] = df_melted[value_col]
    else:
        # Auto-detect: use all columns except time columns
        exclude_cols = {valid_time_col}
        if has_intervals:
            exclude_cols.add(valid_time_end_col)
        value_cols = [col for col in df.columns if col not in exclude_cols]
        
        if len(value_cols) == 0:
            raise ValueError("No value columns found in DataFrame")
        elif len(value_cols) == 1:
            # Single column - use column name as value_key
            df_melted = df[[valid_time_col] + ([valid_time_end_col] if has_intervals else []) + value_cols].copy()
            df_melted['value_key'] = value_cols[0]  # Use column name as value_key
            df_melted['value'] = df_melted[value_cols[0]]
        else:
            # Multiple columns, melt
            df_melted = df.melt(
                id_vars=[valid_time_col] + ([valid_time_end_col] if has_intervals else []),
                value_vars=value_cols,
                var_name='value_key',
                value_name='value'
            )
    
    # Convert to TimeDB format
    rows = []
    for _, row in df_melted.iterrows():
        valid_time = row[valid_time_col]
        
        # Ensure timezone-aware
        if isinstance(valid_time, pd.Timestamp):
            if valid_time.tzinfo is None:
                raise ValueError(f"valid_time must be timezone-aware. Found timezone-naive datetime.")
        elif isinstance(valid_time, datetime):
            if valid_time.tzinfo is None:
                raise ValueError(f"valid_time must be timezone-aware. Found timezone-naive datetime.")
        
        if has_intervals:
            valid_time_end = row[valid_time_end_col]
            if isinstance(valid_time_end, pd.Timestamp):
                if valid_time_end.tzinfo is None:
                    raise ValueError(f"valid_time_end must be timezone-aware. Found timezone-naive datetime.")
            elif isinstance(valid_time_end, datetime):
                if valid_time_end.tzinfo is None:
                    raise ValueError(f"valid_time_end must be timezone-aware. Found timezone-naive datetime.")
            
            rows.append((
                tenant_id,
                valid_time,
                valid_time_end,
                entity_id,
                row['value_key'],
                row['value']
            ))
        else:
            rows.append((
                tenant_id,
                valid_time,
                entity_id,
                row['value_key'],
                row['value']
            ))
    
    return rows


def create() -> None:
    """
    Create or update the database schema.
    
    This function creates the TimeDB tables. It's safe to run multiple times.
    Uses connection string from TIMEDB_DSN or DATABASE_URL environment variable.
    """
    conninfo = _get_conninfo()
    db.create.create_schema(conninfo)


def delete() -> None:
    """
    Delete all TimeDB tables and views.
    
    WARNING: This will delete all data! Use with caution.
    Uses connection string from TIMEDB_DSN or DATABASE_URL environment variable.
    """
    conninfo = _get_conninfo()
    db.delete.delete_schema(conninfo)


def read(
    entity_id: Optional[uuid.UUID] = None,
    tenant_id: Optional[uuid.UUID] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    value_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Read time series values from TimeDB into a pandas DataFrame.
    
    Args:
        entity_id: Filter by entity ID (optional)
        tenant_id: Filter by tenant ID (optional, defaults to zeros UUID for single-tenant)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        value_key: Filter by value key (optional)
    
    Returns:
        DataFrame with columns: valid_time, value_key, value, entity_id
        Indexed by (valid_time, value_key) if entity_id not provided, or (valid_time, value_key, entity_id) if entity_id provided
    """
    conninfo = _get_conninfo()
    
    if tenant_id is None:
        tenant_id = DEFAULT_TENANT_ID
    
    # Build SQL query with entity_id support
    filters = ["v.tenant_id = %(tenant_id)s", "r.tenant_id = %(tenant_id)s", "v.is_current = true"]
    params = {"tenant_id": tenant_id}
    
    if entity_id is not None:
        filters.append("v.entity_id = %(entity_id)s")
        params["entity_id"] = entity_id
    
    if start_valid is not None:
        filters.append("v.valid_time >= %(start_valid)s")
        params["start_valid"] = start_valid
    
    if end_valid is not None:
        filters.append("v.valid_time < %(end_valid)s")
        params["end_valid"] = end_valid
    
    if value_key is not None:
        filters.append("v.value_key = %(value_key)s")
        params["value_key"] = value_key
    
    where_clause = "WHERE " + " AND ".join(filters)
    
    # Include entity_id in SELECT for proper filtering and display
    # Use DISTINCT ON to get only the latest version per (valid_time, value_key, entity_id)
    # Order by known_time DESC to ensure we get the most recent version
    sql = f"""
        SELECT DISTINCT ON (v.valid_time, COALESCE(v.valid_time_end, v.valid_time), v.value_key, v.entity_id)
            v.valid_time,
            v.value_key,
            v.value,
            v.entity_id
        FROM values_table v
        JOIN runs_table r ON v.run_id = r.run_id AND v.tenant_id = r.tenant_id
        {where_clause}
        ORDER BY v.valid_time, COALESCE(v.valid_time_end, v.valid_time), v.value_key, v.entity_id, r.known_time DESC;
    """
    
    try:
        with psycopg.connect(conninfo) as conn:
            df = pd.read_sql(sql, conn, params=params)
    except (errors.UndefinedTable, errors.UndefinedObject) as e:
        # Check if it's a table-related error
        error_msg = str(e)
        if "values_table" in error_msg or "runs_table" in error_msg or "does not exist" in error_msg:
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise
    except Exception as e:
        # Handle case where pandas wraps the psycopg error
        error_msg = str(e)
        if "values_table" in error_msg or "runs_table" in error_msg or "does not exist" in error_msg:
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise
    
    # Ensure timezone-aware pandas datetimes
    df["valid_time"] = pd.to_datetime(df["valid_time"], utc=True)
    
    # Always include entity_id in index for proper grouping
    df = df.set_index(["valid_time", "value_key", "entity_id"]).sort_index()
    
    return df


def insert_run(
    df: pd.DataFrame,
    tenant_id: Optional[uuid.UUID] = None,
    run_id: Optional[uuid.UUID] = None,
    workflow_id: Optional[str] = None,
    run_start_time: Optional[datetime] = None,
    entity_id: Optional[uuid.UUID] = None,
    run_finish_time: Optional[datetime] = None,
    valid_time_col: str = 'valid_time',
    valid_time_end_col: Optional[str] = None,
    value_key: Optional[str] = None,
    value_columns: Optional[List[str]] = None,
    known_time: Optional[datetime] = None,
    run_params: Optional[dict] = None,
) -> InsertResult:
    """
    Insert a run with time series data from a pandas DataFrame.
    
    This function automatically converts the DataFrame to TimeDB format and inserts
    both the run metadata and the time series values atomically.
    
    Args:
        df: DataFrame containing time series data (required)
        tenant_id: Tenant UUID (optional, defaults to zeros UUID for single-tenant installations)
        run_id: Unique identifier for the run (optional, auto-generated if not provided)
        workflow_id: Workflow identifier (optional, defaults to "sdk-insert" if not provided)
        run_start_time: Start time of the run (optional, defaults to current UTC time)
        entity_id: Entity UUID for the time series (optional, auto-generated if not provided).
                   **Important**: If you plan to update this time series later, provide entity_id
                   or save the returned entity_id from the InsertResult. Without it, you won't
                   be able to identify which entity to update.
        run_finish_time: Optional finish time of the run (must be timezone-aware if provided)
        valid_time_col: Column name for valid_time (default: 'valid_time')
        valid_time_end_col: Column name for valid_time_end for interval values (optional)
        value_key: Value key name for single value column (optional, uses column name if not provided)
        value_columns: List of column names to use as value keys for multiple columns (optional)
        known_time: Time of knowledge - when the data was known/available (optional)
        run_params: Optional dictionary of run parameters (will be stored as JSONB)
    
    Returns:
        InsertResult: Named tuple containing (run_id, workflow_id, entity_id, tenant_id) that were used.
                      Use these IDs for future updates or queries.
    
    Examples:
        # Minimal usage - only DataFrame required! (single-tenant installation)
        result = td.insert_run(
            df=df,  # DataFrame with 'valid_time' and 'value' columns
        )
        print(f"Inserted with entity_id: {result.entity_id}")
        
        # Simple time series with single value column (value_key from column name)
        result = td.insert_run(
            df=df,  # DataFrame with 'valid_time' and 'value' columns
            entity_id=entity_id,  # Provide if you want to update this entity later
        )
        
        # Multiple value keys (will auto-melt, column names become value_keys)
        result = td.insert_run(
            df=df,  # DataFrame with 'valid_time', 'mean', 'quantile:0.1', etc.
            entity_id=entity_id,
            value_columns=['mean', 'quantile:0.1', 'quantile:0.9']  # Optional: specify columns
        )
        
        # Interval values
        result = td.insert_run(
            df=df,  # DataFrame with 'valid_time', 'valid_time_end', and 'value' columns
            entity_id=entity_id,
            valid_time_end_col='valid_time_end',
        )
        
        # Multi-tenant usage (provide tenant_id)
        result = td.insert_run(
            df=df,
            tenant_id=my_tenant_id,  # Explicit tenant for multi-tenant installations
        )
    """
    conninfo = _get_conninfo()
    
    # Auto-generate missing IDs
    if tenant_id is None:
        tenant_id = DEFAULT_TENANT_ID
    
    if run_id is None:
        run_id = uuid.uuid4()
    
    if workflow_id is None:
        workflow_id = "sdk-insert"
    
    if run_start_time is None:
        run_start_time = datetime.now(timezone.utc)
    elif run_start_time.tzinfo is None:
        raise ValueError("run_start_time must be timezone-aware")
    
    if entity_id is None:
        entity_id = uuid.uuid4()
    
    # Convert DataFrame to value_rows
    value_rows = _dataframe_to_value_rows(
        df=df,
        tenant_id=tenant_id,
        entity_id=entity_id,
        valid_time_col=valid_time_col,
        valid_time_end_col=valid_time_end_col,
        value_key=value_key,
        value_columns=value_columns,
    )
    
    # Insert run with values
    try:
        db.insert.insert_run_with_values(
            conninfo=conninfo,
            run_id=run_id,
            tenant_id=tenant_id,
            workflow_id=workflow_id,
            run_start_time=run_start_time,
            run_finish_time=run_finish_time,
            value_rows=value_rows,
            known_time=known_time,
            run_params=run_params,
        )
    except (errors.UndefinedTable, errors.UndefinedObject) as e:
        # Check if it's a table-related error
        error_msg = str(e)
        if "runs_table" in error_msg or "values_table" in error_msg or "does not exist" in error_msg:
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise
    except Exception as e:
        # Handle case where the error might be wrapped
        error_msg = str(e)
        if "runs_table" in error_msg or "values_table" in error_msg or "does not exist" in error_msg:
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise
    
    # Return the IDs that were used (including auto-generated ones)
    return InsertResult(
        run_id=run_id,
        workflow_id=workflow_id,
        entity_id=entity_id,
        tenant_id=tenant_id,
    )

