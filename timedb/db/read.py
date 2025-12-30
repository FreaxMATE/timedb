import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timezone
from typing import Optional, Literal
from importlib import resources
import uuid

load_dotenv()

# Read packaged SQL (legacy query, kept for backward compatibility)
SQL_QUERY = resources.files("timedb").joinpath("sql", "pg_read_table.sql").read_text(encoding="utf-8")


def read_values_between(
    conninfo: str,
    *,
    tenant_id: Optional[uuid.UUID] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    mode: Literal["flat", "overlapping"] = "flat",
    all_versions: bool = False,
) -> pd.DataFrame:
    """
    Read time series values from the database.
    
    Args:
        conninfo: Database connection string
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of known_time range (optional)
        end_known: End of known_time range (optional)
        mode: Query mode - "flat" or "overlapping" (default: "flat")
            - "flat": Returns (valid_time, value_key, value) with latest known_time per valid_time
            - "overlapping": Returns (known_time, valid_time, value_key, value) - all rows
        all_versions: If True, include all versions (not just current). If False, only is_current=True (default: False)
    
    Returns:
        DataFrame with columns and index based on mode
    """
    
    # Build WHERE clause filters
    filters = []
    
    # Tenant filter (required for multi-tenant, optional for single-tenant)
    if tenant_id is not None:
        filters.append("v.tenant_id = %(tenant_id)s")
        filters.append("r.tenant_id = %(tenant_id)s")
    
    # Time range filters
    if start_valid is not None:
        filters.append("v.valid_time >= %(start_valid)s")
    if end_valid is not None:
        filters.append("v.valid_time < %(end_valid)s")
    if start_known is not None:
        filters.append("r.known_time >= %(start_known)s")
    if end_known is not None:
        filters.append("r.known_time < %(end_known)s")
    
    # is_current filter (only if all_versions is False)
    if not all_versions:
        filters.append("v.is_current = true")
    
    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)
    
    if mode == "flat":
        # Flat mode: (valid_time, value_key, value) with latest known_time per valid_time
        # Include valid_time_end in DISTINCT ON to handle interval values correctly
        # Use latest known_time to select which row when multiple exist for same (valid_time, value_key)
        sql = f"""
        SELECT DISTINCT ON (v.valid_time, COALESCE(v.valid_time_end, v.valid_time), v.value_key)
            v.valid_time,
            v.value_key,
            v.value
        FROM values_table v
        JOIN runs_table r ON v.run_id = r.run_id AND v.tenant_id = r.tenant_id
        {where_clause}
        ORDER BY v.valid_time, COALESCE(v.valid_time_end, v.valid_time), v.value_key, r.known_time DESC;
        """
        
        engine = create_engine(conninfo)
        params = {
            "tenant_id": tenant_id,
            "start_valid": start_valid,
            "end_valid": end_valid,
            "start_known": start_known,
            "end_known": end_known,
        }
        df = pd.read_sql_query(sql, engine, params=params)
        
        # Ensure timezone-aware pandas datetimes
        df["valid_time"] = pd.to_datetime(df["valid_time"], utc=True)
        
        # Set index on (valid_time, value_key)
        df = df.set_index(["valid_time", "value_key"]).sort_index()
        
    elif mode == "overlapping":
        # Overlapping mode: (known_time, valid_time, value_key, value)
        sql = f"""
        SELECT
            r.known_time,
            v.valid_time,
            v.value_key,
            v.value
        FROM values_table v
        JOIN runs_table r ON v.run_id = r.run_id AND v.tenant_id = r.tenant_id
        {where_clause}
        ORDER BY r.known_time, v.valid_time, v.value_key;
        """
        
        engine = create_engine(conninfo)
        params = {
            "tenant_id": tenant_id,
            "start_valid": start_valid,
            "end_valid": end_valid,
            "start_known": start_known,
            "end_known": end_known,
        }
        df = pd.read_sql_query(sql, engine, params=params)
        
        # Ensure timezone-aware pandas datetimes
        df["known_time"] = pd.to_datetime(df["known_time"], utc=True)
        df["valid_time"] = pd.to_datetime(df["valid_time"], utc=True)
        
        # Set index on (known_time, valid_time, value_key)
        df = df.set_index(["known_time", "valid_time", "value_key"]).sort_index()
        
    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'flat' or 'overlapping'")
    
    return df


if __name__ == "__main__":
    conninfo = os.environ["NEON_PG_URL"]

    # Example 1: filter only on valid_time
    df1 = read_values_between(
        conninfo,
        start_valid=datetime(2025, 12, 25, 0, 0, tzinfo=timezone.utc),
        end_valid=datetime(2025, 12, 28, 0, 0, tzinfo=timezone.utc),
    )

    # Example 2: filter only on known_time
    df2 = read_values_between(
        conninfo,
        start_known=datetime(2025, 12, 26, 0, 0, tzinfo=timezone.utc),
        end_known=datetime(2025, 12, 27, 0, 0, tzinfo=timezone.utc),
    )

    # Example 3: filter on both valid_time and known_time
    df3 = read_values_between(
        conninfo,
        start_valid=datetime(2025, 12, 25, 0, 0, tzinfo=timezone.utc),
        end_valid=datetime(2025, 12, 28, 0, 0, tzinfo=timezone.utc),
        start_known=datetime(2025, 12, 26, 0, 0, tzinfo=timezone.utc),
        end_known=datetime(2025, 12, 27, 0, 0, tzinfo=timezone.utc),
    )

    print(df1.head())
