"""
Example 2: Forecast Revisions - Handling multiple forecast runs

This example demonstrates:
- Creating multiple forecast runs for the same time period
- How timedb handles overlapping forecasts
- Reading data in different modes (flat vs overlapping)
"""
import os
import uuid
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from timedb.db import create, insert, read

load_dotenv()


def main():
    conninfo = os.environ.get("TIMEDB_DSN") or os.environ.get("DATABASE_URL")
    if not conninfo:
        print("ERROR: Set TIMEDB_DSN or DATABASE_URL environment variable")
        return
    
    print("=" * 60)
    print("Example 2: Forecast Revisions")
    print("=" * 60)
    
    # Create schema
    print("\n1. Creating database schema...")
    create.create_schema(conninfo)
    print("   ✓ Schema created")
    
    # Create multiple forecast runs for the same time period
    base_time = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    tenant_id = uuid.uuid4()  # In production, this would come from context
    series_id = uuid.uuid4()  # In production, this would come from context
    
    print("\n2. Creating multiple forecast runs...")
    
    # Run 1: Initial forecast (made at 10:00)
    run1_id = uuid.uuid4()
    run1_time = base_time - timedelta(hours=2)  # Forecast made 2 hours before
    
    value_rows_1 = [
        (tenant_id, base_time + timedelta(hours=i), series_id, "mean", 100.0 + i * 0.5)
        for i in range(6)
    ]
    
    insert.insert_run_with_values(
        conninfo,
        run_id=run1_id,
        tenant_id=tenant_id,
        workflow_id="forecast-v1",
        run_start_time=run1_time,
        known_time=run1_time,  # When this forecast was known
        value_rows=value_rows_1,
    )
    print(f"   ✓ Run 1: {run1_id} (forecast made at {run1_time})")
    
    # Run 2: Revised forecast (made at 11:00, one hour later)
    run2_id = uuid.uuid4()
    run2_time = base_time - timedelta(hours=1)  # Forecast made 1 hour before
    
    value_rows_2 = [
        (tenant_id, base_time + timedelta(hours=i), series_id, "mean", 102.0 + i * 0.6)  # Slightly different values
        for i in range(6)
    ]
    
    insert.insert_run_with_values(
        conninfo,
        run_id=run2_id,
        tenant_id=tenant_id,
        workflow_id="forecast-v2",
        run_start_time=run2_time,
        known_time=run2_time,
        value_rows=value_rows_2,
    )
    print(f"   ✓ Run 2: {run2_id} (forecast made at {run2_time})")
    
    # Step 3: Read in flat mode (gets latest known_time per valid_time)
    print("\n3. Reading in 'flat' mode (latest forecast per timestamp)...")
    df_flat = read.read_values_between(
        conninfo,
        tenant_id=tenant_id,
        start_valid=base_time,
        end_valid=base_time + timedelta(hours=6),
        mode="flat",
    )
    print(f"   ✓ Retrieved {len(df_flat)} values")
    print("\n   Values (showing latest forecast):")
    print(df_flat)
    
    # Step 4: Read in overlapping mode (shows all forecasts)
    print("\n4. Reading in 'overlapping' mode (all forecasts)...")
    df_overlapping = read.read_values_between(
        conninfo,
        tenant_id=tenant_id,
        start_valid=base_time,
        end_valid=base_time + timedelta(hours=6),
        mode="overlapping",
    )
    print(f"   ✓ Retrieved {len(df_overlapping)} values")
    print("\n   Values (showing all forecast versions):")
    print(df_overlapping)
    
    print("\n" + "=" * 60)
    print("Example completed!")
    print("=" * 60)
    print("\nKey insight: 'flat' mode shows the latest forecast, while")
    print("'overlapping' mode shows all forecast revisions for backtesting.")


if __name__ == "__main__":
    main()

