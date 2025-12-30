"""
Example 4: Interval Values - Representing time ranges

This example demonstrates:
- Storing interval values (valid_time to valid_time_end)
- Mixing point-in-time and interval values
- Querying interval data
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
    print("Example 4: Interval Values")
    print("=" * 60)
    
    # Create schema
    print("\n1. Creating database schema...")
    create.create_schema(conninfo)
    print("   ✓ Schema created")
    
    # Insert a mix of point-in-time and interval values
    print("\n2. Inserting mixed point-in-time and interval values...")
    run_id = uuid.uuid4()
    tenant_id = uuid.uuid4()  # In production, this would come from context
    entity_id = uuid.uuid4()  # In production, this would come from context
    base_time = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
    
    value_rows = [
        # Point-in-time values (valid_time_end is None)
        (tenant_id, entity_id, base_time, "instantaneous_power", 100.0),
        (tenant_id, entity_id, base_time + timedelta(hours=1), "instantaneous_power", 105.0),
        (tenant_id, entity_id, base_time + timedelta(hours=2), "instantaneous_power", 110.0),
        
        # Interval values (tenant_id, entity_id, valid_time, valid_time_end, value_key, value)
        (
            tenant_id,
            entity_id,
            base_time,
            base_time + timedelta(hours=1),
            "energy_consumed",
            50.0  # Energy consumed over the hour
        ),
        (
            tenant_id,
            entity_id,
            base_time + timedelta(hours=1),
            base_time + timedelta(hours=2),
            "energy_consumed",
            52.5
        ),
        (
            tenant_id,
            entity_id,
            base_time + timedelta(hours=2),
            base_time + timedelta(hours=3),
            "energy_consumed",
            55.0
        ),
    ]
    
    insert.insert_run_with_values(
        conninfo,
        run_id=run_id,
        tenant_id=tenant_id,
        workflow_id="energy-monitoring",
        run_start_time=base_time,
        value_rows=value_rows,
    )
    print(f"   ✓ Inserted {len(value_rows)} values (mix of point and interval)")
    
    # Read the data
    print("\n3. Reading data...")
    df = read.read_values_between(
        conninfo,
        tenant_id=tenant_id,
        start_valid=base_time,
        end_valid=base_time + timedelta(hours=4),
        mode="flat",
    )
    
    print(f"   ✓ Retrieved {len(df)} values")
    print("\n   Values:")
    print(df)
    
    # Query the database directly to see interval details
    print("\n4. Inspecting interval details...")
    import psycopg
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT valid_time, valid_time_end, value_key, value
                FROM values_table
                WHERE run_id = %s AND tenant_id = %s
                ORDER BY valid_time, value_key
            """, (run_id, tenant_id))
            
            print("\n   Detailed view:")
            print("   " + "-" * 80)
            for row in cur.fetchall():
                valid_time, valid_time_end, value_key, value = row
                if valid_time_end:
                    print(f"   [{valid_time} → {valid_time_end}] {value_key}: {value}")
                else:
                    print(f"   [{valid_time}] {value_key}: {value} (point-in-time)")
    
    print("\n" + "=" * 60)
    print("Example completed!")
    print("=" * 60)
    print("\nKey insight: timedb supports both point-in-time values")
    print("(like instantaneous measurements) and interval values")
    print("(like energy consumed over a time period) in the same schema.")


if __name__ == "__main__":
    main()

