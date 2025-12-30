"""
Example 3: Updates and Annotations - Human-in-the-loop corrections

This example demonstrates:
- Updating existing values with comments and tags
- Tracking who made changes and when
- Using tags for quality flags
"""
import os
import uuid
import psycopg
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from timedb.db import update

load_dotenv()


def main():
    conninfo = os.environ.get("TIMEDB_DSN") or os.environ.get("DATABASE_URL")
    if not conninfo:
        print("ERROR: Set TIMEDB_DSN or DATABASE_URL environment variable")
        return
    
    print("=" * 60)
    print("Example 3: Updates and Annotations")
    print("=" * 60)
    
    # Create schema
    print("\n1. Creating database schema...")
    update.create_schema(conninfo)
    print("   ✓ Schema created")
    
    # Insert initial forecast
    print("\n2. Inserting initial forecast...")
    run_id = uuid.uuid4()
    base_time = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    
    # Insert run and values
    tenant_id = uuid.uuid4()  # In production, this would come from context
    entity_id = uuid.uuid4()  # In production, this would come from context
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            # Insert run
            cur.execute(
                "INSERT INTO runs_table (run_id, tenant_id, workflow_id, run_start_time) VALUES (%s, %s, %s, %s)",
                (run_id, tenant_id, "auto-forecast", base_time - timedelta(hours=1)),
            )
            # Insert initial values
            for i in range(6):
                cur.execute(
                    """
                    INSERT INTO values_table (run_id, tenant_id, entity_id, valid_time, value_key, value, is_current)
                    VALUES (%s, %s, %s, %s, %s, %s, true)
                    """,
                    (run_id, tenant_id, entity_id, base_time + timedelta(hours=i), "mean", 100.0 + i * 0.5),
                )
    print("   ✓ Initial forecast inserted")
    
    # Step 3: Human review and correction
    print("\n3. Human review: correcting a value...")
    with psycopg.connect(conninfo) as conn:
        # Update a specific value with a comment
        record_update = update.RecordUpdate(
            run_id=run_id,
            tenant_id=tenant_id,
            valid_time=base_time + timedelta(hours=2),
            entity_id=entity_id,
            value_key="mean",
            value=105.0,  # Corrected value
            comment="Manual correction: sensor reading was anomalous",
            tags=["reviewed", "corrected"],
            changed_by="analyst@example.com",
        )
        result = update.update_records(conn, [record_update])
        print(f"   ✓ Updated {len(result.updated)} record(s)")
    
    # Step 4: Add quality flags to multiple values
    print("\n4. Adding quality flags to multiple values...")
    with psycopg.connect(conninfo) as conn:
        updates = []
        for i in [0, 1, 3, 4, 5]:
            updates.append(
                update.RecordUpdate(
                    run_id=run_id,
                    tenant_id=tenant_id,
                    valid_time=base_time + timedelta(hours=i),
                    entity_id=entity_id,
                    value_key="mean",
                    tags=["validated"],  # Only update tags, leave value unchanged
                    changed_by="qa-team@example.com",
                )
            )
        result = update.update_records(conn, updates)
        print(f"   ✓ Updated {len(result.updated)} record(s) with tags")
    
    # Step 5: Read and display the annotated data
    print("\n5. Reading annotated data...")
    # Get the comments and tags from the database
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT valid_time, value_key, value, comment, tags, changed_by
                FROM values_table
                WHERE run_id = %s AND is_current = true
                ORDER BY valid_time
            """, (run_id,))
            
            print("\n   Annotated values:")
            print("   " + "-" * 80)
            for row in cur.fetchall():
                valid_time, value_key, value, comment, tags, changed_by = row
                print(f"   Time: {valid_time}")
                print(f"   Value: {value}")
                if comment:
                    print(f"   Comment: {comment}")
                if tags:
                    print(f"   Tags: {', '.join(tags)}")
                if changed_by:
                    print(f"   Changed by: {changed_by}")
                print()
    
    print("=" * 60)
    print("Example completed!")
    print("=" * 60)
    print("\nKey insight: timedb maintains a full audit trail of all")
    print("changes, including who made them and when, with optional")
    print("comments and semantic tags for quality tracking.")


if __name__ == "__main__":
    main()

