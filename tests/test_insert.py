"""Tests for inserting runs and values."""
import pytest
import psycopg
import uuid
from datetime import datetime, timezone, timedelta
from timedb.db import insert, read


def test_insert_run(clean_db, sample_run_id, sample_tenant_id, sample_workflow_id, sample_datetime):
    """Test inserting a run."""
    with psycopg.connect(clean_db) as conn:
        insert.insert_run(
            conn,
            run_id=sample_run_id,
            tenant_id=sample_tenant_id,
            workflow_id=sample_workflow_id,
            run_start_time=sample_datetime,
        )
        
        # Verify run was inserted
        with conn.cursor() as cur:
            cur.execute(
                "SELECT workflow_id, run_start_time FROM runs_table WHERE run_id = %s",
                (sample_run_id,)
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == sample_workflow_id


def test_insert_run_with_known_time(clean_db, sample_run_id, sample_tenant_id, sample_workflow_id, sample_datetime):
    """Test inserting a run with explicit known_time."""
    known_time = sample_datetime - timedelta(hours=1)
    
    with psycopg.connect(clean_db) as conn:
        insert.insert_run(
            conn,
            run_id=sample_run_id,
            tenant_id=sample_tenant_id,
            workflow_id=sample_workflow_id,
            run_start_time=sample_datetime,
            known_time=known_time,
        )
        
        # Verify known_time was set correctly
        with conn.cursor() as cur:
            cur.execute(
                "SELECT known_time FROM runs_table WHERE run_id = %s",
                (sample_run_id,)
            )
            row = cur.fetchone()
            assert row is not None
            # Compare timestamps (allowing for small differences)
            stored_time = row[0]
            assert abs((stored_time - known_time).total_seconds()) < 1


def test_insert_values_point_in_time(clean_db, sample_run_id, sample_tenant_id, sample_entity_id, sample_workflow_id, sample_datetime):
    """Test inserting point-in-time values."""
    with psycopg.connect(clean_db) as conn:
        # Insert run first
        insert.insert_run(
            conn,
            run_id=sample_run_id,
            tenant_id=sample_tenant_id,
            workflow_id=sample_workflow_id,
            run_start_time=sample_datetime,
        )
        
        # Insert values
        value_rows = [
            (sample_tenant_id, sample_datetime, sample_entity_id, "mean", 100.5),
            (sample_tenant_id, sample_datetime + timedelta(hours=1), sample_entity_id, "mean", 101.0),
            (sample_tenant_id, sample_datetime + timedelta(hours=2), sample_entity_id, "quantile:0.5", 102.5),
        ]
        insert.insert_values(conn, run_id=sample_run_id, value_rows=value_rows)
        
        # Verify values were inserted
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM values_table WHERE run_id = %s",
                (sample_run_id,)
            )
            assert cur.fetchone()[0] == 3


def test_insert_values_interval(clean_db, sample_run_id, sample_tenant_id, sample_entity_id, sample_workflow_id, sample_datetime):
    """Test inserting interval values."""
    with psycopg.connect(clean_db) as conn:
        # Insert run first
        insert.insert_run(
            conn,
            run_id=sample_run_id,
            tenant_id=sample_tenant_id,
            workflow_id=sample_workflow_id,
            run_start_time=sample_datetime,
        )
        
        # Insert interval values (tenant_id, valid_time, valid_time_end, entity_id, value_key, value)
        value_rows = [
            (
                sample_tenant_id,
                sample_datetime,
                sample_datetime + timedelta(hours=1),
                sample_entity_id,
                "mean",
                100.5
            ),
        ]
        insert.insert_values(conn, run_id=sample_run_id, value_rows=value_rows)
        
        # Verify interval value was inserted
        with conn.cursor() as cur:
            cur.execute(
                "SELECT valid_time_end FROM values_table WHERE run_id = %s",
                (sample_run_id,)
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] is not None


def test_insert_run_with_values(clean_db, sample_run_id, sample_tenant_id, sample_entity_id, sample_workflow_id, sample_datetime):
    """Test the convenience function that inserts run and values together."""
    value_rows = [
        (sample_tenant_id, sample_datetime, sample_entity_id, "mean", 100.5),
        (sample_tenant_id, sample_datetime + timedelta(hours=1), sample_entity_id, "mean", 101.0),
    ]
    
    insert.insert_run_with_values(
        clean_db,
        run_id=sample_run_id,
        tenant_id=sample_tenant_id,
        workflow_id=sample_workflow_id,
        run_start_time=sample_datetime,
        run_finish_time=None,
        value_rows=value_rows,
    )
    
    # Verify both run and values exist
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM runs_table WHERE run_id = %s", (sample_run_id,))
            assert cur.fetchone()[0] == 1
            
            cur.execute("SELECT COUNT(*) FROM values_table WHERE run_id = %s", (sample_run_id,))
            assert cur.fetchone()[0] == 2


def test_insert_values_timezone_aware(clean_db, sample_run_id, sample_tenant_id, sample_entity_id, sample_workflow_id):
    """Test that timezone-aware datetimes are required."""
    with psycopg.connect(clean_db) as conn:
        insert.insert_run(
            conn,
            run_id=sample_run_id,
            tenant_id=sample_tenant_id,
            workflow_id=sample_workflow_id,
            run_start_time=datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
        )
        
        # Try to insert with timezone-naive datetime - should raise ValueError
        with pytest.raises(ValueError, match="timezone-aware"):
            insert.insert_values(
                conn,
                run_id=sample_run_id,
                value_rows=[(sample_tenant_id, datetime(2025, 1, 1, 12, 0), sample_entity_id, "mean", 100.5)],
            )

