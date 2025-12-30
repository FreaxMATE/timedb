"""
TimeDB - A time series database for PostgreSQL.

High-level SDK usage:
    import timedb as td
    
    # Create schema
    td.create()
    
    # Insert run with DataFrame (minimal - only DataFrame required!)
    result = td.insert_run(
        df=df,  # Only required parameter for single-tenant installations
    )
    # result.entity_id can be saved for future updates
    
    # Insert run with DataFrame (with explicit IDs for multi-tenant)
    result = td.insert_run(
        df=df,
        tenant_id=tenant_id,  # Optional: only needed for multi-tenant
        entity_id=entity_id,  # Optional: provide if you want to update this entity later
    )
"""

from .sdk import create, delete, insert_run, read, InsertResult, DEFAULT_TENANT_ID

__all__ = ['create', 'delete', 'insert_run', 'read', 'InsertResult', 'DEFAULT_TENANT_ID']

