"""
FastAPI application for timedb - MVP version
Provides REST API endpoints for time series database operations.
"""
import os
import uuid
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, ConfigDict
from dotenv import load_dotenv, find_dotenv
import psycopg
import pandas as pd

from . import db

load_dotenv(find_dotenv())

# Database connection string from environment
def get_dsn() -> str:
    """Get database connection string from environment variables."""
    dsn = os.environ.get("TIMEDB_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise HTTPException(
            status_code=500,
            detail="Database connection not configured. Set TIMEDB_DSN or DATABASE_URL environment variable."
        )
    return dsn


# Pydantic models for request/response
class ValueRow(BaseModel):
    """A single value row for insertion."""
    valid_time: datetime
    value_key: str
    value: Optional[float] = None
    valid_time_end: Optional[datetime] = None  # For interval values

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class CreateRunRequest(BaseModel):
    """Request to create a run with values."""
    workflow_id: str
    run_start_time: datetime
    run_finish_time: Optional[datetime] = None
    known_time: Optional[datetime] = None  # Time of knowledge - defaults to inserted_at (now()) if not provided
    run_params: Optional[Dict[str, Any]] = None
    value_rows: List[ValueRow] = Field(default_factory=list)

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class CreateRunResponse(BaseModel):
    """Response after creating a run."""
    run_id: str
    message: str


class RecordUpdateRequest(BaseModel):
    """Request to update a record.
    
    For value, comment, and tags:
    - Omit the field to leave it unchanged
    - Set to null to explicitly clear it
    - Set to a value to update it
    """
    run_id: str
    tenant_id: str
    valid_time: datetime
    entity_id: str
    value_key: str
    value: Optional[float] = Field(default=None, description="Omit to leave unchanged, null to clear, or provide a value")
    comment: Optional[str] = Field(default=None, description="Omit to leave unchanged, null to clear, or provide a value")
    tags: Optional[List[str]] = Field(default=None, description="Omit to leave unchanged, null or [] to clear, or provide tags")
    changed_by: Optional[str] = None

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class UpdateRecordsRequest(BaseModel):
    """Request to update multiple records."""
    updates: List[RecordUpdateRequest]


class UpdateRecordsResponse(BaseModel):
    """Response after updating records."""
    updated: List[Dict[str, Any]]
    skipped_no_ops: List[Dict[str, Any]]


class CreateSchemaRequest(BaseModel):
    """Request to create schema."""
    schema: Optional[str] = None
    with_metadata: bool = False


class ErrorResponse(BaseModel):
    """Error response model."""
    detail: str


# FastAPI app
app = FastAPI(
    title="TimeDB API",
    description="REST API for time series database operations",
    version="0.1.1"
)


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "TimeDB API",
        "version": "0.1.1",
        "endpoints": {
            "read_values": "GET /values",
            "create_run": "POST /runs",
            "update_records": "PUT /values",
            "create_schema": "POST /schema/create",
            "delete_schema": "DELETE /schema/delete",
        }
    }


@app.get("/values", response_model=Dict[str, Any])
async def read_values(
    start_valid: Optional[datetime] = Query(None, description="Start of valid time range (ISO format)"),
    end_valid: Optional[datetime] = Query(None, description="End of valid time range (ISO format)"),
    start_known: Optional[datetime] = Query(None, description="Start of known_time range (ISO format)"),
    end_known: Optional[datetime] = Query(None, description="End of known_time range (ISO format)"),
    mode: str = Query("flat", description="Query mode: 'flat' or 'overlapping'"),
    all_versions: bool = Query(False, description="Include all versions (not just current)"),
):
    """
    Read time series values from the database.
    
    Returns values filtered by valid_time and/or known_time ranges.
    All datetime parameters should be in ISO format (e.g., 2025-01-01T00:00:00Z).
    
    Modes:
    - "flat": Returns (valid_time, value_key, value) with latest known_time per valid_time
    - "overlapping": Returns (known_time, valid_time, value_key, value) - all rows with known_time
    
    all_versions: If True, includes all versions (both is_current=true and false). If False, only current values.
    """
    try:
        dsn = get_dsn()
        
        # Validate mode parameter
        if mode not in ["flat", "overlapping"]:
            raise HTTPException(status_code=400, detail=f"Invalid mode: {mode}. Must be 'flat' or 'overlapping'")
        
        # Convert timezone-naive datetimes to UTC if needed
        if start_valid and start_valid.tzinfo is None:
            start_valid = start_valid.replace(tzinfo=timezone.utc)
        if end_valid and end_valid.tzinfo is None:
            end_valid = end_valid.replace(tzinfo=timezone.utc)
        if start_known and start_known.tzinfo is None:
            start_known = start_known.replace(tzinfo=timezone.utc)
        if end_known and end_known.tzinfo is None:
            end_known = end_known.replace(tzinfo=timezone.utc)
        
        df = db.read.read_values_between(
            dsn,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            mode=mode,
            all_versions=all_versions,
        )
        
        # Convert DataFrame to JSON-serializable format
        df_reset = df.reset_index()
        records = df_reset.to_dict(orient="records")
        
        # Convert datetime objects to ISO format strings
        for record in records:
            for key, value in record.items():
                if isinstance(value, pd.Timestamp):
                    record[key] = value.isoformat()
                elif isinstance(value, datetime):
                    record[key] = value.isoformat()
                elif pd.isna(value):
                    record[key] = None
        
        return {
            "count": len(records),
            "data": records
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading values: {str(e)}")


@app.post("/runs", response_model=CreateRunResponse)
async def create_run(request: CreateRunRequest):
    """
    Create a new run with associated values.
    
    This endpoint creates a run entry and optionally inserts value rows.
    """
    try:
        dsn = get_dsn()
        run_id = uuid.uuid4()
        
        # Convert value_rows to the format expected by insert_run_with_values
        value_rows = []
        for row in request.value_rows:
            if row.valid_time_end is not None:
                # Interval value: (valid_time, valid_time_end, value_key, value)
                value_rows.append((row.valid_time, row.valid_time_end, row.value_key, row.value))
            else:
                # Point-in-time value: (valid_time, value_key, value)
                value_rows.append((row.valid_time, row.value_key, row.value))
        
        # Ensure timezone-aware datetimes
        if request.run_start_time.tzinfo is None:
            run_start_time = request.run_start_time.replace(tzinfo=timezone.utc)
        else:
            run_start_time = request.run_start_time
            
        if request.run_finish_time is not None:
            if request.run_finish_time.tzinfo is None:
                run_finish_time = request.run_finish_time.replace(tzinfo=timezone.utc)
            else:
                run_finish_time = request.run_finish_time
        else:
            run_finish_time = None
        
        if request.known_time is not None:
            if request.known_time.tzinfo is None:
                known_time = request.known_time.replace(tzinfo=timezone.utc)
            else:
                known_time = request.known_time
        else:
            known_time = None  # Will default to run_start_time in insert_run
        
        db.insert.insert_run_with_values(
            conninfo=dsn,
            run_id=run_id,
            workflow_id=request.workflow_id,
            run_start_time=run_start_time,
            run_finish_time=run_finish_time,
            known_time=known_time,
            value_rows=value_rows,
            run_params=request.run_params,
        )
        
        return CreateRunResponse(
            run_id=str(run_id),
            message="Run created successfully"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating run: {str(e)}")


@app.put("/values", response_model=UpdateRecordsResponse)
async def update_records(request: UpdateRecordsRequest):
    """
    Update one or more records in the values table.
    
    This endpoint supports tri-state updates:
    - Omit a field to leave it unchanged
    - Set to None to explicitly clear the field
    - Set to a value to update it
    """
    try:
        dsn = get_dsn()
        
        # Convert request updates to RecordUpdate objects
        updates = []
        for req_update in request.updates:
            # Parse UUIDs
            try:
                run_id_uuid = uuid.UUID(req_update.run_id)
                tenant_id_uuid = uuid.UUID(req_update.tenant_id)
                entity_id_uuid = uuid.UUID(req_update.entity_id)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid UUID format: {e}")
            
            # Ensure timezone-aware datetime
            valid_time = req_update.valid_time
            if valid_time.tzinfo is None:
                valid_time = valid_time.replace(tzinfo=timezone.utc)
            
            # Use model_dump to check which fields were actually provided
            # exclude_unset=True gives us only fields that were explicitly set
            provided_fields = req_update.model_dump(exclude_unset=True)
            
            # Build update dict with _UNSET for fields not provided
            update_dict = {
                "run_id": run_id_uuid,
                "tenant_id": tenant_id_uuid,
                "valid_time": valid_time,
                "entity_id": entity_id_uuid,
                "value_key": req_update.value_key,
                "changed_by": req_update.changed_by,
            }
            
            # Check if each field was provided in the request
            if "value" in provided_fields:
                update_dict["value"] = req_update.value  # Can be None (explicit clear) or a float
            else:
                update_dict["value"] = db.update._UNSET  # Not provided, leave unchanged
                
            if "comment" in provided_fields:
                update_dict["comment"] = req_update.comment  # Can be None (explicit clear) or a string
            else:
                update_dict["comment"] = db.update._UNSET  # Not provided, leave unchanged
                
            if "tags" in provided_fields:
                update_dict["tags"] = req_update.tags  # Can be None or [] (explicit clear) or a list
            else:
                update_dict["tags"] = db.update._UNSET  # Not provided, leave unchanged
            
            update = db.update.RecordUpdate(**update_dict)
            updates.append(update)
        
        # Execute updates
        with psycopg.connect(dsn) as conn:
            outcome = db.update.update_records(conn, updates)
        
        # Convert response to JSON-serializable format
        updated = [
            {
                "run_id": str(r.key.run_id),
                "tenant_id": str(r.key.tenant_id),
                "valid_time": r.key.valid_time.isoformat(),
                "entity_id": str(r.key.entity_id),
                "value_key": r.key.value_key,
                "version_id": r.version_id
            }
            for r in outcome.updated
        ]
        
        skipped = [
            {
                "run_id": str(k.run_id),
                "tenant_id": str(k.tenant_id),
                "valid_time": k.valid_time.isoformat(),
                "entity_id": str(k.entity_id),
                "value_key": k.value_key
            }
            for k in outcome.skipped_no_ops
        ]
        
        return UpdateRecordsResponse(
            updated=updated,
            skipped_no_ops=skipped
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating records: {str(e)}")


@app.post("/schema/create")
async def create_schema(request: CreateSchemaRequest):
    """
    Create or update the database schema.
    
    This endpoint creates the timedb tables. It's safe to run multiple times.
    """
    try:
        dsn = get_dsn()
        
        # Set schema/search_path if provided
        old_pgoptions = os.environ.get("PGOPTIONS")
        if request.schema:
            os.environ["PGOPTIONS"] = f"-c search_path={request.schema}"
        
        try:
            db.create.create_schema(dsn)
            message = "Base timedb tables created/updated successfully."
            
            if request.with_metadata:
                db.create_with_metadata.create_schema_metadata(dsn)
                message += " Optional metadata schema created/updated successfully."
            
            return {"message": message}
        finally:
            # Restore PGOPTIONS
            if request.schema:
                if old_pgoptions is None:
                    os.environ.pop("PGOPTIONS", None)
                else:
                    os.environ["PGOPTIONS"] = old_pgoptions
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating schema: {str(e)}")


@app.delete("/schema/delete")
async def delete_schema(schema: Optional[str] = Query(None, description="Schema name")):
    """
    Delete all timedb tables and views.
    
    WARNING: This will delete all data! Use with caution.
    """
    try:
        dsn = get_dsn()
        
        # Set schema/search_path if provided
        old_pgoptions = os.environ.get("PGOPTIONS")
        if schema:
            os.environ["PGOPTIONS"] = f"-c search_path={schema}"
        
        try:
            db.delete.delete_schema(dsn)
            return {"message": "All timedb tables (including metadata) deleted successfully."}
        finally:
            # Restore PGOPTIONS
            if schema:
                if old_pgoptions is None:
                    os.environ.pop("PGOPTIONS", None)
                else:
                    os.environ["PGOPTIONS"] = old_pgoptions
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting schema: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

