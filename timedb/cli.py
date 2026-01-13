# timedb/cli.py
import os
import sys
import click
#from importlib import import_module
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

load_dotenv() 

def dsn_option(fn):
    return click.option(
        "--dsn", "-d",
        envvar=["TIMEDB_DSN", "DATABASE_URL"],
        help="Postgres DSN",
    )(fn)

@click.group()
def cli():
    """timedb CLI"""

@cli.group()
def create():
    """Create resources (e.g. tables)"""
    pass

@cli.group()
def delete():
    """Delete resources (e.g. tables)"""
    pass

@cli.command("api")
@click.option("--host", default="127.0.0.1", help="Host to bind to")
@click.option("--port", default=8000, type=int, help="Port to bind to")
@click.option("--reload", is_flag=True, help="Enable auto-reload for development")
def start_api(host, port, reload):
    """
    Start the FastAPI server.
    Example: timedb api --host 127.0.0.1 --port 8000
    """
    try:
        import uvicorn
        click.echo(f"Starting TimeDB API server on http://{host}:{port}")
        click.echo(f"API docs available at http://{host}:{port}/docs")
        click.echo("Press Ctrl+C to stop the server")
        uvicorn.run(
            "timedb.api:app",
            host=host,
            port=port,
            reload=reload,
        )
    except ImportError:
        click.echo("ERROR: FastAPI dependencies not installed. Run: pip install fastapi uvicorn[standard]", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)

@create.command("tables")
@dsn_option
@click.option("--schema", "-s", default=None, help="Schema name to use for the tables (sets search_path for the DDL).")
@click.option("--with-metadata/--no-metadata", default=False, help="Also create the optional metadata_table addon.")
@click.option("--with-users/--no-users", default=False, help="Also create the optional users_table for authentication.")
@click.option("--yes", "-y", is_flag=True, help="Do not prompt for confirmation")
@click.option("--dry-run", is_flag=True, help="Print the DDL (from db.create.DDL) and exit")
def create_tables(dsn, schema, with_metadata, with_users, yes, dry_run):
    """
    Create timedb tables using db.create.create_schema().
    Example: timedb create tables --dsn postgresql://... --schema timedb --with-metadata
    """
    conninfo = dsn
    if not conninfo:
        click.echo("ERROR: no DSN provided. Use --dsn or set TIMEDB_DSN / DATABASE_URL", err=True)
        sys.exit(2)

    # Import the implementation module(s) from repository
    try:
        from . import db
    except Exception as e:
        click.echo(f"ERROR: cannot import db module: {e}", err=True)
        sys.exit(1)

    # Dry-run -> print the DDL string defined in module (db.create.DDL)
    if dry_run:
        ddl = getattr(db.create, "DDL", None)
        if ddl is None:
            click.echo("No DDL found in pg_create_table module.", err=True)
            sys.exit(1)
        click.echo(ddl)
        return

    if not yes:
        click.echo("About to create/update timedb schema/tables using pg_create_table.create_schema().")
        click.echo(f"Connection: {conninfo}")
        if schema:
            click.echo(f"Schema/search_path: {schema}")
        if with_metadata:
            click.echo("Will also create the optional metadata_table addon.")
        if with_users:
            click.echo("Will also create the optional users_table for authentication.")
        if not click.confirm("Continue?"):
            click.echo("Aborted.")
            return

    # If schema specified, set PGOPTIONS to ensure the DDL runs with that search_path.
    old_pgoptions = os.environ.get("PGOPTIONS")
    if schema:
        # Set search_path for server session using libpq/pgoptions mechanism
        # This ensures create_schema's psycopg.connect() inherits the search_path.
        os.environ["PGOPTIONS"] = f"-c search_path={schema}"
    try:
        # call the create_schema function from db.create
        create_schema = getattr(db.create, "create_schema", None)
        if create_schema is None:
            raise RuntimeError("db.create.create_schema not found")
        create_schema(conninfo)
        click.echo("Base timedb tables created/updated successfully.")

        if with_metadata:
            try:
                create_schema_metadata = getattr(db.create_with_metadata, "create_schema_metadata", None)
                if create_schema_metadata is None:
                    raise RuntimeError("db.create_with_metadata.create_schema_metadata not found")
                create_schema_metadata(conninfo)
                click.echo("Optional metadata schema created/updated successfully.")
            except Exception as e:
                click.echo(f"ERROR creating metadata schema: {e}", err=True)
                sys.exit(1)

        if with_users:
            try:
                create_schema_users = getattr(db.create_with_users, "create_schema_users", None)
                if create_schema_users is None:
                    raise RuntimeError("db.create_with_users.create_schema_users not found")
                create_schema_users(conninfo)
                click.echo("Optional users schema created/updated successfully.")
            except Exception as e:
                click.echo(f"ERROR creating users schema: {e}", err=True)
                sys.exit(1)

    except Exception as exc:
        click.echo(f"ERROR creating tables: {exc}", err=True)
        sys.exit(1)
    finally:
        # restore PGOPTIONS
        if schema:
            if old_pgoptions is None:
                os.environ.pop("PGOPTIONS", None)
            else:
                os.environ["PGOPTIONS"] = old_pgoptions

@delete.command("tables")
@dsn_option
@click.option("--schema", "-s", default=None, help="Schema name to use for the tables (sets search_path for the DDL).")
@click.option("--yes", "-y", is_flag=True, help="Do not prompt for confirmation")
def delete_tables(dsn, schema, yes):
    """
    Delete all timedb tables (including metadata tables).
    Example: timedb delete tables --dsn postgresql://... --schema timedb
    """
    conninfo = dsn
    if not conninfo:
        click.echo("ERROR: no DSN provided. Use --dsn or set TIMEDB_DSN / DATABASE_URL", err=True)
        sys.exit(2)

    if not yes:
        click.echo("WARNING: This will delete ALL timedb tables and their data!")
        click.echo("This includes: runs_table, values_table, metadata_table, and all views.")
        click.echo(f"Connection: {conninfo}")
        if schema:
            click.echo(f"Schema/search_path: {schema}")
        if not click.confirm("Are you sure you want to continue? This action cannot be undone."):
            click.echo("Aborted.")
            return

    # Import the implementation module from repository
    try:
        from . import db
    except Exception as e:
        click.echo(f"ERROR: cannot import db module: {e}", err=True)
        sys.exit(1)

    # If schema specified, set PGOPTIONS to ensure the DDL runs with that search_path.
    old_pgoptions = os.environ.get("PGOPTIONS")
    if schema:
        # Set search_path for server session using libpq/pgoptions mechanism
        # This ensures delete_schema's psycopg.connect() inherits the search_path.
        os.environ["PGOPTIONS"] = f"-c search_path={schema}"
    try:
        # call the delete_schema function from db.delete
        delete_schema = getattr(db.delete, "delete_schema", None)
        if delete_schema is None:
            raise RuntimeError("pg_delete_table.delete_schema not found")
        delete_schema(conninfo)
        click.echo("All timedb tables (including metadata) deleted successfully.")
            
    except Exception as exc:
        click.echo(f"ERROR deleting tables: {exc}", err=True)
        sys.exit(1)
    finally:
        # restore PGOPTIONS
        if schema:
            if old_pgoptions is None:
                os.environ.pop("PGOPTIONS", None)
            else:
                os.environ["PGOPTIONS"] = old_pgoptions

@cli.group()
def users():
    """Manage users and API keys"""
    pass


@users.command("create")
@dsn_option
@click.option("--tenant-id", "-t", required=True, help="Tenant UUID for the user")
@click.option("--email", "-e", required=True, help="User email address")
def create_user(dsn, tenant_id, email):
    """
    Create a new user with an API key.
    Example: timedb users create --tenant-id 123e4567-e89b-12d3-a456-426614174000 --email user@example.com
    """
    import uuid
    import psycopg

    conninfo = dsn
    if not conninfo:
        click.echo("ERROR: no DSN provided. Use --dsn or set TIMEDB_DSN / DATABASE_URL", err=True)
        sys.exit(2)

    try:
        tenant_uuid = uuid.UUID(tenant_id)
    except ValueError:
        click.echo(f"ERROR: Invalid tenant-id UUID: {tenant_id}", err=True)
        sys.exit(1)

    try:
        from . import db
    except Exception as e:
        click.echo(f"ERROR: cannot import db module: {e}", err=True)
        sys.exit(1)

    try:
        with psycopg.connect(conninfo) as conn:
            user = db.users.create_user(conn, tenant_id=tenant_uuid, email=email)
            click.echo(f"User created successfully!")
            click.echo(f"  User ID:   {user['user_id']}")
            click.echo(f"  Tenant ID: {user['tenant_id']}")
            click.echo(f"  Email:     {user['email']}")
            click.echo(f"  API Key:   {user['api_key']}")
            click.echo("")
            click.echo("Save the API key - it will not be shown again.")
    except psycopg.errors.UniqueViolation as e:
        if "api_key" in str(e):
            click.echo("ERROR: API key collision (extremely rare). Please try again.", err=True)
        else:
            click.echo(f"ERROR: User with email '{email}' already exists for this tenant.", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


@users.command("list")
@dsn_option
@click.option("--tenant-id", "-t", default=None, help="Filter by tenant UUID")
@click.option("--include-inactive", is_flag=True, help="Include inactive users")
def list_users(dsn, tenant_id, include_inactive):
    """
    List all users.
    Example: timedb users list --tenant-id 123e4567-e89b-12d3-a456-426614174000
    """
    import uuid
    import psycopg

    conninfo = dsn
    if not conninfo:
        click.echo("ERROR: no DSN provided. Use --dsn or set TIMEDB_DSN / DATABASE_URL", err=True)
        sys.exit(2)

    tenant_uuid = None
    if tenant_id:
        try:
            tenant_uuid = uuid.UUID(tenant_id)
        except ValueError:
            click.echo(f"ERROR: Invalid tenant-id UUID: {tenant_id}", err=True)
            sys.exit(1)

    try:
        from . import db
    except Exception as e:
        click.echo(f"ERROR: cannot import db module: {e}", err=True)
        sys.exit(1)

    try:
        with psycopg.connect(conninfo) as conn:
            users_list = db.users.list_users(conn, tenant_id=tenant_uuid, include_inactive=include_inactive)
            if not users_list:
                click.echo("No users found.")
                return

            click.echo(f"Found {len(users_list)} user(s):\n")
            for user in users_list:
                status = "active" if user['is_active'] else "inactive"
                click.echo(f"  Email:     {user['email']}")
                click.echo(f"  User ID:   {user['user_id']}")
                click.echo(f"  Tenant ID: {user['tenant_id']}")
                click.echo(f"  Status:    {status}")
                click.echo(f"  Created:   {user['created_at']}")
                click.echo("")
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


@users.command("regenerate-key")
@dsn_option
@click.option("--email", "-e", required=True, help="User email address")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
def regenerate_key(dsn, email, yes):
    """
    Regenerate API key for a user.
    Example: timedb users regenerate-key --email user@example.com
    """
    import psycopg

    conninfo = dsn
    if not conninfo:
        click.echo("ERROR: no DSN provided. Use --dsn or set TIMEDB_DSN / DATABASE_URL", err=True)
        sys.exit(2)

    if not yes:
        click.echo(f"About to regenerate API key for user: {email}")
        click.echo("The old API key will be invalidated immediately.")
        if not click.confirm("Continue?"):
            click.echo("Aborted.")
            return

    try:
        from . import db
    except Exception as e:
        click.echo(f"ERROR: cannot import db module: {e}", err=True)
        sys.exit(1)

    try:
        with psycopg.connect(conninfo) as conn:
            new_key = db.users.regenerate_api_key(conn, email=email)
            if new_key is None:
                click.echo(f"ERROR: User with email '{email}' not found.", err=True)
                sys.exit(1)
            click.echo(f"API key regenerated successfully!")
            click.echo(f"  Email:       {email}")
            click.echo(f"  New API Key: {new_key}")
            click.echo("")
            click.echo("Save the API key - it will not be shown again.")
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


@users.command("deactivate")
@dsn_option
@click.option("--email", "-e", required=True, help="User email address")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
def deactivate_user(dsn, email, yes):
    """
    Deactivate a user (revoke API access).
    Example: timedb users deactivate --email user@example.com
    """
    import psycopg

    conninfo = dsn
    if not conninfo:
        click.echo("ERROR: no DSN provided. Use --dsn or set TIMEDB_DSN / DATABASE_URL", err=True)
        sys.exit(2)

    if not yes:
        click.echo(f"About to deactivate user: {email}")
        click.echo("The user will no longer be able to authenticate with their API key.")
        if not click.confirm("Continue?"):
            click.echo("Aborted.")
            return

    try:
        from . import db
    except Exception as e:
        click.echo(f"ERROR: cannot import db module: {e}", err=True)
        sys.exit(1)

    try:
        with psycopg.connect(conninfo) as conn:
            success = db.users.deactivate_user(conn, email=email)
            if not success:
                click.echo(f"ERROR: User with email '{email}' not found.", err=True)
                sys.exit(1)
            click.echo(f"User '{email}' deactivated successfully.")
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


@users.command("activate")
@dsn_option
@click.option("--email", "-e", required=True, help="User email address")
def activate_user(dsn, email):
    """
    Activate a user (restore API access).
    Example: timedb users activate --email user@example.com
    """
    import psycopg

    conninfo = dsn
    if not conninfo:
        click.echo("ERROR: no DSN provided. Use --dsn or set TIMEDB_DSN / DATABASE_URL", err=True)
        sys.exit(2)

    try:
        from . import db
    except Exception as e:
        click.echo(f"ERROR: cannot import db module: {e}", err=True)
        sys.exit(1)

    try:
        with psycopg.connect(conninfo) as conn:
            success = db.users.activate_user(conn, email=email)
            if not success:
                click.echo(f"ERROR: User with email '{email}' not found.", err=True)
                sys.exit(1)
            click.echo(f"User '{email}' activated successfully.")
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()