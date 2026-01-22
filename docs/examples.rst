Examples
========

TimeDB includes interactive Jupyter notebooks that demonstrate key features and usage patterns. These examples are located in the ``examples/`` directory of the repository.

Prerequisites
-------------

Before running the examples, ensure you have:

1. **PostgreSQL Database**: A PostgreSQL database (version 12+)

2. **Environment Variables**: Set your database connection string:

   .. code-block:: bash

      export TIMEDB_DSN="postgresql://user:password@host:port/database"

3. **Jupyter**: Install Jupyter to run the notebooks:

   .. code-block:: bash

      pip install jupyter

Running Notebooks
-----------------

Open any notebook with:

.. code-block:: bash

   jupyter notebook examples/nb_01_write_read_pandas.ipynb
   # OR
   jupyter lab examples/nb_01_write_read_pandas.ipynb

Available Notebooks
-------------------

Getting Started
~~~~~~~~~~~~~~~

**Notebook 1: Writing and Reading with Pandas** (``nb_01_write_read_pandas.ipynb``)

Learn the fundamentals of working with TimeDB:

- Writing time series data from pandas DataFrames
- Reading data back into DataFrames
- Working with series IDs
- Understanding the basic data model

This is the best place to start if you're new to TimeDB.

**Notebook 2: Units Validation** (``nb_02_units_validation.ipynb``)

Working with physical units using Pint:

- Using pint for unit handling (MW, kW, MWh, etc.)
- Validating units on insert and read
- Automatic unit conversions
- Working with pint-pandas Series

Core Features
~~~~~~~~~~~~~

**Notebook 3: Forecast Revisions** (``nb_03_forecast_revisions.ipynb``)

Understanding TimeDB's three-dimensional time model:

- Creating multiple forecast runs for the same time period
- Understanding ``known_time`` (when the forecast was made)
- Flat vs overlapping query modes
- Analyzing how forecasts evolve over time

This notebook demonstrates one of TimeDB's key differentiators from traditional time series databases.

**Notebook 4: Time Series Changes** (``nb_04_timeseries_changes.ipynb``)

Human-in-the-loop data management:

- Updating existing values with new data
- Adding annotations to explain changes
- Using tags for quality flags (reviewed, corrected, etc.)
- Tracking who made changes and when (audit trail)
- Viewing version history

**Notebook 5: Multiple Series** (``nb_05_multiple_series.ipynb``)

Working with multiple time series:

- Managing multiple series in a single database
- Querying and filtering by series
- Batch operations across series

Advanced Topics
~~~~~~~~~~~~~~~

**Notebook 6: Advanced Querying** (``nb_06_advanced_querying.ipynb``)

Advanced data retrieval patterns:

- Complex filters and queries
- Time range queries
- Working with intervals (``valid_time_end``)
- Performance considerations

**Notebook 7: API Usage** (``nb_07_api_usage.ipynb``)

Using the REST API:

- Starting the API server
- Making HTTP requests to read and write data
- Understanding API endpoints
- Error handling

**Notebook 8: Authentication** (``nb_08_authentication.ipynb``, ``nb_08a_authentication_cli.ipynb``, ``nb_08b_authentication_sdk.ipynb``)

Multi-tenant authentication:

- Setting up user authentication
- Creating and managing API keys
- Tenant isolation
- CLI-based user management
- SDK-based user management

Workflow Examples
-----------------

The ``examples/workflows/`` directory contains real-world workflow examples that demonstrate how to use TimeDB with external data sources:

**Fingrid Wind Forecast** (``workflow_fingrid_wind_forecast.py``)

Fetches wind power forecast data from Fingrid's API and stores it in TimeDB:

- Scheduled data ingestion using Modal
- Working with external APIs
- Storing forecast data with proper ``known_time``

**Nord Pool Intraday** (``workflow_nordpool_id.py``)

Fetches intraday market data from Nord Pool and stores it with metadata:

- Working with interval data (``valid_time`` to ``valid_time_end``)
- Storing associated metadata
- Handling complex data structures

These workflows require additional setup (API keys, Modal configuration) but serve as templates for building your own data pipelines.

Next Steps
----------

After exploring the examples:

1. :doc:`CLI Usage <cli>` - Learn command-line operations
2. :doc:`SDK Usage <sdk>` - Detailed Python API reference
3. :doc:`API Setup <api_setup>` - Deploy the REST API

