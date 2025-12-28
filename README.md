# timedb


- Overlapping forecasts 
- Human manual updates, tagging and commenting 
- Multi-interval time series 
- Metadata added on timestamp/time interval level


Bitemporal model --> Handled with run_time and valid_time 
model metadata (lineage) --> Handled with run_params
Probabilistic forecasts & ensembles --> Handled through value_key
Quality flags, validation & reconciliation --> Handled with tags
(raw, validated, imputed, suspect, reconciled)
Backfill & late-arrival handling --> Handled with run_time and valid_time
Change audit trail --> Handled with versioning 

Unit handling (e.g. MW, kW) --> This isn't handled yet. 

---- 
Pattern: store timestamps in timestamptz (UTC canonical), but also store local_ts and tz where helpful, or store tz per series. Keep conversions deterministic.
--> This isn't handled yet but could be handled in the app-layer. 

Range/interval support (tsrange/tstzrange) --> Not handled
Efficient indexing & partitioning for scale --> Not handled
Data retention, TTL, and archiving --> Not handled
Eventing & subscriptions --> Not handled
Support for irregular/uneven timesteps --> Not handled
Python SDK with objects such as TimeSeries, etc