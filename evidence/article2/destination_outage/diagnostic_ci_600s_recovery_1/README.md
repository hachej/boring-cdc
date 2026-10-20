# Article 2 destination outage

Result: **fail**. Failure codes: DESTINATION_DID_NOT_CATCH_UP.

The M3 live fixture used actual PostgreSQL snapshot/import/fence and verified the initial ClickHouse batch. The shipped `boring-cdc run` process then owned capture and destination delivery. ClickHouse was stopped with `docker stop` for 120 seconds while PostgreSQL transactions continued. The candidate keyed query was compared with a separate PostgreSQL query. Managed Estuary comparison remains unavailable.

Run `python3 validate.py observation.json` offline. A failing outcome exits nonzero and is retained.
