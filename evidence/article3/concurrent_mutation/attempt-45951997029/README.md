# Targeted mutation fixture collision

[CI run 45951997029](https://github.com/hachej/boring-cdc/actions/runs/45951997029) tested commit `1cf085257def42ae4b3b89261657463e36a5ce00`. The copy ran for 212.108 seconds. Capture preserved the targeted 12-event transaction and the full keyed oracle matched all 199,996 final rows in PostgreSQL and ClickHouse.

The targeted check still failed. It chose key 5011 for an update while that key's range was pending. The background workload later deleted key 5011, leaving it absent in both systems. The targeted oracle still expected the earlier update. The fixture fix reserves background workload keys before selecting targets. This packet preserves the first verdict without interpreting a fixture collision as destination loss.

The two raw query files are stored as gzip. Decompress them before comparing their bytes with the original CI manifest digests.
