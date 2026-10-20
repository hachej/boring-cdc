# Local diagnostic: ephemeral Docker port remap

This packet is a real failed 120-second ClickHouse outage run at Git `8c269353`. It must remain failed. During the run, the configured HTTPS endpoint was `127.0.0.1:33340`; after `docker start`, `docker port` reported `127.0.0.1:33342` for container port 8443. The scenario had used `-p 127.0.0.1::8443`, so Docker assigned a new host port after restart. The connector kept retrying against the original endpoint and the checkpoint stayed at seq 14. Capture continued and status stayed degraded.

The harness now pins both host ports and verifies the binding after restart. This packet is diagnostic evidence of the harness fault, not a pass for the destination-outage scenario. Its original packet files are unchanged.
