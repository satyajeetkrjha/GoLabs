```
This lab models an OpenTelemetry-style ingestion pipeline with bounded queues, early load shedding, TTL-based drops, and exporter retries with backoff.
It builds an intuition for how backpressure, retries, and downstream slowness cause memory growth, drops, and OOMs in real collector deployments.

```