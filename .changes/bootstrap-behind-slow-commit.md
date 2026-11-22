---
kind: fixed
---
A bootstrap is no longer answered `503 INTERNAL_RETRYABLE` when PostgreSQL
takes over a second to flush the commit of a change `calque` is recording.
