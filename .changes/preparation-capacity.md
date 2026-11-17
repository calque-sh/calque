---
kind: breaking
---
`calque serve` and `calque dev` refuse to start when PostgreSQL could not
hold their preparations. Each preparation holds a replication slot and a
WAL sender beside change capture's, and the server needs 24 connections plus
6 per preparation. A server that started under tighter
`max_replication_slots`, `max_wal_senders`, `max_connections` or connection
limits now refuses, naming the limit to raise.
