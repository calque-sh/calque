---
kind: breaking
---
`docker-compose.selfhost.yml` publishes PostgreSQL on 25452 and MinIO on 29001
by default, instead of 55452 and 59001. Those sat in Linux's ephemeral port
range, where `docker compose up` could fail with "address already in use". To
keep the old ports, set `CALQUE_SELFHOST_POSTGRES_PORT=55452` and
`CALQUE_SELFHOST_S3_PORT=59001`.
