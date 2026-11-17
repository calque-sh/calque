---
kind: fixed
---
A table whose row-level security would filter Calque's reads is refused
with `ROW_LEVEL_SECURITY_ACTIVE`, naming the role that needs `BYPASSRLS`,
instead of serving an empty snapshot.
