---
kind: changed
---
When a statement on the control store or the canonical log fails, `calque`
logs a warning with its SQLSTATE, or the connection's error when PostgreSQL
sent none. Before, the warning only said the connection failed or returned
an uncertain result.
