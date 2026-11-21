---
kind: fixed
---
A page whose generated bindings name fewer tables than its server serves now
loads, answers queries and follows changes to the tables it names, instead of
failing with `SCHEMA_MISMATCH`. It ignores the other tables and their changes.
A table the bindings name that the server does not serve is still refused.
