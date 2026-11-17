---
kind: breaking
---
A state directory prepared by 0.3.0, and a recovery unit 0.3.0 took, are
refused and need `calque destroy`. Calque now records each served column's
version, so `ALTER COLUMN ... TYPE` to the column's own type with a `USING`
expression stops the server, asking for `calque destroy`, instead of leaving
the old values served.
