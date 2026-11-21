---
kind: added
---
`inList(column, values)` matches the rows whose column equals any listed
value, like PostgreSQL's `column = ANY(values)`. Use it instead of an `or` of
`eq` calls: the list is held apart from the 64 KiB query specification, up to
50,000 values and 4 MiB, so a query that filters by hundreds of ids no longer
fails with `query specification exceeds 65536 bytes`, and it runs faster than
the `or` of `eq` form. A NULL cell matches
neither `inList` nor its `not`, and an empty list matches no row.
