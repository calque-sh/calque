---
kind: changed
---
After the first page whose generated bindings name some of the served tables
loads, `calque serve` prepares an artifact of only those tables, and later
pages that name the same tables download nothing of the others. Under a
`partitioned` scope, every page still downloads every served table of its
tenant.
