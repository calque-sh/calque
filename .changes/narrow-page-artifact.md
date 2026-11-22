---
kind: changed
---
After the first page whose generated bindings name some of the served tables
loads, `calque serve` prepares an artifact of only those tables once the
newest cut's artifact is fully encoded, and later pages that name the same
tables download nothing of the others. While writes keep re-cutting the
source before its encodes finish, as at 100k rows under steady writes, those
pages still download every served table. Under a `partitioned` scope, every
page still downloads every served table of its tenant.
