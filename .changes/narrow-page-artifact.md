---
kind: changed
---
After the first page whose generated bindings name some of the served tables
loads, `calque serve` prepares an artifact of only those tables at each new
cut, and later pages that name the same tables download nothing of the
others, under steady writes too. Right after a new cut, such a page
downloads every served table until that cut's artifact of its tables is
ready, and under steady writes that artifact is less compressed than on a
quiet source. Under a `partitioned` scope, every page still downloads every
served table of its tenant.
